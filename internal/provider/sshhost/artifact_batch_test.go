package sshhost

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/provider"
)

func batchArtifactFixture(t *testing.T, count int) (*harness, provider.RemoteReference, []provider.Artifact) {
	t.Helper()
	h := newHarness(t)
	remote := h.submit(t)
	files := map[string]string{}
	artifacts := make([]provider.Artifact, count)
	for i := range count {
		name := fmt.Sprintf("outputs/file-%04d.bin", i)
		data := fmt.Sprintf("value-%04d", i)
		files[name] = data
		artifacts[i] = provider.Artifact{Remote: remote, Path: name, Bytes: int64(len(data)), SHA256: provider.Digest([]byte(data))}
	}
	files["outputs/excluded-private.bin"] = "private excluded bytes"
	files["outputs/undeclared.bin"] = "undeclared bytes"
	h.host.finish(0, files)
	return h, remote, artifacts
}

func TestArtifactBatchSharesConnectionAndSelectsExactFilesAtScale(t *testing.T) {
	for _, count := range []int{0, 200, 500} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			h, remote, artifacts := batchArtifactFixture(t, count)
			h.host.calls = nil
			before := h.dials
			visited := 0
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := h.adapter.FetchBatch(ctx, remote, artifacts, func(artifact provider.Artifact, r io.Reader) error {
				data, err := io.ReadAll(r)
				if err != nil {
					return err
				}
				if artifact != artifacts[visited] || provider.Digest(data) != artifact.SHA256 {
					return errors.New("wrong selected bytes")
				}
				visited++
				return nil
			})
			if err != nil || visited != count || h.dials-before != 1 {
				t.Fatalf("batch did not share envelope: err=%v visits=%d dials=%d", err, visited, h.dials-before)
			}
			copies, inspects, engines := 0, 0, 0
			for _, args := range h.host.calls {
				if len(args) < 3 {
					continue
				}
				switch {
				case slices.Equal(args[1:3], []string{"container", "cp"}):
					if copies >= count || args[3] != h.host.container(t).id+":"+workRoot+"/"+artifacts[copies].Path {
						t.Fatal("unselected file crossed SSH", args)
					}
					copies++
				case slices.Equal(args[1:3], []string{"container", "inspect"}):
					inspects++
				case args[1] == "info":
					engines++
				}
			}
			if copies != count || inspects != 2 || engines != 2 {
				t.Fatalf("batch controls scaled with files: copies=%d inspect=%d engines=%d", copies, inspects, engines)
			}
			if h.host.created != 1 || h.host.starts != 1 {
				t.Fatal("collection created or restarted execution")
			}
		})
	}
}

func TestArtifactBatchFailureCancelsAndJoinsBeforeReturn(t *testing.T) {
	for _, fault := range []string{"visitor failure", "visitor panic", "visitor incomplete", "cancel", "digest", "link", "missing", "final identity", "final running"} {
		t.Run(fault, func(t *testing.T) {
			h, remote, artifacts := batchArtifactFixture(t, 3)
			h.host.calls = nil
			visited := 0
			call, cancel := context.WithCancel(t.Context())
			defer cancel()
			c := h.host.container(t)
			switch fault {
			case "digest":
				c.files["relay/work/"+artifacts[1].Path] = fakeFile{data: []byte("wrongbytes"), uid: nobody}
			case "link":
				c.files["relay/work/"+artifacts[1].Path] = fakeFile{link: true}
			case "missing":
				delete(c.files, "relay/work/"+artifacts[1].Path)
			}
			sentinel := errors.New("visitor stopped")
			err := h.adapter.FetchBatch(call, remote, artifacts, func(_ provider.Artifact, r io.Reader) error {
				visited++
				switch fault {
				case "visitor failure":
					return sentinel
				case "visitor panic":
					panic("private diagnostic")
				case "visitor incomplete":
					return nil
				case "cancel":
					cancel()
					return call.Err()
				}
				_, err := io.Copy(io.Discard, r)
				if visited == len(artifacts) {
					switch fault {
					case "final identity":
						h.host.engineID = "changed-engine-000000001"
					case "final running":
						c.status = "running"
					}
				}
				return err
			})
			if err == nil || provider.Reason(err, "") == "" || h.host.created != 1 || h.host.starts != 1 {
				t.Fatal("batch failure invented success or work", err)
			}
			if fault == "visitor failure" && !errors.Is(err, sentinel) {
				t.Fatal("visitor cause lost", err)
			}
			if fault == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal("batch ignored cancellation", err)
			}
			if strings.Contains(err.Error(), "private diagnostic") {
				t.Fatal("batch panic leaked diagnostic", err)
			}
			// Every producer is joined before return: no later copies can appear.
			before := h.host.commands("cp")
			if before > len(artifacts) {
				t.Fatal("batch retried a copy")
			}
			if fault == "visitor failure" || fault == "visitor panic" || fault == "visitor incomplete" || fault == "cancel" {
				if visited != 1 || before > 1 {
					t.Fatalf("visitor failure continued transfer: visits=%d copies=%d", visited, before)
				}
			}
		})
	}
}

func TestArtifactBatchEmptyStillRefusesChangedOrActiveRemote(t *testing.T) {
	for _, fault := range []string{"engine", "container", "running"} {
		h, remote, _ := batchArtifactFixture(t, 0)
		switch fault {
		case "engine":
			h.host.engineID = "changed-engine-000000001"
		case "container":
			h.host.container(t).labels[labelPrefix+"attempt"] = "other"
		case "running":
			h.host.container(t).status = "running"
		}
		visits := 0
		err := h.adapter.FetchBatch(t.Context(), remote, nil, func(provider.Artifact, io.Reader) error { visits++; return nil })
		if err == nil || visits != 0 {
			t.Fatal("cached blobs bypassed remote revalidation", fault, err)
		}
	}
}

func TestArtifactBatchRejectsUnpinnedRequestBeforeRemoteIO(t *testing.T) {
	h, remote, artifacts := batchArtifactFixture(t, 2)
	before := h.dials
	for _, targets := range [][]provider.Artifact{{artifacts[0], artifacts[0]}, {{Remote: remote, Path: "../secret", Bytes: 1, SHA256: provider.Digest([]byte("x"))}}} {
		err := h.adapter.FetchBatch(t.Context(), remote, targets, func(provider.Artifact, io.Reader) error { return nil })
		if err == nil || h.dials != before {
			t.Fatal("invalid batch reached remote", err)
		}
	}
}

func TestArtifactBatchEOFWaitsForProviderDigestCheck(t *testing.T) {
	h, remote, artifacts := batchArtifactFixture(t, 1)
	artifacts[0].SHA256 = provider.Digest([]byte("another-value"))
	var received bytes.Buffer
	var readErr error
	err := h.adapter.FetchBatch(t.Context(), remote, artifacts, func(_ provider.Artifact, r io.Reader) error { _, readErr = io.Copy(&received, r); return readErr })
	if err == nil || readErr == nil || received.Len() == 0 {
		t.Fatal("unverified bytes received a successful EOF", err, readErr)
	}
}

func TestArtifactBatchRejectsTruncatedOrExtraArchiveBeforeVerifiedEOF(t *testing.T) {
	for _, fault := range []string{"truncated", "extra member"} {
		t.Run(fault, func(t *testing.T) {
			h, remote, artifacts := batchArtifactFixture(t, 1)
			var wire bytes.Buffer
			archive := tar.NewWriter(&wire)
			payload := []byte("value-0000")
			if err := archive.WriteHeader(&tar.Header{Name: "file-0000.bin", Mode: 0600, Size: int64(len(payload)), Typeflag: tar.TypeReg}); err != nil {
				t.Fatal(err)
			}
			if _, err := archive.Write(payload); err != nil {
				t.Fatal(err)
			}
			if fault == "extra member" {
				if err := archive.WriteHeader(&tar.Header{Name: "unselected.bin", Mode: 0600, Size: 1, Typeflag: tar.TypeReg}); err != nil {
					t.Fatal(err)
				}
				if _, err := archive.Write([]byte("x")); err != nil {
					t.Fatal(err)
				}
			}
			if err := archive.Close(); err != nil {
				t.Fatal(err)
			}
			data := wire.Bytes()
			if fault == "truncated" {
				data = data[:100]
			}
			h.adapter.dial = func(context.Context, Target, Credential) (session, error) {
				return verificationSession{&scriptedExec{answer: func(args []string, in io.Reader, out io.Writer) (Result, error) {
					if len(args) > 3 && slices.Equal(args[1:3], []string{"container", "cp"}) {
						_, err := out.Write(data)
						return Result{}, err
					}
					return h.host.Run(t.Context(), nil, args, in, out)
				}}}, nil
			}
			var streamErr error
			err := h.adapter.FetchBatch(t.Context(), remote, artifacts, func(_ provider.Artifact, r io.Reader) error { _, streamErr = io.Copy(io.Discard, r); return streamErr })
			if err == nil || streamErr == nil {
				t.Fatal("malformed archive produced verified EOF", fault, err)
			}
		})
	}
}
