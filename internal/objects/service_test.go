package objects_test

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/testsupport"
)

type catalog struct {
	*testsupport.Memory
	commit func(context.Context, domain.ObjectMetadata) error
	get    func(context.Context, domain.WorkspaceID, domain.ObjectID) (domain.ObjectMetadata, error)
}

func (c catalog) CommitObject(ctx context.Context, m domain.ObjectMetadata) error {
	if c.commit != nil {
		return c.commit(ctx, m)
	}
	return c.Memory.CommitObject(ctx, m)
}
func (c catalog) GetObject(ctx context.Context, w domain.WorkspaceID, id domain.ObjectID) (domain.ObjectMetadata, error) {
	if c.get != nil {
		return c.get(ctx, w, id)
	}
	return c.Memory.GetObject(ctx, w, id)
}

type blobWriter func(context.Context, domain.ObjectMetadata, io.Reader) (domain.ObjectMetadata, error)

func (b blobWriter) Put(ctx context.Context, m domain.ObjectMetadata, r io.Reader) (domain.ObjectMetadata, error) {
	return b(ctx, m, r)
}

func setup(t *testing.T) (*auth.Service, auth.Principal, *testsupport.Memory, *blobfs.Store) {
	t.Helper()
	mem := testsupport.NewMemory(auth.Workspace{ID: "a", Enabled: true}, auth.Workspace{ID: "b", Enabled: true})
	access, err := auth.New(mem, mem, nil)
	if err != nil {
		t.Fatal(err)
	}
	secret, _, err := access.Issue(context.Background(), "a", []auth.Scope{auth.Read, auth.Write}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := access.Authenticate(context.Background(), secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	fs, err := blobfs.New(filepath.Join(t.TempDir(), "blobs"), blobfs.Limits{MaxObjectBytes: 1024, MaxTotalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	return access, p, mem, fs
}

func TestUploadCommitsOnlyAfterVerifiedBytes(t *testing.T) {
	a, p, mem, fs := setup(t)
	var seen domain.ObjectMetadata
	repo := catalog{Memory: mem, commit: func(ctx context.Context, m domain.ObjectMetadata) error {
		r, err := fs.Open(ctx, m.WorkspaceID, m.ID)
		if err != nil {
			t.Fatal("commit before verified publication", err)
		}
		defer r.Close()
		body, err := io.ReadAll(r)
		if err != nil || string(body) != "payload" {
			t.Fatalf("wrong bytes: %s %v", body, err)
		}
		seen = m
		return mem.CommitObject(ctx, m)
	}}
	s, _ := objects.New(a, fs, repo)
	m, err := s.Upload(context.Background(), p, "a", 7, "", strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if m != seen || !m.Valid() || mem.ObjectCount() != 1 {
		t.Fatal("upload not committed")
	}
	got, err := s.Get(context.Background(), p, "a", m.ID)
	if err != nil || got != m {
		t.Fatalf("lookup: %v", err)
	}
	if _, err := s.Get(context.Background(), p, "b", m.ID); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("cross workspace", err)
	}
}

func TestFailedAndAmbiguousMetadataCommitPreserveBlob(t *testing.T) {
	for _, alreadyCommitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "before commit", true: "lost commit acknowledgement"}[alreadyCommitted], func(t *testing.T) {
			a, p, mem, fs := setup(t)
			var staged domain.ObjectMetadata
			repo := catalog{Memory: mem, commit: func(ctx context.Context, m domain.ObjectMetadata) error {
				staged = m
				if alreadyCommitted {
					if err := mem.CommitObject(ctx, m); err != nil {
						t.Fatal(err)
					}
				}
				return errors.New("private-path-secret-canary")
			}}
			s, _ := objects.New(a, fs, repo)
			m, err := s.Upload(context.Background(), p, "a", 7, "", strings.NewReader("payload"))
			if !errors.Is(err, objects.ErrUnavailable) || m.Valid() || strings.Contains(err.Error(), "secret-canary") {
				t.Fatalf("unsafe commit result: %v", err)
			}
			r, err := fs.Open(context.Background(), "a", staged.ID)
			if err != nil {
				t.Fatal("uncertain commit deleted complete blob", err)
			}
			r.Close()
			_, err = s.Get(context.Background(), p, "a", staged.ID)
			if alreadyCommitted && err != nil {
				t.Fatal("committed metadata lost", err)
			}
			if !alreadyCommitted && !errors.Is(err, objects.ErrNotFound) {
				t.Fatal("uncommitted bytes exposed by metadata API", err)
			}
		})
	}
}

func TestRevocationDuringUploadPreventsMetadataPublication(t *testing.T) {
	a, p, mem, fs := setup(t)
	writer := blobWriter(func(ctx context.Context, m domain.ObjectMetadata, r io.Reader) (domain.ObjectMetadata, error) {
		result, err := fs.Put(ctx, m, r)
		if err != nil {
			return result, err
		}
		if err := a.Revoke(ctx, p.TokenID()); err != nil {
			t.Fatal(err)
		}
		return result, nil
	})
	s, _ := objects.New(a, writer, mem)
	m, err := s.Upload(context.Background(), p, "a", 7, "", strings.NewReader("payload"))
	if !errors.Is(err, auth.ErrUnauthenticated) || m.Valid() || mem.ObjectCount() != 0 {
		t.Fatalf("revoked upload committed: %v", err)
	}
}

func TestUnscopedOrLyingRepositoriesFailClosed(t *testing.T) {
	a, p, mem, fs := setup(t)
	wrong := domain.ObjectMetadata{ID: "obj_b", WorkspaceID: "b", Bytes: 0, SHA256: domain.SHA256Digest(strings.Repeat("a", 64))}
	s, _ := objects.New(a, fs, catalog{Memory: mem, get: func(context.Context, domain.WorkspaceID, domain.ObjectID) (domain.ObjectMetadata, error) {
		return wrong, nil
	}})
	if _, err := s.Get(context.Background(), p, "a", "obj_b"); !errors.Is(err, objects.ErrNotFound) {
		t.Fatal("wrong owner leaked", err)
	}
	s, _ = objects.New(a, blobWriter(func(context.Context, domain.ObjectMetadata, io.Reader) (domain.ObjectMetadata, error) {
		return wrong, nil
	}), mem)
	if _, err := s.Upload(context.Background(), p, "a", -1, "", strings.NewReader("")); !errors.Is(err, objects.ErrUnavailable) || mem.ObjectCount() != 0 {
		t.Fatal("wrong blob identity committed", err)
	}
}

func TestAuthorizationAndFailedTransferNeverCommit(t *testing.T) {
	a, p, mem, fs := setup(t)
	s, _ := objects.New(a, fs, mem)
	cases := []struct {
		name      string
		principal auth.Principal
		workspace domain.WorkspaceID
		length    int64
		digest    domain.SHA256Digest
	}{
		{"anonymous", auth.Principal{}, "a", 7, ""}, {"foreign", p, "b", 7, ""}, {"length", p, "a", 8, ""},
		{"digest", p, "a", 7, domain.SHA256Digest(strings.Repeat("a", 64))}, {"invalid", p, "a", -2, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if m, err := s.Upload(context.Background(), tc.principal, tc.workspace, tc.length, tc.digest, strings.NewReader("payload")); err == nil || m.Valid() {
				t.Fatal("invalid upload accepted")
			}
		})
	}
	if mem.ObjectCount() != 0 {
		t.Fatal("failed transfer committed")
	}
}
