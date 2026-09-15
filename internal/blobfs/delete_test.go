package blobfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func TestDeleteExactIdentityAndRestart(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "blobs")
	limits := Limits{MaxObjectBytes: 1024, MaxTotalBytes: 4096}
	s, err := New(root, limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	m, err := s.Put(ctx, domain.ObjectMetadata{ID: "exact", WorkspaceID: "a", Bytes: -1}, strings.NewReader("retained bytes"))
	if err != nil {
		t.Fatal(err)
	}
	wrong := m
	wrong.SHA256 = domain.SHA256Digest(strings.Repeat("0", 64))
	if _, err = s.Delete(ctx, wrong); !errors.Is(err, ErrCorrupt) {
		t.Fatal("incorrect deletion declaration accepted", err)
	}
	s.rename = func(string, string) error { return errors.New("injected rename failure") }
	if _, err = s.Delete(ctx, m); !errors.Is(err, ErrUnavailable) || s.used != m.Bytes || s.count != 1 {
		t.Fatal("failed deletion lost accounting", err)
	}
	s.rename = os.Rename
	if absent, err := s.Delete(ctx, m); err != nil || absent || s.used != 0 || s.count != 0 {
		t.Fatal("exact deletion failed", absent, err)
	}
	if absent, err := s.Delete(ctx, m); err != nil || !absent || s.used != 0 {
		t.Fatal("deletion was not idempotent", absent, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(root, limits)
	if err != nil {
		t.Fatal(err)
	}
	if absent, err := s.Delete(ctx, m); err != nil || !absent {
		t.Fatal("restart deletion", absent, err)
	}
}

func TestDeleteRejectsChangedBytesAndUnknownChildren(t *testing.T) {
	for _, mode := range []string{"changed", "extra"} {
		t.Run(mode, func(t *testing.T) {
			s, err := New(filepath.Join(t.TempDir(), "blobs"), Limits{MaxObjectBytes: 1024, MaxTotalBytes: 4096})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			m, err := s.Put(context.Background(), domain.ObjectMetadata{ID: "target", WorkspaceID: "a", Bytes: -1}, strings.NewReader("original"))
			if err != nil {
				t.Fatal(err)
			}
			base := filepath.Join(s.root, "workspaces", encodedID("a"), "objects", encodedID("target"))
			name := "data"
			if mode == "extra" {
				name = "unknown-file"
			}
			if err := os.WriteFile(filepath.Join(base, name), []byte("modified"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Delete(context.Background(), m); !errors.Is(err, ErrCorrupt) {
				t.Fatal("unverified material deleted", err)
			}
			if _, err := os.Stat(base); err != nil {
				t.Fatal("evidence lost", err)
			}
		})
	}
}

func TestDeleteFinishesOwnedQuarantineAfterReopen(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "blobs")
	limits := Limits{MaxObjectBytes: 1024, MaxTotalBytes: 4096}
	s, err := New(root, limits)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.Put(ctx, domain.ObjectMetadata{ID: "target", WorkspaceID: "a", Bytes: -1}, strings.NewReader("original"))
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(s.root, "workspaces", encodedID("a"))
	retired := filepath.Join(base, "temporary", "upload-retired-"+encodedID("target"))
	if err := os.Rename(filepath.Join(base, "objects", encodedID("target")), retired); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(root, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if absent, err := s.Delete(ctx, m); err != nil || !absent || s.used != 0 || s.count != 0 {
		t.Fatal("quarantine recovery failed", absent, err)
	}
	if _, err := os.Stat(retired); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("quarantine not removed", err)
	}
}
