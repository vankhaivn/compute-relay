package objects_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/localinput"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/packaging"
)

func TestImportUsesVerifiedUploadAndOwnershipCommit(t *testing.T) {
	a, p, mem, fs := setup(t)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	inputs, err := localinput.New([]localinput.RootSpec{{Name: "source", Path: root, Workspaces: []string{"a"}}}, packaging.DefaultLimits(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer inputs.Close()
	svc, _ := objects.New(a, fs, mem)
	importer, err := objects.NewImporter(svc, inputs)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := importer.Import(context.Background(), p, "a", localinput.Request{Root: "source", Kind: "file", Path: "file.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if !meta.Valid() || mem.ObjectCount() != 1 {
		t.Fatal("metadata not committed")
	}
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("changed!"), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := fs.Open(context.Background(), "a", meta.ID)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(f)
	f.Close()
	if err != nil || string(b) != "snapshot" {
		t.Fatal("source edit mutated imported object")
	}
	for _, request := range []localinput.Request{{Root: "missing", Kind: "file", Path: "file.txt"}, {Root: "source", Kind: "file", Path: "../escape"}, {Root: "source", Kind: "file", Path: "file.txt", SHA256: strings.Repeat("0", 64)}} {
		if _, err := importer.Import(context.Background(), p, "a", request); err == nil {
			t.Fatal("unsafe import committed")
		}
	}
	if _, err := importer.Import(context.Background(), p, "b", localinput.Request{Root: "source", Kind: "file", Path: "file.txt"}); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("workspace escalation", err)
	}
	if err := a.Revoke(context.Background(), p.TokenID()); err != nil {
		t.Fatal(err)
	}
	if _, err := importer.Import(context.Background(), p, "a", localinput.Request{Root: "source", Kind: "file", Path: "file.txt"}); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("revoked token allowed import", err)
	}
	if mem.ObjectCount() != 1 {
		t.Fatal("failed import altered metadata")
	}
}
func TestImportPreservesBlobAfterAmbiguousCommit(t *testing.T) {
	a, p, mem, fs := setup(t)
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("snapshot"), 0o600); err != nil {
		t.Fatal(err)
	}
	inputs, err := localinput.New([]localinput.RootSpec{{Name: "source", Path: root, Workspaces: []string{"a"}}}, packaging.DefaultLimits(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer inputs.Close()
	var committed domain.ObjectMetadata
	repo := catalog{Memory: mem, commit: func(ctx context.Context, m domain.ObjectMetadata) error {
		committed = m
		if err := mem.CommitObject(ctx, m); err != nil {
			t.Fatal(err)
		}
		return errors.New("lost-acknowledgement")
	}}
	svc, _ := objects.New(a, fs, repo)
	importer, _ := objects.NewImporter(svc, inputs)
	if _, err := importer.Import(context.Background(), p, "a", localinput.Request{Root: "source", Kind: "file", Path: "file.txt"}); !errors.Is(err, objects.ErrUnavailable) {
		t.Fatal(err)
	}
	content, err := fs.Open(context.Background(), "a", committed.ID)
	if err != nil {
		t.Fatal("possibly committed import was deleted", err)
	}
	content.Close()
}
