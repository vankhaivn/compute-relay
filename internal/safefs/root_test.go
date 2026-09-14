package safefs

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestPortablePathCorpus(t *testing.T) {
	for _, name := range []string{"", "..", "../outside", "/abs", "a/../b", `a\b`, "C:foo", "nul.txt", "con", "a/aux.py", "COM1", "LPT9", "a\x00b", "é", "a//b", "a/", "a .", ". "} {
		if ValidPath(name) {
			t.Fatal("unsafe path accepted", name)
		}
	}
	for _, name := range []string{".", "a", "src/main.py", "a-b/c_d", ".hidden"} {
		if !ValidPath(name) {
			t.Fatal("safe path rejected", name)
		}
	}
}
func TestRootRejectsLinksAndStaysAnchored(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("inside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "file"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	f, err := r.Open("file")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(f)
	f.Close()
	if err != nil || string(b) != "inside" {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(root, "file"), filepath.Join(root, "hard")); err == nil {
		if f, err := r.Open("hard"); err == nil {
			f.Close()
			t.Fatal("hardlink accepted")
		}
		_ = os.Remove(filepath.Join(root, "hard"))
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Log("symlink fixture unavailable", err)
	} else {
		if f, err := r.Open("link/file"); err == nil {
			f.Close()
			t.Fatal("symlink parent followed")
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if f, err := r.Open("file"); err == nil {
		f.Close()
		t.Fatal("closed root accepted")
	}
}
