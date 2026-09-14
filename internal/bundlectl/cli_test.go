package bundlectl

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/packaging"
)

func TestPreviewCreateInspectNoOverwrite(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(t.TempDir(), "job.tar.gz")
	// This is untrusted workload text. The command must only package it, never execute it.
	marker := filepath.Join(t.TempDir(), "must-not-exist")
	code := "open(" + fmtQuote(marker) + ", 'w').write('executed')\n"
	if err := os.WriteFile(filepath.Join(root, "main.py"), []byte(code), 0o600); err != nil {
		t.Fatal(err)
	}
	var preview bytes.Buffer
	if err := Run(context.Background(), []string{"preview", "--root", root, "--include", "main.py"}, &preview, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var p packaging.Preview
	if err := json.Unmarshal(preview.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	args := []string{"create", "--root", root, "--include", "main.py", "--output", out, "--expect-manifest-sha256", p.ManifestSHA256}
	var receipt bytes.Buffer
	if err := Run(context.Background(), args, &receipt, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
		t.Fatal("overwrote existing output")
	}
	current, _ := os.ReadFile(out)
	if !bytes.Equal(first, current) {
		t.Fatal("destination changed")
	}
	var inspected bytes.Buffer
	if err := Run(context.Background(), []string{"inspect", "--file", out}, &inspected, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(receipt.Bytes(), inspected.Bytes()) {
		t.Fatal("inspect receipt differs")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("workload executed")
	}
}
func fmtQuote(s string) string { b, _ := json.Marshal(s); return string(b) }
func TestErrorsDoNotPublishBundle(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "main.py"), []byte("print(1)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "bundle.tar.gz")
	cases := [][]string{{}, {"unknown"}, {"create", "--root", root, "--output", out}, {"create", "--root", root, "--include", ".", "--output", out}, {"create", "--root", root, "--include", "main.py", "--output", filepath.Join(root, "bundle.tar.gz")}, {"create", "--root", root, "--include", "main.py", "--output", out, "--expect-manifest-sha256", strings.Repeat("0", 64)}, {"inspect"}, {"preview", "--root", root, "--include", "main.py", "positional"}}
	for _, args := range cases {
		if err := Run(context.Background(), args, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatalf("invalid args accepted: %v", args)
		}
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatal("failed create published output")
	}
}
