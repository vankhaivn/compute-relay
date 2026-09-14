// Command packagesmoke runs a finite, synthetic local packaging/import check. No provider,
// credentials or uploaded command execution participates. It removes its temporary files.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/bundlectl"
	"github.com/vankhaivn/compute-relay/internal/localinput"
	"github.com/vankhaivn/compute-relay/internal/packaging"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "packaging smoke failed:", err)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir, err := os.MkdirTemp("", "compute-relay-packagesmoke-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	root := filepath.Join(dir, "project")
	if err := os.Mkdir(root, 0o700); err != nil {
		return err
	}
	data := strings.Repeat("synthetic input\n", 16384)
	for name, content := range map[string]string{"main.py": "raise RuntimeError('must never execute locally')\n", "input.txt": data, ".env": "must stay excluded"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
			return err
		}
	}
	var preview bytes.Buffer
	if err := bundlectl.Run(ctx, []string{"preview", "--root", root, "--include", "main.py", "--include", "input.txt"}, &preview, io.Discard); err != nil {
		return err
	}
	var p packaging.Preview
	if err := json.Unmarshal(preview.Bytes(), &p); err != nil {
		return err
	}
	archive := filepath.Join(dir, "job.tar.gz")
	if err := bundlectl.Run(ctx, []string{"create", "--root", root, "--include", "main.py", "--include", "input.txt", "--output", archive, "--expect-manifest-sha256", p.ManifestSHA256}, io.Discard, io.Discard); err != nil {
		return err
	}
	if err := bundlectl.Run(ctx, []string{"inspect", "--file", archive}, io.Discard, io.Discard); err != nil {
		return err
	}
	m, err := localinput.New([]localinput.RootSpec{{Name: "source", Path: root, Workspaces: []string{"a"}}}, packaging.DefaultLimits(), 2<<20)
	if err != nil {
		return err
	}
	defer m.Close()
	stream, err := m.Open(ctx, "a", localinput.Request{Root: "source", Kind: "file", Path: "input.txt"})
	if err != nil {
		return err
	}
	copied, err := io.ReadAll(stream)
	_ = stream.Close()
	if err != nil || string(copied) != data {
		return errors.New("raw snapshot mismatch")
	}
	if s, err := m.Open(ctx, "b", localinput.Request{Root: "source", Kind: "file", Path: "input.txt"}); !errors.Is(err, localinput.ErrForbidden) {
		if s != nil {
			s.Close()
		}
		return errors.New("workspace root isolation failed")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "passed-offline", "bundle_files": len(p.Manifest.Files), "raw_input_bytes": len(copied), "manifest_identity_verified": true, "workspace_root_isolation": true, "provider_calls": 0, "workload_executed": false})
