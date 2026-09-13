// Package devtool implements repository-local developer tasks without requiring GNU Make.
package devtool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// Runner executes developer tasks from the repository root.
type Runner struct {
	Dir    string
	Stdout io.Writer
	Stderr io.Writer
}

// Run executes one developer task.
func (r Runner) Run(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: devtool <fmt|fmt-check|mod-check|contract-check|contract-lock|vet|test|test-race|build|check>")
	}

	switch args[0] {
	case "fmt":
		return r.format(ctx, true)
	case "fmt-check":
		return r.format(ctx, false)
	case "mod-check":
		if err := r.command(ctx, nil, "go", "mod", "tidy", "-diff"); err != nil {
			return fmt.Errorf("module files are not tidy: %w", err)
		}
		return r.command(ctx, nil, "go", "mod", "verify")
	case "contract-check":
		return r.command(ctx, nil, "go", "run", "./cmd/contractcheck", "check")
	case "contract-lock":
		return r.command(ctx, nil, "go", "run", "./cmd/contractcheck", "write-lock")
	case "vet":
		return r.command(ctx, nil, "go", "vet", "./...")
	case "test":
		return r.command(ctx, nil, "go", "test", "./...")
	case "test-race":
		return r.command(ctx, nil, "go", "test", "-race", "./...")
	case "build":
		return r.command(ctx, []string{"CGO_ENABLED=0"}, "go", "build", "-trimpath", "./...")
	case "check":
		for _, task := range []string{"fmt-check", "mod-check", "contract-check", "vet", "test", "build"} {
			fmt.Fprintf(r.Stdout, "==> %s\n", task)
			if err := r.Run(ctx, []string{task}); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unknown developer task %q", args[0])
	}
}

func (r Runner) format(ctx context.Context, write bool) error {
	files, err := GoFiles(r.Dir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}

	args := make([]string, 0, len(files)+1)
	if write {
		args = append(args, "-w")
	} else {
		args = append(args, "-d")
	}
	args = append(args, files...)

	if write {
		return r.command(ctx, nil, "gofmt", args...)
	}

	var output bytes.Buffer
	cmd := exec.CommandContext(ctx, "gofmt", args...)
	cmd.Dir = r.Dir
	cmd.Stdout = &output
	cmd.Stderr = r.Stderr
	runErr := cmd.Run()
	if strings.TrimSpace(output.String()) != "" {
		fmt.Fprint(r.Stderr, output.String())
		return errors.New("Go files are not formatted; run scripts/dev.sh fmt (or the PowerShell/CMD equivalent)")
	}
	if runErr != nil {
		return fmt.Errorf("gofmt: %w", runErr)
	}
	return nil
}

func (r Runner) command(ctx context.Context, extraEnv []string, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = r.Dir
	cmd.Env = append(os.Environ(), extraEnv...)
	cmd.Stdout = r.Stdout
	cmd.Stderr = r.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

// GoFiles returns repository Go source files in deterministic order.
func GoFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".venv", "vendor":
				if path != root {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if strings.HasSuffix(entry.Name(), ".go") {
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			files = append(files, relative)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk Go files: %w", err)
	}
	sort.Strings(files)
	return files, nil
}
