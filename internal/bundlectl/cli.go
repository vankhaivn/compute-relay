// Package bundlectl implements local, credential-free bundle commands.
package bundlectl

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/packaging"
)

type selections []string

func (s *selections) String() string         { return strings.Join(*s, ",") }
func (s *selections) Set(value string) error { *s = append(*s, value); return nil }

// Run never uses a shell, credentials or provider. Output is a JSON preview/receipt.
func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: bundle <preview|create|inspect> [flags]")
	}
	action := args[0]
	f := flag.NewFlagSet("bundle "+action, flag.ContinueOnError)
	f.SetOutput(stderr)
	root := f.String("root", "", "explicit project directory")
	output := f.String("output", "", "new bundle path outside project (create only)")
	input := f.String("file", "", "existing bundle (inspect only)")
	expected := f.String("expect-manifest-sha256", "", "require the digest from a reviewed preview")
	var includes selections
	f.Var(&includes, "include", "explicit relative file/directory; repeatable")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if action == "inspect" {
		if *input == "" || *root != "" || *output != "" || len(includes) > 0 || *expected != "" {
			return errors.New("inspect requires only --file")
		}
		// Only inspect a regular local file; special inputs could block without a filesystem
		// deadline. Archive members are never extracted or executed.
		info, err := os.Lstat(*input)
		if err != nil || !info.Mode().IsRegular() {
			return packaging.ErrPath
		}
		fd, err := os.Open(*input)
		if err != nil {
			return packaging.ErrUnavailable
		}
		defer fd.Close()
		opened, err := fd.Stat()
		if err != nil || !os.SameFile(info, opened) {
			return packaging.ErrChanged
		}
		report, err := packaging.Inspect(ctx, fd, packaging.DefaultLimits())
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(report)
	}
	if action != "preview" && action != "create" {
		return errors.New("unknown bundle command")
	}
	if *root == "" || len(includes) == 0 || *input != "" || action == "preview" && (*output != "" || *expected != "") {
		return errors.New("supply --root and explicit --include selections")
	}
	project, err := packaging.OpenProject(*root, packaging.DefaultLimits(), nil)
	if err != nil {
		return err
	}
	defer project.Close()
	plan, err := project.Prepare(ctx, includes)
	if err != nil {
		return err
	}
	if action == "preview" {
		return json.NewEncoder(stdout).Encode(plan.Preview())
	}
	if *output == "" {
		return errors.New("create requires --output")
	}
	if *expected != "" && plan.Preview().ManifestSHA256 != *expected {
		return packaging.ErrChanged
	}
	// Canonicalize the destination parent, not a possibly malicious destination symlink.
	parent, err := filepath.EvalSymlinks(filepath.Dir(*output))
	if err != nil {
		return packaging.ErrUnavailable
	}
	parent, err = filepath.Abs(parent)
	if err != nil {
		return packaging.ErrUnavailable
	}
	source, err := filepath.EvalSymlinks(*root)
	if err != nil {
		return packaging.ErrUnavailable
	}
	source, err = filepath.Abs(source)
	if err != nil {
		return packaging.ErrUnavailable
	}
	target := filepath.Join(parent, filepath.Base(*output))
	relative, err := filepath.Rel(source, target)
	if err != nil {
		return packaging.ErrPath
	}
	if relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("bundle output must be outside project root")
	}
	temp, err := os.CreateTemp(parent, ".compute-relay-bundle-*")
	if err != nil {
		return packaging.ErrUnavailable
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	report, err := plan.Write(ctx, temp)
	if err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return packaging.ErrUnavailable
	}
	if err := temp.Close(); err != nil {
		return packaging.ErrUnavailable
	}
	// Hard-link publication is atomic and fails if destination exists. No overwrite/rename
	// fallback on filesystems that cannot support it. The temporary name is removed above.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Link(temp.Name(), target); err != nil {
		return fmt.Errorf("bundle publication failed (destination must be new): %w", packaging.ErrUnavailable)
	}
	return json.NewEncoder(stdout).Encode(report)
}
