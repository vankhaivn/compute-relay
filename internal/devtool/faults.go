package devtool

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

//go:embed fault-matrix.json
var faultCatalog []byte

var faultTestName = regexp.MustCompile(`^Test[A-Z0-9][A-Za-z0-9_]*$`)

const (
	faultModule    = "github.com/vankhaivn/compute-relay/"
	maxFaultLine   = 1 << 20
	maxFaultOutput = 64 << 20
)

type faultTest struct {
	File string `json:"file"`
	Name string `json:"name"`
}
type faultCase struct {
	Number   int         `json:"number"`
	Scenario string      `json:"scenario"`
	Tests    []faultTest `json:"tests"`
}

func (t faultTest) key() string { return faultModule + path.Dir(t.File) + ":" + t.Name }

// This catalog is reviewed repository evidence, not user-supplied test commands.
// Source presence and a fresh pass are necessary but do not prove that assertions
// are adequate; semantic coverage and live-provider limitations still need review.
func loadFaultCases(root string) ([]faultCase, error) {
	return checkFaultCatalog(root, faultCatalog)
}

func checkFaultCatalog(root string, raw []byte) ([]faultCase, error) {
	var catalog struct {
		Version int         `json:"version"`
		Cases   []faultCase `json:"cases"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&catalog) != nil || d.Decode(new(any)) != io.EOF || catalog.Version != 1 || len(catalog.Cases) != 25 {
		return nil, errors.New("fault matrix must contain exactly 25 version-1 cases")
	}
	proposal, err := os.ReadFile(filepath.Join(root, "docs", "proposal.md"))
	if err != nil {
		return nil, fmt.Errorf("read approved proposal: %w", err)
	}
	expected, err := proposalFaultScenarios(proposal)
	if err != nil {
		return nil, err
	}
	functions := map[string]map[string]bool{}
	scenarios := map[string]bool{}
	for i, c := range catalog.Cases {
		if c.Number != i+1 || c.Scenario == "" || scenarios[c.Scenario] || c.Scenario != expected[i] || len(c.Tests) == 0 || len(c.Tests) > 8 {
			return nil, fmt.Errorf("invalid or untraceable fault case %d", i+1)
		}
		scenarios[c.Scenario] = true
		seen := map[string]bool{}
		for _, ref := range c.Tests {
			if !strings.HasPrefix(ref.File, "internal/") || path.Clean(ref.File) != ref.File || strings.ContainsAny(ref.File, "\\:") || !strings.HasSuffix(ref.File, "_test.go") || !faultTestName.MatchString(ref.Name) || seen[ref.key()] {
				return nil, fmt.Errorf("invalid test reference for FM%02d", c.Number)
			}
			seen[ref.key()] = true
			if functions[ref.File] == nil {
				file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, filepath.FromSlash(ref.File)), nil, 0)
				if err != nil {
					return nil, fmt.Errorf("parse %s: %w", ref.File, err)
				}
				functions[ref.File] = map[string]bool{}
				for _, decl := range file.Decls {
					if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil {
						functions[ref.File][fn.Name.Name] = true
					}
				}
			}
			if !functions[ref.File][ref.Name] {
				return nil, fmt.Errorf("missing evidence %s::%s", ref.File, ref.Name)
			}
		}
	}
	return catalog.Cases, nil
}

type faultOutcome struct{ started, passed bool }
type faultEvent struct {
	Action  string
	Package string
	Test    string
	Output  string
}

// Only stdout from this invocation's `go test -json -count=1` is accepted.
// Bound both incomplete records and the aggregate stream; never load a saved log
// as qualification or fall back to a green package without the named tests.
type faultStream struct {
	tests    map[string]faultOutcome
	packages map[string]faultOutcome
	pending  []byte
	tail     []byte
	total    int
	failure  error
	cancel   context.CancelFunc
}

func newFaultStream(cases []faultCase, cancel context.CancelFunc) *faultStream {
	s := &faultStream{tests: map[string]faultOutcome{}, packages: map[string]faultOutcome{}, cancel: cancel}
	for _, c := range cases {
		for _, ref := range c.Tests {
			s.tests[ref.key()] = faultOutcome{}
			s.packages[faultModule+path.Dir(ref.File)] = faultOutcome{}
		}
	}
	return s
}
func (s *faultStream) reject(err error) (int, error) {
	if s.failure == nil {
		s.failure = err
		s.cancel()
	}
	return 0, s.failure
}
func (s *faultStream) Write(data []byte) (int, error) {
	if s.failure != nil {
		return 0, s.failure
	}
	if len(data) > maxFaultOutput-s.total {
		return s.reject(errors.New("fault-test output exceeds 64 MiB"))
	}
	s.total += len(data)
	original := len(data)
	for len(data) > 0 {
		end := bytes.IndexByte(data, '\n')
		part := data
		if end >= 0 {
			part = data[:end]
		}
		if len(part) > maxFaultLine-len(s.pending) {
			return s.reject(errors.New("fault-test event exceeds 1 MiB"))
		}
		s.pending = append(s.pending, part...)
		if end < 0 {
			break
		}
		var e faultEvent
		if err := json.Unmarshal(s.pending, &e); err != nil {
			return s.reject(errors.New("invalid go test JSON event"))
		}
		s.pending = s.pending[:0]
		if err := s.event(e); err != nil {
			return s.reject(err)
		}
		data = data[end+1:]
	}
	return original, nil
}
func (s *faultStream) event(e faultEvent) error {
	if e.Action == "fail" || e.Action == "build-fail" {
		return fmt.Errorf("fault evidence failed: %s::%s", e.Package, e.Test)
	}
	if e.Action == "output" {
		s.tail = append(s.tail, e.Output...)
		if len(s.tail) > 32<<10 {
			s.tail = append([]byte(nil), s.tail[len(s.tail)-(32<<10):]...)
		}
		return nil
	}
	p, relevant := s.packages[e.Package]
	if !relevant {
		return nil // build output may name a dependency, never a qualifying test.
	}
	if e.Test == "" {
		switch e.Action {
		case "start":
			if p.started || p.passed {
				return errors.New("duplicate package start")
			}
			p.started = true
		case "pass":
			if !p.started || p.passed {
				return errors.New("package pass without one start")
			}
			for key, test := range s.tests {
				if strings.HasPrefix(key, e.Package+":") && !test.passed {
					return fmt.Errorf("package passed without required test %s", key)
				}
			}
			p.passed = true
		case "skip":
			return fmt.Errorf("required package skipped: %s", e.Package)
		default:
			return fmt.Errorf("unknown package event %q", e.Action)
		}
		s.packages[e.Package] = p
		return nil
	}
	root := strings.SplitN(e.Test, "/", 2)[0]
	key := e.Package + ":" + root
	test, required := s.tests[key]
	if !required {
		return nil
	}
	if !p.started || p.passed {
		return errors.New("test event outside its running package")
	}
	if e.Action == "skip" {
		return fmt.Errorf("required evidence or subtest skipped: %s::%s", e.Package, e.Test)
	}
	switch e.Action {
	case "run":
		if e.Test == root {
			if test.started || test.passed {
				return fmt.Errorf("duplicate evidence run: %s", key)
			}
			test.started = true
		}
	case "pass":
		if e.Test == root {
			if !test.started || test.passed {
				return fmt.Errorf("evidence pass without one run: %s", key)
			}
			test.passed = true
		}
	case "pause", "cont":
	default:
		return fmt.Errorf("unknown test event %q", e.Action)
	}
	s.tests[key] = test
	return nil
}
func (s *faultStream) finish() error {
	if s.failure != nil {
		return s.failure
	}
	if len(s.pending) != 0 {
		return errors.New("unterminated go test JSON event")
	}
	for key, test := range s.tests {
		if !test.started || !test.passed {
			return fmt.Errorf("required test did not run and pass: %s", key)
		}
	}
	for pkg, p := range s.packages {
		if !p.started || !p.passed {
			return fmt.Errorf("required package did not finish: %s", pkg)
		}
	}
	return nil
}

func (r Runner) faults(ctx context.Context) error {
	cases, err := loadFaultCases(r.Dir)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	stream := newFaultStream(cases, cancel)
	packages, names := []string{}, []string{}
	unique := map[string]bool{}
	for pkg := range stream.packages {
		packages = append(packages, "./"+strings.TrimPrefix(pkg, faultModule))
	}
	for _, c := range cases {
		for _, ref := range c.Tests {
			if !unique[ref.Name] {
				names = append(names, regexp.QuoteMeta(ref.Name))
				unique[ref.Name] = true
			}
		}
	}
	sort.Strings(packages)
	sort.Strings(names)
	args := append([]string{"test", "-json", "-count=1", "-timeout=5m", "-run", "^(" + strings.Join(names, "|") + ")$"}, packages...)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir, cmd.Stdout, cmd.Stderr = r.Dir, stream, r.Stderr
	cmd.WaitDelay = 2 * time.Second
	runErr := cmd.Run()
	evidenceErr := stream.finish()
	if err := errors.Join(runErr, evidenceErr, ctx.Err()); err != nil {
		if r.Stderr != nil {
			_, _ = r.Stderr.Write(stream.tail)
		}
		return fmt.Errorf("fault matrix not qualified: %w", err)
	}
	for _, c := range cases {
		if _, err := fmt.Fprintf(r.Stdout, "FM%02d PASS %s\n", c.Number, c.Scenario); err != nil {
			return err
		}
		for _, ref := range c.Tests {
			if _, err := fmt.Fprintf(r.Stdout, "  %s::%s\n", ref.File, ref.Name); err != nil {
				return err
			}
		}
	}
	_, err = fmt.Fprintf(r.Stdout, "fault-matrix: 25/25 passed-offline; %d named tests; %s %s/%s; no live-provider qualification\n", len(stream.tests), runtime.Version(), runtime.GOOS, runtime.GOARCH)
	return err
}
