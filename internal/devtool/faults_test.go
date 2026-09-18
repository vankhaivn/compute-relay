package devtool

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFaultCatalogMatchesProposalAndSource(t *testing.T) {
	cases, err := loadFaultCases(filepath.Join("..", ".."))
	if err != nil || len(cases) != 25 {
		t.Fatal("fault catalog drift", err)
	}
}

func TestFaultCatalogRejectsMissingOrUntraceableEvidence(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"docs/development", "internal/fixture"} {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(dir)), 0700); err != nil {
			t.Fatal(err)
		}
	}
	ref := faultTest{File: "internal/fixture/fixture_test.go", Name: "TestProof"}
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(ref.File)), []byte("package fixture\nimport \"testing\"\nfunc TestProof(t *testing.T) {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cases := make([]faultCase, 25)
	for i := range cases {
		cases[i] = faultCase{Number: i + 1, Scenario: fmt.Sprintf("Case %d.", i+1), Tests: []faultTest{ref}}
	}
	if err := os.WriteFile(filepath.Join(root, "docs", "development", "proposal.md"), []byte(proposalList()), 0600); err != nil {
		t.Fatal(err)
	}
	encode := func(cases []faultCase) []byte {
		raw, err := json.Marshal(map[string]any{"version": 1, "cases": cases})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	good := encode(cases)
	if _, err := checkFaultCatalog(root, good); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"count", "number", "scenario", "substring", "reordered", "duplicate-scenario", "empty", "missing-function", "missing-file", "traversal", "duplicate-reference", "unknown-field", "trailing"} {
		t.Run(mode, func(t *testing.T) {
			var copy struct{ Cases []faultCase }
			if err := json.Unmarshal(good, &copy); err != nil {
				t.Fatal(err)
			}
			c := copy.Cases
			switch mode {
			case "count":
				c = c[:24]
			case "number":
				c[0].Number = 2
			case "scenario":
				c[0].Scenario = "Not in the approved proposal"
			case "substring":
				c[0].Scenario = "Case 1"
			case "reordered":
				c[0].Scenario, c[1].Scenario = c[1].Scenario, c[0].Scenario
			case "duplicate-scenario":
				c[1].Scenario = c[0].Scenario
			case "empty":
				c[0].Tests = nil
			case "missing-function":
				c[0].Tests[0].Name = "TestMissing"
			case "missing-file":
				c[0].Tests[0].File = "internal/fixture/missing_test.go"
			case "traversal":
				c[0].Tests[0].File = "internal/../../outside_test.go"
			case "duplicate-reference":
				c[0].Tests = append(c[0].Tests, c[0].Tests[0])
			}
			raw := encode(c)
			if mode == "unknown-field" {
				raw = append([]byte(`{"unknown":1,`), raw[1:]...)
			}
			if mode == "trailing" {
				raw = append(raw, []byte(` {}`)...)
			}
			if _, err := checkFaultCatalog(root, raw); err == nil {
				t.Fatal("invalid catalog accepted")
			}
		})
	}
}

func proofCases() []faultCase {
	return []faultCase{{Number: 1, Scenario: "fixture", Tests: []faultTest{{File: "internal/fixture/fixture_test.go", Name: "TestProof"}}}}
}
func proofEvents() []faultEvent {
	pkg := faultModule + "internal/fixture"
	return []faultEvent{
		{Action: "start", Package: pkg},
		{Action: "run", Package: pkg, Test: "TestProof"},
		{Action: "output", Package: pkg, Test: "TestProof", Output: "ordinary fixture output\n"},
		{Action: "pass", Package: pkg, Test: "TestProof"},
		{Action: "pass", Package: pkg},
	}
}
func eventBytes(t testing.TB, events []faultEvent) []byte {
	t.Helper()
	var b bytes.Buffer
	for _, event := range events {
		if err := json.NewEncoder(&b).Encode(event); err != nil {
			t.Fatal(err)
		}
	}
	return b.Bytes()
}
func TestFaultStreamRequiresExecutedTestsAndCompletePackages(t *testing.T) {
	pkg := faultModule + "internal/fixture"
	for _, mode := range []string{"pass", "missing", "package-only", "wrong-package", "test-skip", "subtest-skip", "test-fail", "build-fail", "no-run", "no-package-end", "duplicate-run", "duplicate-pass", "unknown-action", "truncated", "malformed", "unrelated-skip"} {
		t.Run(mode, func(t *testing.T) {
			events := proofEvents()
			switch mode {
			case "missing":
				events = nil
			case "package-only":
				events = []faultEvent{events[0], events[4]}
			case "wrong-package":
				for i := range events {
					events[i].Package += "/other"
				}
			case "test-skip", "test-fail":
				events[3].Action = strings.TrimPrefix(mode, "test-")
			case "subtest-skip":
				events = append(events[:3], faultEvent{Action: "skip", Package: pkg, Test: "TestProof/required-case"}, events[3], events[4])
			case "build-fail":
				events = []faultEvent{{Action: "build-fail"}}
			case "no-run":
				events = append(events[:1], events[2:]...)
			case "no-package-end":
				events = events[:4]
			case "duplicate-run":
				events = append(events[:2], append([]faultEvent{events[1]}, events[2:]...)...)
			case "duplicate-pass":
				events = append(events[:4], events[3], events[4])
			case "unknown-action":
				events[1].Action = "future-action"
			case "unrelated-skip":
				events = append(events[:1], append([]faultEvent{{Action: "skip", Package: pkg, Test: "TestHelper"}}, events[1:]...)...)
			}
			raw := eventBytes(t, events)
			if mode == "truncated" {
				raw = raw[:len(raw)-1]
			}
			if mode == "malformed" {
				raw = []byte("not JSON\n")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream := newFaultStream(proofCases(), cancel)
			// Split every record across many writes like os/exec's copying goroutine.
			for len(raw) > 0 {
				n := min(7, len(raw))
				if _, err := stream.Write(raw[:n]); err != nil {
					break
				}
				raw = raw[n:]
			}
			good := mode == "pass" || mode == "unrelated-skip"
			if err := stream.finish(); (err == nil) != good {
				t.Fatal("incorrect qualification", mode, err)
			}
			if stream.failure != nil && ctx.Err() == nil {
				t.Fatal("malformed stream did not cancel the command")
			}
		})
	}
}
func TestFaultStreamBoundsRecordsOutputAndDiagnostics(t *testing.T) {
	for _, mode := range []string{"record", "aggregate"} {
		t.Run(mode, func(t *testing.T) {
			cancelled := false
			s := newFaultStream(proofCases(), func() { cancelled = true })
			data := bytes.Repeat([]byte{'x'}, maxFaultLine+1)
			if mode == "aggregate" {
				s.total = maxFaultOutput
				data = []byte("{}\n")
			}
			if _, err := s.Write(data); err == nil || !cancelled || s.finish() == nil {
				t.Fatal("unbounded test stream accepted")
			}
		})
	}
	s := newFaultStream(proofCases(), func() {})
	if err := s.event(faultEvent{Action: "output", Output: strings.Repeat("x", 1<<20)}); err != nil || len(s.tail) != 32<<10 {
		t.Fatal("diagnostic tail was not bounded", err)
	}
}
func FuzzFaultEventStream(f *testing.F) {
	f.Add(eventBytes(f, proofEvents()))
	f.Add([]byte(`{"Action":"pass"}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			return
		}
		s := newFaultStream(proofCases(), func() {})
		_, _ = s.Write(raw)
		_ = s.finish()
	})
}

// A temporary standard-library module verifies the actual go test event protocol.
// It contains only repository-owned pass/skip/fail fixtures, never a workload.
func TestFaultStreamConsumesRealGoTestEvents(t *testing.T) {
	for _, mode := range []string{"pass", "skip", "fail"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "internal", "fixture")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module github.com/vankhaivn/compute-relay\n\ngo 1.23\n"), 0600); err != nil {
				t.Fatal(err)
			}
			body := ""
			switch mode {
			case "skip":
				body = `t.Run("required", func(t *testing.T) { t.Skip("fixture skip") })`
			case "fail":
				body = `t.Fatal("fixture failure")`
			}
			source := "package fixture\nimport \"testing\"\nfunc TestProof(t *testing.T) { " + body + " }\n"
			if err := os.WriteFile(filepath.Join(dir, "fixture_test.go"), []byte(source), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			stream := newFaultStream(proofCases(), cancel)
			cmd := exec.CommandContext(ctx, "go", "test", "-json", "-count=1", "-run=^TestProof$", "./internal/fixture")
			cmd.Dir, cmd.Stdout = root, stream
			cmd.Env = append(os.Environ(), "GOWORK=off")
			cmd.WaitDelay = time.Second
			runErr := cmd.Run()
			err := stream.finish()
			if mode == "pass" && (runErr != nil || err != nil) {
				t.Fatal("fresh Go output rejected", runErr, err, string(stream.tail))
			}
			if mode != "pass" && err == nil {
				t.Fatal("real skipped/failed evidence accepted")
			}
			if mode == "skip" && !strings.Contains(err.Error(), "subtest skipped:") {
				t.Fatal("fixture did not reach the actual skip event", err)
			}
			if mode == "fail" && !strings.Contains(err.Error(), "::TestProof") {
				t.Fatal("fixture did not reach the actual test failure", err)
			}
		})
	}
}
