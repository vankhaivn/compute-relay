package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
)

type checkFunc func(context.Context, kaggle.Mode) (kaggle.Report, error)

func (f checkFunc) Check(ctx context.Context, m kaggle.Mode) (kaggle.Report, error) { return f(ctx, m) }
func fixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	c := kaggle.Config{InstanceID: "fixture", Revision: "1", AccountName: "fixture_user", CredentialRef: "env:EXPLICIT_TOKEN", PythonExecutable: filepath.Join(t.TempDir(), "python.exe")}
	raw, _ := json.Marshal(c)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestPreflightCommandRequiresExplicitNetworkOptIn(t *testing.T) {
	path := fixture(t)
	for _, allow := range []bool{false, true} {
		args := []string{"--config", path}
		if allow {
			args = append(args, "--allow-read-only")
		}
		var out, diagnostic bytes.Buffer
		mk := func(c kaggle.Config) (checker, error) {
			if c.CredentialRef != "env:EXPLICIT_TOKEN" {
				t.Fatal("reference changed")
			}
			return checkFunc(func(_ context.Context, m kaggle.Mode) (kaggle.Report, error) {
				expected := kaggle.Local
				if allow {
					expected = kaggle.ReadOnly
				}
				if m != expected {
					t.Fatal("implicit network")
				}
				return kaggle.Report{Protocol: 1, Mode: m, Problem: "none", BatchReady: false}, nil
			}), nil
		}
		if code := run(context.Background(), args, &out, &diagnostic, mk); code != 0 {
			t.Fatal(code, diagnostic.String())
		}
		if strings.Contains(out.String(), "EXPLICIT_TOKEN") || strings.Contains(out.String(), "fixture_user") {
			t.Fatal("private configuration reflected")
		}
		var result map[string]any
		if json.Unmarshal(out.Bytes(), &result) != nil || result["checked_at"] == nil {
			t.Fatal("missing timestamp")
		}
	}
}
func TestPreflightCommandNoWorkOnHelpOrInvalidConfiguration(t *testing.T) {
	mk := func(kaggle.Config) (checker, error) { t.Fatal("checker constructed"); return nil, nil }
	for _, args := range [][]string{{"--help"}, {}, {"--token", "SYNTHETIC_SECRET"}, {"--config", "nonexistent"}, {"--config", fixture(t), "unexpected"}} {
		var out, diagnostic bytes.Buffer
		code := run(context.Background(), args, &out, &diagnostic, mk)
		if len(args) == 1 && args[0] == "--help" {
			if code != 0 {
				t.Fatal(code)
			}
		} else if code == 0 {
			t.Fatal("invalid arguments succeeded")
		}
		if strings.Contains(out.String()+diagnostic.String(), "SYNTHETIC_SECRET") {
			t.Fatal("secret reflected")
		}
	}
}
func TestPreflightCommandSanitizesInternalFailures(t *testing.T) {
	var out, diagnostic bytes.Buffer
	mk := func(kaggle.Config) (checker, error) {
		return checkFunc(func(context.Context, kaggle.Mode) (kaggle.Report, error) {
			return kaggle.Report{}, errors.New("SYNTHETIC_SECRET")
		}), nil
	}
	if code := run(context.Background(), []string{"--config", fixture(t), "--allow-read-only"}, &out, &diagnostic, mk); code != 1 {
		t.Fatal(code)
	}
	if out.Len() != 0 || strings.Contains(diagnostic.String(), "SYNTHETIC_SECRET") {
		t.Fatal("internal output leaked")
	}
}
