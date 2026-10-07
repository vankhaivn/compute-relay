package operatorcli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOperatorParserAcceptsOnlyExplicitCommandFlags(t *testing.T) {
	for _, args := range [][]string{
		{"init", "--root", "run"}, {"state", "--root", "run"}, {"serve", "--root", "run", "--listen", "127.0.0.1:0"},
		{"serve", "--root", "run", "--provider-config", "provider.json", "--provider-profile", "kaggle-t4", "--provider-machine-shape", "NvidiaTeslaT4", "--max-provider-attempts", "2", "--allow-private-staging", "--allow-gpu"},
		{"workspace", "create", "--root", "run", "--id", "app"}, {"workspace", "disable", "--root", "run", "--id", "app"},
		{"token", "issue", "--root", "run", "--workspace", "app", "--scope", "read", "--scope", "write", "--output", "private-token", "--ttl", "2h"},
		{"token", "revoke", "--root", "run", "--id", "tok_id"}, {"validate", "--file", "job.json"},
	} {
		request, err := Parse(args)
		if err != nil || request.Command != args[0] {
			t.Fatal(args, err)
		}
	}
	r, err := Parse([]string{"token", "issue", "--root", "run", "--workspace", "app", "--scope", "read", "--output", "token"})
	if err != nil || r.TTL != 24*time.Hour || len(r.Scopes) != 1 {
		t.Fatal(r, err)
	}
}
func TestOperatorInvalidAndHelpNeverPerformAnActionOrEchoInput(t *testing.T) {
	for _, args := range [][]string{
		nil, {"unknown-SYNTHETIC_SECRET"}, {"init"}, {"init", "--root", "a", "--root", "b"},
		{"state", "--root", "run", "--scope", "read"}, {"serve", "--root", "run", "--allow-gpu=false"},
		{"serve", "--root", "run", "--provider-config", "provider.json"},
		{"serve", "--root", "run", "--provider-config", "provider.json", "--provider-profile", "kaggle-t4", "--provider-machine-shape", "NvidiaTeslaT4", "--max-provider-attempts", "0", "--allow-private-staging", "--allow-gpu"},
		{"workspace", "missing", "--root", "run"}, {"token", "issue", "--root", "run", "--workspace", "app", "--scope", "read", "--scope", "read", "--output", "token"},
		{"token", "issue", "--root", "run", "--workspace", "app", "--scope", "admin", "--output", "token"},
		{"token", "issue", "--root", "run", "--workspace", "app", "--scope", "read", "--output", "token", "--ttl", "0s"},
		{"token", "issue", "--root", "run", "--workspace", "app", "--scope", "read", "--output", "token", "--ttl", "721h"},
		{"token", "revoke", "--root", "run", "--id", "tok_x", "--ttl", "1h"},
		{"validate", "--file", "job", "--root", "run"}, {"init", "--root", "run", "SYNTHETIC_SECRET"}, {"serve", "--root", "run", "--listen", "a", "--listen", "b"},
	} {
		var out, diagnostic bytes.Buffer
		code := Run(context.Background(), args, &out, &diagnostic, func(context.Context, Request, io.Writer) error {
			t.Fatal("invalid flags performed an action")
			return nil
		})
		if code != 2 || out.Len() != 0 || strings.Contains(diagnostic.String(), "SYNTHETIC_SECRET") {
			t.Fatal(args, code, out.String(), diagnostic.String())
		}
	}
	for _, args := range [][]string{{"serve", "--help"}, {"token", "--help"}, {"workspace", "create", "--help"}, {"init", "-h"}} {
		var out, diagnostic bytes.Buffer
		code := Run(context.Background(), args, &out, &diagnostic, func(context.Context, Request, io.Writer) error { t.Fatal("help performed an action"); return nil })
		if code != 0 || out.Len() == 0 || diagnostic.Len() != 0 {
			t.Fatal(code)
		}
	}
}
func TestOperatorActionErrorsAndReceiptWriterAreNotReflected(t *testing.T) {
	var out, diagnostic bytes.Buffer
	calls := 0
	code := Run(context.Background(), []string{"state", "--root", "run"}, &out, &diagnostic, func(ctx context.Context, r Request, dst io.Writer) error {
		calls++
		if r.Root != "run" {
			t.Fatal(r)
		}
		return errors.New("SYNTHETIC_PRIVATE_PATH")
	})
	if code != 1 || calls != 1 || strings.Contains(diagnostic.String(), "SYNTHETIC") {
		t.Fatal(code, diagnostic.String())
	}
}

func TestManagedServeParserDefaultsAndExplicitPolicy(t *testing.T) {
	python, err := filepath.Abs("synthetic-python3")
	if err != nil {
		t.Fatal(err)
	}
	base := []string{"serve", "--root", "run", "--managed-python", python}
	r, err := Parse(base)
	if err != nil || r.ManagedPython != python || r.ManagedMachineShape != "NvidiaTeslaT4" || r.ManagedMaxWallSeconds != 1800 || r.ManagedMaxWorkers != 2 || r.ManagedAllowInternet {
		t.Fatal("managed defaults do not match the bounded policy", err)
	}
	if r.ProviderConfig != "" || r.ProviderProfile != "" || r.MaxProviderAttempts != 0 || r.AllowGPU || r.AllowPrivateStaging {
		t.Fatal("managed policy implied standalone credentials or compute consent")
	}
	r, err = Parse(append(append([]string{}, base...), "--managed-machine-shape", "NvidiaTeslaP100", "--managed-max-wall-seconds", "86400", "--managed-max-workers", "16", "--managed-allow-internet"))
	if err != nil || r.ManagedMachineShape != "NvidiaTeslaP100" || r.ManagedMaxWallSeconds != 86400 || r.ManagedMaxWorkers != 16 || !r.ManagedAllowInternet {
		t.Fatal("explicit managed policy was not preserved", err)
	}
	r, err = Parse(append(append([]string{}, base...), "--managed-max-wall-seconds", "1", "--managed-max-workers", "1", "--managed-allow-internet=false"))
	if err != nil || r.ManagedMaxWallSeconds != 1 || r.ManagedMaxWorkers != 1 || r.ManagedAllowInternet {
		t.Fatal("minimum managed bounds or explicit internet denial rejected", err)
	}
	r, err = Parse([]string{"serve", "--root", "run"})
	if err != nil || r.ManagedPython != "" || r.ManagedMachineShape != "" || r.ManagedMaxWallSeconds != 0 || r.ManagedMaxWorkers != 0 || r.ManagedAllowInternet {
		t.Fatal("plain serve acquired managed defaults", err)
	}
	r, err = Parse([]string{"token", "issue", "--root", "run", "--workspace", "app", "--scope", "manage", "--scope", "execute", "--output", "private-token"})
	if err != nil || len(r.Scopes) != 2 || r.Scopes[0] != "manage" || r.Scopes[1] != "execute" {
		t.Fatal("explicit independent management/execution scopes rejected", err)
	}
}

func TestManagedServeParserRejectsAmbiguousOrUnboundedFlags(t *testing.T) {
	python, err := filepath.Abs("synthetic-python3")
	if err != nil {
		t.Fatal(err)
	}
	base := []string{"serve", "--root", "run", "--managed-python", python}
	for _, flags := range [][]string{
		{"--provider-config", "SYNTHETIC_PRIVATE_PATH"}, {"--provider-profile", "kaggle-t4"}, {"--provider-machine-shape", "NvidiaTeslaT4"},
		{"--max-provider-attempts", "1"}, {"--allow-gpu"}, {"--allow-gpu=false"}, {"--allow-private-staging"},
		{"--managed-python", python}, {"--managed-machine-shape", "NvidiaTeslaT4", "--managed-machine-shape", "NvidiaTeslaP100"},
		{"--managed-max-wall-seconds", "1", "--managed-max-wall-seconds", "2"}, {"--managed-max-workers", "1", "--managed-max-workers", "2"},
		{"--managed-allow-internet", "--managed-allow-internet=false"}, {"--managed-allow-internet=not-bool"},
		{"--managed-machine-shape", "T4"}, {"--managed-machine-shape", "cpu"}, {"--managed-machine-shape", ""},
		{"--managed-max-wall-seconds", "0"}, {"--managed-max-wall-seconds", "86401"}, {"--managed-max-wall-seconds", "-1"}, {"--managed-max-wall-seconds", "+1"},
		{"--managed-max-workers", "0"}, {"--managed-max-workers", "17"}, {"--managed-max-workers", "01"}, {"--managed-max-workers", "1x"},
	} {
		var out, diagnostic bytes.Buffer
		args := append(append([]string{}, base...), flags...)
		if code := Run(context.Background(), args, &out, &diagnostic, func(context.Context, Request, io.Writer) error {
			t.Fatal("invalid managed flags reached an action")
			return nil
		}); code != 2 || out.Len() != 0 || strings.Contains(diagnostic.String(), "SYNTHETIC") {
			t.Fatal("invalid managed flags were accepted or reflected")
		}
	}
	for _, python := range []string{"", "python3", "./python3", "SYNTHETIC_PRIVATE_PATH", python + "\n"} {
		if _, err := Parse([]string{"serve", "--root", "run", "--managed-python", python}); !errors.Is(err, ErrArguments) {
			t.Fatal("invalid managed Python path accepted")
		}
	}
	for _, flags := range [][]string{
		{"--managed-machine-shape", "NvidiaTeslaT4"}, {"--managed-max-wall-seconds", "1800"}, {"--managed-max-workers", "2"}, {"--managed-allow-internet=false"},
	} {
		if _, err := Parse(append([]string{"serve", "--root", "run"}, flags...)); !errors.Is(err, ErrArguments) {
			t.Fatal("managed companion flags selected a mode without its interpreter")
		}
	}
	for _, args := range [][]string{
		{"init", "--root", "run"}, {"state", "--root", "run"}, {"workspace", "create", "--root", "run", "--id", "app"},
		{"token", "issue", "--root", "run", "--workspace", "app", "--scope", "read", "--output", "token"}, {"validate", "--file", "job.json"},
	} {
		for _, flag := range [][]string{{"--managed-python", python}, {"--managed-machine-shape", "NvidiaTeslaT4"}, {"--managed-max-wall-seconds", "1800"}, {"--managed-max-workers", "2"}, {"--managed-allow-internet=false"}} {
			if _, err := Parse(append(append([]string{}, args...), flag...)); !errors.Is(err, ErrArguments) {
				t.Fatal("managed flags escaped the serve command")
			}
		}
	}
	standalone := []string{"serve", "--root", "run", "--provider-config", "provider.json", "--provider-profile", "kaggle-t4", "--provider-machine-shape", "NvidiaTeslaT4", "--max-provider-attempts", "2", "--allow-private-staging", "--allow-gpu"}
	for _, flag := range []string{"--allow-gpu", "--allow-private-staging", "--managed-python=" + python} {
		if _, err := Parse(append(append([]string{}, standalone...), flag)); !errors.Is(err, ErrArguments) {
			t.Fatal("duplicate or mixed standalone flags accepted")
		}
	}
}

func TestManagedServeHelpDescribesPolicyWithoutPerformingWork(t *testing.T) {
	var out, diagnostic bytes.Buffer
	if code := Run(context.Background(), []string{"serve", "--help"}, &out, &diagnostic, func(context.Context, Request, io.Writer) error {
		t.Fatal("help started managed runtime")
		return nil
	}); code != 0 || diagnostic.Len() != 0 {
		t.Fatal("managed serve help failed")
	}
	for _, required := range []string{"--managed-python ABS", "--managed-machine-shape NvidiaTeslaT4", "--managed-max-wall-seconds 1800", "--managed-max-workers 2", "--managed-allow-internet", "each attempt needs separate API execution authorization"} {
		if !strings.Contains(out.String(), required) {
			t.Fatal("help omitted managed policy or authorization semantics")
		}
	}
}
