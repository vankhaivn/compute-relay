package operatorcli

import (
	"bytes"
	"context"
	"errors"
	"io"
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
