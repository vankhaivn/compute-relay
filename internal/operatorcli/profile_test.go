package operatorcli

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func TestProfileFlagsAreClosedBeforeAdministrativeWork(t *testing.T) {
	for _, args := range [][]string{
		{"profile", "apply", "--root", "state", "--file", "profile.json"},
		{"profile", "show", "--root", "state", "--name", "example"},
		{"profile", "grant", "--root", "state", "--workspace", "app", "--name", "example"},
		{"profile", "revoke", "--root", "state", "--workspace", "app", "--name", "example"},
	} {
		if _, err := Parse(args); err != nil {
			t.Fatal(args, err)
		}
	}
	for _, args := range [][]string{
		{"profile", "apply", "--root", "state"},
		{"profile", "apply", "--root", "state", "--file", "one", "--file", "two"},
		{"profile", "show", "--root", "state", "--name", "example", "--file", "unused"},
		{"profile", "grant", "--root", "state", "--name", "example"},
		{"profile", "revoke", "--root", "state", "--workspace", "app", "--name", "example", "--ttl", "1h"},
		{"profile", "show", "--root", "state", "--name", "example\x00"},
	} {
		var out, diagnostic bytes.Buffer
		code := Run(context.Background(), args, &out, &diagnostic, func(context.Context, Request, io.Writer) error {
			t.Fatal("invalid flags performed administrative work")
			return nil
		})
		if code != 2 || out.Len() != 0 {
			t.Fatal(args, code)
		}
	}
	var out, diagnostic bytes.Buffer
	if Run(context.Background(), []string{"profile", "--help"}, &out, &diagnostic, nil) != 0 || diagnostic.Len() != 0 {
		t.Fatal("profile help required state")
	}
}
