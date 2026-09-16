package kaggle

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{InstanceID: "personal", Revision: "revision-1", AccountName: "fixture_user", CredentialRef: "env:PROBE_TOKEN", PythonExecutable: filepath.Join(t.TempDir(), "python.exe")}
}
func verified() Report {
	r := baseline(ReadOnly)
	r.Authentication = "verified"
	r.AccountBinding = "matched"
	r.Quota = "unknown"
	return r
}
func TestPreflightLocalNeverResolvesAndReadOnlyUsesFreshCredential(t *testing.T) {
	c := testConfig(t)
	lookups := 0
	value := "synthetic-token"
	calls := []Mode{}
	resolver, _ := credentials.NewEnvironment([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { lookups++; return value, true })
	p, err := New(c, resolver)
	if err != nil {
		t.Fatal(err)
	}
	original := c
	c.AccountName = "changed"
	p.run = func(_ context.Context, c Config, m Mode, b []byte) (Report, error) {
		if c != original {
			t.Fatal("mutable config")
		}
		calls = append(calls, m)
		if m == Local {
			if len(b) != 0 {
				t.Fatal("secret in local check")
			}
			return baseline(Local), nil
		}
		if string(b) != value {
			t.Fatal("cached/wrong secret")
		}
		return verified(), nil
	}
	if r, err := p.Check(context.Background(), Local); err != nil || r != baseline(Local) || lookups != 0 {
		t.Fatal(r, err)
	}
	for _, v := range []string{"synthetic-token", "synthetic-rotated"} {
		value = v
		if r, err := p.Check(context.Background(), ReadOnly); err != nil || r != verified() {
			t.Fatal(r, err)
		}
	}
	if lookups != 2 || len(calls) != 5 {
		t.Fatal("unexpected lookup/call count", lookups, calls)
	}
	if _, err := p.Check(context.Background(), "automatic"); !errors.Is(err, ErrMode) || len(calls) != 5 {
		t.Fatal("mode bypass", err)
	}
}
func TestPreflightStopsBeforeCredentialOnLocalMismatch(t *testing.T) {
	c := testConfig(t)
	resolver, _ := credentials.NewEnvironment([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { t.Fatal("credential read"); return "", false })
	p, _ := New(c, resolver)
	p.run = func(context.Context, Config, Mode, []byte) (Report, error) {
		r := baseline(Local)
		r.Local = "version_mismatch"
		r.Problem = "version_mismatch"
		return r, nil
	}
	r, err := p.Check(context.Background(), ReadOnly)
	if err != nil || r.Local != "version_mismatch" || r.Authentication != "not_checked" {
		t.Fatal(r, err)
	}
}
func TestPreflightRejectsSecretAndSanitizesErrors(t *testing.T) {
	for _, value := range []string{"", "synthetic token", "synthetic\n", strings.Repeat("x", 8193)} {
		c := testConfig(t)
		resolver, _ := credentials.NewEnvironment([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { return value, true })
		p, _ := New(c, resolver)
		p.run = func(_ context.Context, _ Config, m Mode, _ []byte) (Report, error) {
			if m != Local {
				t.Fatal("bad token dispatched")
			}
			return baseline(Local), nil
		}
		r, err := p.Check(context.Background(), ReadOnly)
		if err != nil || r.Problem != "credential_unavailable" || !r.valid(ReadOnly) {
			t.Fatal(r, err)
		}
	}
	c := testConfig(t)
	resolver, _ := credentials.NewEnvironment([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { return "SYNTHETIC_CANARY", true })
	p, _ := New(c, resolver)
	p.run = func(_ context.Context, _ Config, m Mode, _ []byte) (Report, error) {
		if m == Local {
			return baseline(Local), nil
		}
		return Report{}, errors.New("SYNTHETIC_CANARY")
	}
	if _, err := p.Check(context.Background(), ReadOnly); err != ErrProcess {
		t.Fatal("raw failure reflected", err)
	}
}
func TestPreflightCancellationAndSerializedChecks(t *testing.T) {
	c := testConfig(t)
	resolver, _ := credentials.NewEnvironment([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { return "synthetic", true })
	p, _ := New(c, resolver)
	entered := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	p.run = func(context.Context, Config, Mode, []byte) (Report, error) {
		close(entered)
		<-release
		return baseline(Local), nil
	}
	go func() {
		defer wg.Done()
		if _, err := p.Check(context.Background(), Local); err != nil {
			t.Error(err)
		}
	}()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Check(ctx, ReadOnly); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	wg.Wait()
}
func TestPreflightConfigAndReportAreClosed(t *testing.T) {
	c := testConfig(t)
	raw, _ := json.Marshal(c)
	if got, err := ParseConfig(raw); err != nil || got != c {
		t.Fatal(got, err)
	}
	for _, raw := range [][]byte{
		[]byte(`{}`), []byte(`null`), append(raw, []byte(` {}`)...),
		[]byte(strings.Replace(string(raw), `"revision":`, `"Revision":`, 1)),
		[]byte(strings.Replace(string(raw), `"revision":`, `"revision":"other","revision":`, 1)),
		[]byte(strings.Replace(string(raw), `env:PROBE_TOKEN`, `file:/private/token`, 1)),
	} {
		if _, err := ParseConfig(raw); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	r := verified()
	encoded, _ := json.Marshal(r)
	for _, bad := range []string{
		strings.Replace(string(encoded), `"batch_ready":false,`, "", 1),
		strings.Replace(string(encoded), `"batch_ready":false`, `"batch_ready":true`, 1),
		strings.Replace(string(encoded), `"authentication":"verified"`, `"authentication":"SYNTHETIC_CANARY"`, 1),
		strings.Replace(string(encoded), `"protocol":1`, `"protocol":1,"protocol":1`, 1),
	} {
		var got Report
		if closedObject([]byte(bad), &got) == nil && got.valid(ReadOnly) {
			t.Fatal("invalid report accepted")
		}
	}
}
