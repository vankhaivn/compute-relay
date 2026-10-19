package kaggle

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/ports"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{InstanceID: "personal", Revision: "revision-1", AccountName: "fixture_user", CredentialRef: "vault:con_fixture"}
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
	resolver, _ := fixtureCredentials([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { lookups++; return value, true })
	p, err := New(c, resolver)
	if err != nil {
		t.Fatal(err)
	}
	original := c
	c.AccountName = "changed"
	p.run = func(_ context.Context, c Config, m Mode, b []byte, _ bool) (Report, error) {
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
	if lookups != 2 || len(calls) != 2 {
		t.Fatal("unexpected lookup/call count", lookups, calls)
	}
	if _, err := p.Check(context.Background(), "automatic"); !errors.Is(err, ErrMode) || len(calls) != 2 {
		t.Fatal("mode bypass", err)
	}
}
func TestPreflightLocalNeedsNoCredentialOrNetwork(t *testing.T) {
	c := testConfig(t)
	resolver, _ := fixtureCredentials([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { t.Fatal("credential read"); return "", false })
	p, _ := New(c, resolver)
	p.run = func(context.Context, Config, Mode, []byte, bool) (Report, error) {
		t.Fatal("local check performed a network request")
		return Report{}, nil
	}
	r, err := p.Check(context.Background(), Local)
	if err != nil || r != baseline(Local) {
		t.Fatal(r, err)
	}
}
func TestPreflightRejectsSecretAndSanitizesErrors(t *testing.T) {
	for _, value := range []string{"", "synthetic token", "synthetic\n", strings.Repeat("x", 8193)} {
		c := testConfig(t)
		resolver, _ := fixtureCredentials([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { return value, true })
		p, _ := New(c, resolver)
		p.run = func(_ context.Context, _ Config, m Mode, _ []byte, _ bool) (Report, error) {
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
	resolver, _ := fixtureCredentials([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { return "SYNTHETIC_CANARY", true })
	p, _ := New(c, resolver)
	p.run = func(_ context.Context, _ Config, m Mode, _ []byte, _ bool) (Report, error) {
		if m == Local {
			return baseline(Local), nil
		}
		return Report{}, errors.New("SYNTHETIC_CANARY")
	}
	if _, err := p.Check(context.Background(), ReadOnly); err != ErrUnavailable {
		t.Fatal("raw failure reflected", err)
	}
}
func TestPreflightCancellationAndSerializedChecks(t *testing.T) {
	c := testConfig(t)
	resolver, _ := fixtureCredentials([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { return "synthetic", true })
	p, _ := New(c, resolver)
	entered := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	p.run = func(context.Context, Config, Mode, []byte, bool) (Report, error) {
		close(entered)
		<-release
		return verified(), nil
	}
	go func() {
		defer wg.Done()
		if _, err := p.Check(context.Background(), ReadOnly); err != nil {
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

func TestPreflightRejectsIgnoredCredentialCallbackViolations(t *testing.T) {
	for _, fault := range []string{"no-callback", "twice", "invalid-then-valid"} {
		t.Run(fault, func(t *testing.T) {
			p, err := New(testConfig(t), stagingCredentialFunc(func(_ context.Context, _ ports.CredentialRef, use func([]byte) error) error {
				if fault == "no-callback" {
					return nil
				}
				first := []byte("SYNTHETIC_TOKEN")
				if fault == "invalid-then-valid" {
					first = []byte("invalid token")
				}
				_ = use(first)
				_ = use([]byte("SYNTHETIC_TOKEN"))
				return nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			p.run = func(context.Context, Config, Mode, []byte, bool) (Report, error) {
				calls++
				return verified(), nil
			}
			if report, err := p.Check(context.Background(), ReadOnly); err != ErrProtocol || report != (Report{}) || calls > 1 {
				t.Fatal("resolver violation established verification", report, err, calls)
			}
		})
	}
}

func TestPreflightReportIsClosed(t *testing.T) {
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
