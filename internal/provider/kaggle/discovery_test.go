package kaggle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func discoveryFixture(t *testing.T) *Discovery {
	t.Helper()
	d, err := NewDiscovery(testConfig(t).PythonExecutable, &stagingClock{at: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func discoveryVerified() discoveryResponse {
	return discoveryResponse{Protocol: 1, Status: "verified", Account: "fixture_user", QuotaStatus: "unknown", QuotaReason: "missing_quota"}
}

func TestDiscoverySameAccountUsesFreshTokenAndKeepsBindingPrivate(t *testing.T) {
	d := discoveryFixture(t)
	var tokens []string
	d.run = func(_ context.Context, _ string, token []byte) (discoveryResponse, error) {
		tokens = append(tokens, string(token))
		r := discoveryVerified()
		r.QuotaStatus, r.QuotaReason = "known", "none"
		r.LimitNS, r.UsedNS, r.ReservedNS = "19999999999", "2000000001", "3000000001"
		return r, nil
	}
	for _, token := range []string{"SYNTHETIC_FIRST", "SYNTHETIC_REPLACEMENT"} {
		result, err := d.Discover(context.Background(), []byte(token))
		if err != nil || result.CanonicalAccount() != "fixture_user" {
			t.Fatal("canonical account not verified", err)
		}
		q := result.Quota()
		if q.Status != provider.QuotaKnown || q.Validate() != nil || *q.Remaining != 12 || !q.ObservedAt.Equal(d.clock.Now()) {
			t.Fatal("quota lost precision or provenance", q)
		}
		*q.Remaining = 999
		if *result.Quota().Remaining != 12 {
			t.Fatal("cached result aliases caller memory")
		}
		raw, err := json.Marshal(result)
		if err != nil || string(raw) != "{}" || strings.Contains(fmt.Sprintf("%v %+v %#v", result, result, result), "fixture_user") {
			t.Fatal("private account escaped through serialization")
		}
	}
	if strings.Join(tokens, ",") != "SYNTHETIC_FIRST,SYNTHETIC_REPLACEMENT" {
		t.Fatal("credential was cached or replaced")
	}
}

func TestDiscoveryKeepsUnknownUnavailableAndKnownZeroQuotaDistinct(t *testing.T) {
	for _, tc := range []struct {
		status string
		reason string
	}{
		{"unknown", "missing_quota"}, {"unknown", "paid_or_unknown"},
		{"unavailable", "read_unavailable"}, {"known", "none"},
	} {
		t.Run(tc.status+tc.reason, func(t *testing.T) {
			d := discoveryFixture(t)
			d.run = func(context.Context, string, []byte) (discoveryResponse, error) {
				r := discoveryVerified()
				r.QuotaStatus, r.QuotaReason = tc.status, tc.reason
				if tc.status == "known" {
					r.LimitNS, r.UsedNS, r.ReservedNS = "0", "0", "0"
				}
				return r, nil
			}
			result, err := d.Discover(context.Background(), []byte("SYNTHETIC_TOKEN"))
			q := result.Quota()
			if err != nil || result.CanonicalAccount() != "fixture_user" || string(q.Status) != tc.status || q.Validate() != nil {
				t.Fatal("account/quota conflated", err, q)
			}
			if tc.status == "known" {
				if q.Remaining == nil || *q.Remaining != 0 {
					t.Fatal("known zero was lost")
				}
			} else if q.Remaining != nil || q.Limit != nil || q.Used != nil {
				t.Fatal("unknown became a number")
			}
		})
	}
}

func TestDiscoveryInvalidTokenCannotInvokeChild(t *testing.T) {
	d := discoveryFixture(t)
	d.run = func(context.Context, string, []byte) (discoveryResponse, error) {
		t.Fatal("invalid token reached child")
		return discoveryResponse{}, nil
	}
	for _, token := range []string{"", "space token", "newline\n", "\x00", "\xff", strings.Repeat("x", 8193)} {
		result, err := d.Discover(context.Background(), []byte(token))
		if err != ErrCredentialRejected || result.CanonicalAccount() != "" {
			t.Fatal("invalid token produced identity", err)
		}
	}
}

func TestDiscoveryFailureNeverRetainsAccountOrReflectsProviderText(t *testing.T) {
	for _, tc := range []struct {
		status string
		want   error
	}{
		{"credential_rejected", ErrCredentialRejected}, {"unavailable", ErrDiscoveryUnavailable}, {"invalid_response", ErrProtocol},
	} {
		d := discoveryFixture(t)
		d.run = func(context.Context, string, []byte) (discoveryResponse, error) {
			return discoveryResponse{Protocol: 1, Status: tc.status}, nil
		}
		result, err := d.Discover(context.Background(), []byte("SYNTHETIC_TOKEN"))
		if err != tc.want || result.CanonicalAccount() != "" || !result.Quota().ObservedAt.IsZero() {
			t.Fatal("failed verification retained evidence", err)
		}
	}
	for _, account := range []string{"", "Fixture_User", " leading", "a", "../other", "fixturé", "fixture_user\n", strings.Repeat("a", 51), "synthetic_token"} {
		d := discoveryFixture(t)
		d.run = func(context.Context, string, []byte) (discoveryResponse, error) {
			r := discoveryVerified()
			r.Account = account
			return r, nil
		}
		result, err := d.Discover(context.Background(), []byte("synthetic_token"))
		if err != ErrProtocol || result.CanonicalAccount() != "" {
			t.Fatal("noncanonical or echoed identity accepted", err)
		}
	}
	for _, panicInstead := range []bool{false, true} {
		d := discoveryFixture(t)
		d.run = func(context.Context, string, []byte) (discoveryResponse, error) {
			if panicInstead {
				panic("SYNTHETIC_TOKEN")
			}
			return discoveryVerified(), errors.New("SYNTHETIC_TOKEN")
		}
		result, err := d.Discover(context.Background(), []byte("SYNTHETIC_TOKEN"))
		if err != ErrProcess || result.CanonicalAccount() != "" {
			t.Fatal("raw error leaked or identity retained", err)
		}
	}
}

func TestDiscoveryCancellationSerializesAndTimestampPrecedesRead(t *testing.T) {
	d := discoveryFixture(t)
	at := d.clock.Now()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	d.run = func(context.Context, string, []byte) (discoveryResponse, error) {
		close(entered)
		<-release
		d.clock.(*stagingClock).at = at.Add(4 * time.Minute)
		return discoveryVerified(), nil
	}
	go func() {
		defer close(done)
		r, err := d.Discover(context.Background(), []byte("SYNTHETIC_TOKEN"))
		if err != nil || !r.Quota().ObservedAt.Equal(at) {
			t.Error("latency refreshed observation", err)
		}
	}()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := d.Discover(ctx, []byte("SYNTHETIC_TOKEN")); err != context.Canceled {
		t.Fatal("waiting discovery ignored cancellation", err)
	}
	close(release)
	<-done
	d.run = func(context.Context, string, []byte) (discoveryResponse, error) {
		d.clock.(*stagingClock).at = at.Add(-time.Minute)
		return discoveryVerified(), nil
	}
	if _, err := d.Discover(context.Background(), []byte("SYNTHETIC_TOKEN")); err != ErrProtocol {
		t.Fatal("backwards clock accepted", err)
	}
}

func TestDiscoveryConfigurationAndVaultBindings(t *testing.T) {
	c := testConfig(t)
	for _, ref := range []ports.CredentialRef{"env:PROBE_TOKEN", "vault:con_fixture"} {
		c.CredentialRef = ref
		raw, _ := json.Marshal(c)
		if _, err := ParseConfig(raw); err != nil {
			t.Fatal("explicit credential reference rejected", err)
		}
	}
	for _, ref := range []ports.CredentialRef{"vault:", "vault:other", "vault:con_", "vault:con_bad/path", "file:/token", "ambient"} {
		c.CredentialRef = ref
		if c.Validate() == nil {
			t.Fatal("invalid reference accepted")
		}
	}
	for _, path := range []string{"", "relative/python", "/tmp/helper.sh", "/tmp/helper.cmd", "/tmp/helper\n"} {
		if _, err := NewDiscovery(path, &stagingClock{}); err != ErrConfig {
			t.Fatal("invalid interpreter accepted", err)
		}
	}
	if _, err := NewDiscovery(c.PythonExecutable, nil); err != ErrConfig {
		t.Fatal("missing clock accepted", err)
	}
}
