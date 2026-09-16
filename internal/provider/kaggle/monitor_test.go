package kaggle

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

func monitorFixture(t *testing.T) *Monitor {
	t.Helper()
	c := testConfig(t)
	resolver, err := credentials.NewEnvironment([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { return "SYNTHETIC_TOKEN", true })
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewMonitor(c, resolver, &stagingClock{at: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	m.local = func(context.Context, Config, Mode, []byte) (Report, error) { return baseline(Local), nil }
	return m
}

func knownQuota(limit, used, reserved string) monitorResponse {
	return monitorResponse{Protocol: 1, Status: "known", Reason: "none", LimitNS: limit, UsedNS: used, ReservedNS: reserved}
}

func TestMonitorQuotaRoundsReservationsDownstreamWithoutInventingReset(t *testing.T) {
	at := time.Now().UTC()
	q, err := quotaFromResponse(knownQuota("19999999999", "2000000001", "3000000001"), at)
	if err != nil || q.Validate() != nil || q.Status != provider.QuotaKnown || *q.Limit != 19 || *q.Used != 3 || *q.Remaining != 12 || q.Precision != "lower_bound" || q.Unit != "seconds" || q.ResetAt != nil || !q.ObservedAt.Equal(at) {
		t.Fatal("quota not conservative", q, err)
	}
	gate := scheduler.Quota{Observation: &q}
	if reason, _ := gate.Decide("gpu", 12, true, QuotaFreshFor, at); reason != scheduler.Eligible {
		t.Fatal("valid conservative allowance rejected", reason)
	}
	if reason, _ := gate.Decide("gpu", 13, true, QuotaFreshFor, at); reason != scheduler.QuotaInsufficient {
		t.Fatal("reserved time was spent twice", reason)
	}
	zero, err := quotaFromResponse(knownQuota("1000000000", "1000000001", "0"), at)
	if err != nil || *zero.Remaining != 0 {
		t.Fatal("negative remainder not clamped", err)
	}
	for _, result := range []monitorResponse{{Protocol: 1, Status: "unknown", Reason: "missing_quota"}, {Protocol: 1, Status: "unknown", Reason: "paid_or_unknown"}, {Protocol: 1, Status: "unavailable", Reason: "read_unavailable"}} {
		q, err := quotaFromResponse(result, at)
		if err != nil || q.Validate() != nil || q.Limit != nil || q.Used != nil || q.Remaining != nil || q.ResetAt != nil {
			t.Fatal("absent evidence became zero", q, err)
		}
	}
}

func TestMonitorQuotaFreshnessDoesNotRefreshOrClearExhaustion(t *testing.T) {
	at := time.Now().UTC()
	q, _ := quotaFromResponse(knownQuota("10000000000", "10000000000", "0"), at)
	stale, err := AgeQuota(q, at.Add(QuotaFreshFor))
	if err != nil || stale.Status != provider.QuotaStale || !stale.ObservedAt.Equal(at) || stale.ResetAt != nil || *stale.Remaining != 0 {
		t.Fatal("aging fabricated new quota", stale, err)
	}
	if reason, _ := (scheduler.Quota{Observation: &stale}).Decide("gpu", 1, false, QuotaFreshFor, at.Add(24*time.Hour)); reason != scheduler.QuotaExhausted {
		t.Fatal("old zero stopped being exhausted", reason)
	}
	*stale.Remaining = 100
	if *q.Remaining != 0 {
		t.Fatal("quota snapshot aliases caller memory")
	}
	positive, _ := quotaFromResponse(knownQuota("10000000000", "0", "0"), at)
	fresh, _ := AgeQuota(positive, at.Add(QuotaFreshFor-time.Nanosecond))
	if fresh.Status != provider.QuotaKnown {
		t.Fatal("freshness boundary regressed")
	}
	if _, err := AgeQuota(q, at.Add(-time.Nanosecond)); !errors.Is(err, ErrProtocol) {
		t.Fatal("future quota accepted")
	}
}

func TestMonitorResponseRejectsMalformedAndContradictoryEvidence(t *testing.T) {
	for _, text := range []string{"", "-1", "+1", "00", "1.0", "1e9", " 1", strconv.FormatInt(maxQuotaNanos+1, 10)} {
		r := knownQuota(text, "0", "0")
		if r.valid("quota") {
			t.Fatal("invalid duration accepted", text)
		}
	}
	for _, r := range []monitorResponse{
		{Protocol: 1, Status: "unknown", Reason: "missing_quota", UsedNS: "0"},
		{Protocol: 1, Status: "unavailable", Reason: "read_unavailable", TextB64: "c2VjcmV0"},
		{Protocol: 1, Status: "logs", Reason: "none", Availability: "live"},
		{Protocol: 1, Status: "logs", Reason: "none", Availability: "delayed", TextB64: "/w=="},
		{Protocol: 1, Status: "logs", Reason: "none", Availability: "delayed", TextB64: base64.StdEncoding.EncodeToString(make([]byte, maxLogSnapshot+1))},
	} {
		if r.valid("quota") || r.valid("logs") {
			t.Fatal("contradictory evidence accepted")
		}
	}
	if knownQuota("0", "0", "0").valid("logs") {
		t.Fatal("wrong mode accepted")
	}
}

type monitorCredentialFunc func(context.Context, ports.CredentialRef, func([]byte) error) error

func (f monitorCredentialFunc) WithCredential(ctx context.Context, ref ports.CredentialRef, use func([]byte) error) error {
	return f(ctx, ref, use)
}

func TestMonitorChecksLocalBeforeCredentialsAndSanitizesFailures(t *testing.T) {
	for _, fault := range []string{"local", "missing", "bad-token", "callback-twice", "process", "panic", "protocol"} {
		t.Run(fault, func(t *testing.T) {
			m := monitorFixture(t)
			calls := 0
			m.run = func(context.Context, Config, string, []byte, monitorRequest) (monitorResponse, error) {
				calls++
				switch fault {
				case "process":
					return monitorResponse{}, errors.New("SYNTHETIC_TOKEN")
				case "panic":
					panic("SYNTHETIC_TOKEN")
				case "protocol":
					return monitorResponse{Protocol: 1, Status: "SYNTHETIC_TOKEN"}, nil
				}
				return knownQuota("1000000000", "0", "0"), nil
			}
			if fault == "local" {
				m.local = func(context.Context, Config, Mode, []byte) (Report, error) { return Report{}, ErrProcess }
				m.credentials = monitorCredentialFunc(func(context.Context, ports.CredentialRef, func([]byte) error) error {
					t.Fatal("secret lookup before local pins")
					return nil
				})
			} else if fault == "missing" || fault == "bad-token" || fault == "callback-twice" {
				m.credentials = monitorCredentialFunc(func(ctx context.Context, ref ports.CredentialRef, use func([]byte) error) error {
					if fault == "missing" {
						return errors.New("SYNTHETIC_TOKEN")
					}
					if fault == "bad-token" {
						return use([]byte("bad token"))
					}
					_ = use([]byte("SYNTHETIC_TOKEN"))
					_ = use([]byte("SYNTHETIC_TOKEN"))
					return nil // Even a resolver ignoring the callback error cannot qualify.
				})
			}
			q, err := m.ReadQuota(context.Background())
			if err == nil || strings.Contains(err.Error(), "SYNTHETIC_TOKEN") || q.Status != provider.QuotaUnavailable || q.Remaining != nil {
				t.Fatal("failure leaked success or secret", q, err)
			}
			if calls > 1 || (fault == "local" || fault == "missing" || fault == "bad-token") && calls != 0 {
				t.Fatal("unintended read", calls)
			}
		})
	}
}

func TestMonitorRotationTimestampAndSerializedReads(t *testing.T) {
	m := monitorFixture(t)
	clock := m.clock.(*stagingClock)
	value := "FIRST_TOKEN"
	lookups := 0
	m.credentials, _ = credentials.NewEnvironment([]ports.CredentialRef{m.config.CredentialRef}, func(string) (string, bool) { lookups++; return value, true })
	m.run = func(_ context.Context, c Config, mode string, token []byte, r monitorRequest) (monitorResponse, error) {
		if mode != "quota" || c != m.config || r.Owner != c.AccountName || r.Execution != nil || string(token) != value {
			t.Fatal("read changed account or cached secret")
		}
		return knownQuota("1000000000", "0", "0"), nil
	}
	for _, token := range []string{"FIRST_TOKEN", "ROTATED_TOKEN"} {
		value = token
		at := clock.Now()
		q, err := m.ReadQuota(context.Background())
		if err != nil || !q.ObservedAt.Equal(at) || q.ResetAt != nil {
			t.Fatal(q, err)
		}
	}
	if lookups != 2 {
		t.Fatal("credential lookup not fresh")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	m.run = func(context.Context, Config, string, []byte, monitorRequest) (monitorResponse, error) {
		close(entered)
		<-release
		return knownQuota("1000000000", "0", "0"), nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := m.ReadQuota(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.ReadQuota(ctx); !errors.Is(err, context.Canceled) {
		t.Error("queued read ignored cancellation", err)
	}
	close(release)
	wg.Wait()
}
