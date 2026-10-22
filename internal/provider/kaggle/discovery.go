package kaggle

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

var (
	ErrCredentialRejected   = errors.New("Kaggle credential rejected")
	ErrDiscoveryUnavailable = errors.New("Kaggle account discovery unavailable")
)

// Discovery performs explicit read-only verification of supplied credentials.
// Construction, result access and quota aging never perform provider calls.
type Discovery struct {
	clock ports.Clock
	slot  chan struct{}
	run   func(context.Context, []byte) (discoveryResponse, error)
}

// DiscoveryResult is private binding evidence, not a public connection response.
// Unexported fields prevent accidental JSON disclosure of the provider username.
type DiscoveryResult struct {
	account string
	quota   provider.QuotaObservation
}

func (r DiscoveryResult) CanonicalAccount() string { return r.account }

// Quota returns an independent snapshot; its timestamp is never refreshed here.
func (r DiscoveryResult) Quota() provider.QuotaObservation {
	q, _ := AgeQuota(r.quota, r.quota.ObservedAt)
	return q
}

func (DiscoveryResult) String() string   { return "Kaggle discovery result (private)" }
func (DiscoveryResult) GoString() string { return "Kaggle discovery result (private)" }

func NewDiscovery(clock ports.Clock, pacing *kaggleapi.RequestPacer) (*Discovery, error) {
	if clock == nil {
		return nil, ErrConfig
	}
	return &Discovery{clock: clock, slot: make(chan struct{}, 1), run: func(ctx context.Context, token []byte) (discoveryResponse, error) {
		return runDiscovery(ctx, token, pacing)
	}}, nil
}

// Discover makes at most one authentication and one quota request, with a minute
// including the serialized wait. The native client erases its credential copy;
// the caller owns and clears the original. Successful identity with unknown/unavailable
// quota is still verified, but cannot establish known GPU capacity.
func (d *Discovery) Discover(parent context.Context, token []byte) (result DiscoveryResult, err error) {
	reason := "discovery_response_invalid"
	defer func() {
		if recover() != nil {
			result, err = DiscoveryResult{}, ErrUnavailable
			reason = "discovery_call_failed"
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				err = contextStep(err, "discovery")
			} else {
				err = provider.WithReason(err, reason)
			}
		}
	}()
	if d == nil {
		return result, ErrConfig
	}
	if len(token) == 0 || len(token) > credentials.MaxBytes {
		reason = "discovery_credential_rejected"
		return result, ErrCredentialRejected
	}
	for _, b := range token {
		if b < 33 || b > 126 {
			reason = "discovery_credential_rejected"
			return result, ErrCredentialRejected
		}
	}
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	select {
	case d.slot <- struct{}{}:
		defer func() { <-d.slot }()
	case <-ctx.Done():
		return result, ctx.Err()
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	at := d.clock.Now().UTC()
	if at.IsZero() {
		return result, ErrConfig
	}
	r, runErr := d.run(ctx, token)
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if runErr != nil {
		// Never reflect arbitrary client or adapter errors containing secrets.
		if errors.Is(runErr, ErrProtocol) {
			return result, ErrProtocol
		}
		reason = "discovery_call_failed"
		return result, ErrUnavailable
	}
	if !r.valid() || d.clock.Now().Before(at) {
		return result, ErrProtocol
	}
	switch r.Status {
	case "credential_rejected":
		reason = "discovery_credential_rejected"
		return result, ErrCredentialRejected
	case "unavailable":
		reason = "discovery_account_read_failed"
		return result, ErrDiscoveryUnavailable
	case "invalid_response":
		return result, ErrProtocol
	}
	if bytes.Contains([]byte(r.Account), token) {
		return result, ErrProtocol
	}
	quota, err := quotaFromResponse(r.quota(), at)
	if err != nil {
		return result, ErrProtocol
	}
	return DiscoveryResult{account: r.Account, quota: quota}, nil
}

type discoveryResponse struct {
	Protocol    int    `json:"protocol"`
	Status      string `json:"status"`
	Account     string `json:"account"`
	QuotaStatus string `json:"quota_status"`
	QuotaReason string `json:"quota_reason"`
	LimitNS     string `json:"limit_ns"`
	UsedNS      string `json:"used_ns"`
	ReservedNS  string `json:"reserved_ns"`
}

func (r discoveryResponse) quota() monitorResponse {
	return monitorResponse{Protocol: 1, Status: r.QuotaStatus, Reason: r.QuotaReason,
		LimitNS: r.LimitNS, UsedNS: r.UsedNS, ReservedNS: r.ReservedNS}
}

func (r discoveryResponse) valid() bool {
	if r.Protocol != 1 {
		return false
	}
	switch r.Status {
	case "verified":
		return accountPattern.MatchString(r.Account) && r.quota().valid("quota")
	case "credential_rejected", "unavailable", "invalid_response":
		return r == (discoveryResponse{Protocol: 1, Status: r.Status})
	default:
		return false
	}
}
