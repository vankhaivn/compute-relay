package kaggle

import (
	"context"
	"encoding/base64"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

const maxLogSnapshot = 64 << 10
const maxQuotaNanos int64 = 366 * 24 * 60 * 60 * 1_000_000_000

// Monitor is an explicitly invoked read-only service, not a polling goroutine.
// Share an instance to serialize reads for one immutable configuration/account.
type Monitor struct {
	config      Config
	credentials ports.CredentialResolver
	clock       ports.Clock
	slot        chan struct{}
	local       invocation
	run         monitorInvocation
}
type monitorInvocation func(context.Context, Config, string, []byte, monitorRequest) (monitorResponse, error)
type monitorRequest struct {
	Protocol  int               `json:"protocol"`
	Owner     string            `json:"owner"`
	Execution *executionRequest `json:"execution"`
}
type monitorResponse struct {
	Protocol     int    `json:"protocol"`
	Status       string `json:"status"`
	Reason       string `json:"reason"`
	LimitNS      string `json:"limit_ns"`
	UsedNS       string `json:"used_ns"`
	ReservedNS   string `json:"reserved_ns"`
	TextB64      string `json:"text_b64"`
	Availability string `json:"availability"`
	Truncated    bool   `json:"truncated"`
}

func NewMonitor(c Config, resolver ports.CredentialResolver, clock ports.Clock) (*Monitor, error) {
	if c.Validate() != nil || resolver == nil || clock == nil {
		return nil, ErrConfig
	}
	return &Monitor{config: c, credentials: resolver, clock: clock, slot: make(chan struct{}, 1), local: runPython, run: runMonitor}, nil
}

func quotaNanos(text string) (int64, error) {
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value < 0 || value > maxQuotaNanos || strconv.FormatInt(value, 10) != text {
		return 0, ErrProtocol
	}
	return value, nil
}

func (r monitorResponse) valid(mode string) bool {
	if r.Protocol != 1 || mode != "quota" && mode != "logs" {
		return false
	}
	blank := monitorResponse{Protocol: 1, Status: r.Status, Reason: r.Reason}
	switch r.Status {
	case "unknown":
		return mode == "quota" && (r.Reason == "missing_quota" || r.Reason == "paid_or_unknown") && r == blank
	case "unavailable":
		return (r.Reason == "read_unavailable" || mode == "logs" && r.Reason == "missing_log") && r == blank
	case "invalid":
		return mode == "logs" && r.Reason == "identity_mismatch" && r == blank
	case "known":
		if mode != "quota" || r.Reason != "none" {
			return false
		}
		for _, text := range []string{r.LimitNS, r.UsedNS, r.ReservedNS} {
			if _, err := quotaNanos(text); err != nil {
				return false
			}
		}
		blank.LimitNS, blank.UsedNS, blank.ReservedNS = r.LimitNS, r.UsedNS, r.ReservedNS
		return r == blank
	case "logs":
		if mode != "logs" || r.Reason != "none" || (r.Availability != "delayed" && r.Availability != "after_completion") {
			return false
		}
		raw, err := base64.StdEncoding.Strict().DecodeString(r.TextB64)
		if err != nil || len(raw) > maxLogSnapshot || !utf8.Valid(raw) || base64.StdEncoding.EncodeToString(raw) != r.TextB64 {
			return false
		}
		blank.TextB64, blank.Availability, blank.Truncated = r.TextB64, r.Availability, r.Truncated
		return r == blank
	default:
		return false
	}
}

func (m *Monitor) call(parent context.Context, mode string, target *executionRequest) (result monitorResponse, err error) {
	defer func() {
		if recover() != nil {
			result = monitorResponse{}
			err = ErrProcess
		}
	}()
	if m == nil || mode != "quota" && mode != "logs" || (mode == "quota") != (target == nil) {
		return result, ErrConfig
	}
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	select {
	case m.slot <- struct{}{}:
		defer func() { <-m.slot }()
	case <-ctx.Done():
		return result, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	local, err := m.local(ctx, m.config, Local, nil)
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	if err != nil || !local.valid(Local) || local.Local != "ready" {
		return result, ErrProcess
	}
	called, violated := false, false
	err = m.credentials.WithCredential(ctx, m.config.CredentialRef, func(token []byte) error {
		if called {
			violated = true
			return ErrProtocol
		}
		called = true
		if len(token) == 0 || len(token) > credentials.MaxBytes {
			return ErrConfig
		}
		for _, b := range token {
			if b < 33 || b > 126 {
				return ErrConfig
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		result, err = m.run(ctx, m.config, mode, token, monitorRequest{Protocol: 1, Owner: m.config.AccountName, Execution: target})
		return err
	})
	if ctx.Err() != nil {
		return monitorResponse{}, ctx.Err()
	}
	if err != nil || !called || violated {
		return monitorResponse{}, ErrProcess
	} // never wrap a resolver/SDK secret
	if !result.valid(mode) {
		return monitorResponse{}, ErrProtocol
	}
	return result, nil
}
