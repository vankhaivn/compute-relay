package kaggle

import (
	"context"
	"encoding/base64"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
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
	LogOffset int               `json:"log_offset,omitempty"`
	LogPrefix string            `json:"log_prefix,omitempty"`
	LogLimit  int               `json:"log_limit,omitempty"`
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
	Replay       bool   `json:"replay"`
	Offset       int    `json:"offset"`
	Prefix       string `json:"prefix"`
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
		failure := provider.LogReadFailure{Reason: r.Reason}
		return (mode == "quota" && r.Reason == "read_unavailable" || mode == "logs" && failure.Valid()) && r == blank
	case "reset":
		return mode == "logs" && r.Reason == "log_changed" && r == blank
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
		if mode != "logs" || r.Reason != "none" || (r.Availability != "live" && r.Availability != "delayed" && r.Availability != "after_completion") {
			return false
		}
		raw, err := base64.StdEncoding.Strict().DecodeString(r.TextB64)
		if err != nil || len(raw) > maxLogSnapshot || !utf8.Valid(raw) || base64.StdEncoding.EncodeToString(raw) != r.TextB64 {
			return false
		}
		if !r.Replay && (r.Offset != 0 || r.Prefix != "") {
			return false
		}
		if r.Replay && (r.Offset < 0 || r.Offset > 32<<20 || !domain.SHA256Digest(r.Prefix).Valid()) {
			return false
		}
		blank.Replay, blank.Offset, blank.Prefix = r.Replay, r.Offset, r.Prefix
		blank.TextB64, blank.Availability, blank.Truncated = r.TextB64, r.Availability, r.Truncated
		return r == blank
	default:
		return false
	}
}

func (m *Monitor) call(parent context.Context, mode string, target *executionRequest) (monitorResponse, error) {
	if m == nil {
		return monitorResponse{}, ErrConfig
	}
	return m.callRequest(parent, mode, monitorRequest{Protocol: 1, Owner: m.config.AccountName, Execution: target})
}

func (m *Monitor) callRequest(parent context.Context, mode string, request monitorRequest) (result monitorResponse, err error) {
	target := request.Execution
	defer func() {
		if recover() != nil {
			result = monitorResponse{}
			err = ErrProcess
		}
	}()
	if m == nil || mode != "quota" && mode != "logs" || (mode == "quota") != (target == nil) {
		return result, ErrConfig
	}
	budget := time.Minute
	if mode == "logs" {
		budget = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, budget)
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
		result, err = m.run(ctx, m.config, mode, token, request)
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
