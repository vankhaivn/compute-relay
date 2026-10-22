// Package kaggle implements finite private batch execution through Kaggle's API.
package kaggle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle/kaggleapi"
)

const (
	Local    Mode = "local"
	ReadOnly Mode = "read_only"
)

type Mode string

var (
	ErrConfig      = errors.New("invalid Kaggle configuration")
	ErrMode        = errors.New("explicit local or read-only preflight mode is required")
	ErrUnavailable = errors.New("Kaggle API unavailable")
	ErrProtocol    = errors.New("invalid Kaggle response")
	idPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	accountPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{1,49}$`)
)

// Config is immutable, non-secret instance configuration. AccountName is a
// conservative canonical subset, not a claim about all upstream username rules.
// No arbitrary command, endpoint, environment override or credential value exists.
// Pacing is a host-owned runtime dependency and is never serialized.
type Config struct {
	InstanceID    string                  `json:"instance_id"`
	Revision      string                  `json:"revision"`
	AccountName   string                  `json:"account_name"`
	CredentialRef ports.CredentialRef     `json:"credential_ref"`
	Pacing        *kaggleapi.RequestPacer `json:"-"`
}

func (c Config) Validate() error {
	_, vaultRef := credentials.VaultReference(c.CredentialRef)
	if !idPattern.MatchString(c.InstanceID) || !idPattern.MatchString(c.Revision) ||
		!accountPattern.MatchString(c.AccountName) || !vaultRef {
		return ErrConfig
	}
	return nil
}

// Report contains only validated enums; no provider response, token, username,
// or exception text is returned. Local="ready" means the native client is built
// in. BatchReady is independent of account authentication.
type Report struct {
	Protocol       int    `json:"protocol"`
	Mode           Mode   `json:"mode"`
	Local          string `json:"local"`
	Authentication string `json:"authentication"`
	AccountBinding string `json:"account_binding"`
	Quota          string `json:"quota"`
	BatchReady     bool   `json:"batch_ready"`
	Problem        string `json:"problem"`
}

func baseline(mode Mode) Report {
	return Report{Protocol: 1, Mode: mode, Local: "ready", Authentication: "not_checked", AccountBinding: "not_checked", Quota: "not_checked", Problem: "none"}
}
func (r Report) valid(mode Mode) bool {
	if r.Protocol != 1 || r.Mode != mode || (mode != Local && mode != ReadOnly) || r.BatchReady {
		return false
	}
	switch r.Local {
	case "unavailable", "version_mismatch":
		return r.Authentication == "not_checked" && r.AccountBinding == "not_checked" && r.Quota == "not_checked" && r.Problem == r.Local
	case "ready":
	default:
		return false
	}
	if mode == Local {
		return r == baseline(Local)
	}
	switch r.Problem {
	case "credential_unavailable", "credential_rejected":
		return r.Authentication == "failed" && r.AccountBinding == "not_checked" && r.Quota == "not_checked"
	case "access_denied", "provider_unavailable", "invalid_response":
		return r.Authentication == "unavailable" && r.AccountBinding == "not_checked" && r.Quota == "not_checked"
	case "account_mismatch":
		return r.Authentication == "verified" && r.AccountBinding == "mismatch" && r.Quota == "not_checked"
	case "quota_unavailable":
		return r.Authentication == "verified" && r.AccountBinding == "matched" && r.Quota == "unavailable"
	case "none":
		return r.Authentication == "verified" && r.AccountBinding == "matched" && (r.Quota == "available" || r.Quota == "unknown")
	default:
		return false
	}
}

// closedObject rejects duplicate keys before ordinary decoding can discard them.
// All fields are flat scalar values; unknown fields and trailing data are rejected.
func closedObject(raw []byte, target any) error {
	if !utf8.Valid(raw) {
		return ErrProtocol
	}
	allowed := map[string]bool{}
	typ := reflect.TypeOf(target).Elem()
	for i := 0; i < typ.NumField(); i++ {
		allowed[typ.Field(i).Tag.Get("json")] = true
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	t, err := d.Token()
	if err != nil || t != json.Delim('{') {
		return ErrProtocol
	}
	seen := map[string]bool{}
	for d.More() {
		t, err = d.Token()
		key, ok := t.(string)
		if err != nil || !ok || seen[key] || !allowed[key] {
			return ErrProtocol
		}
		seen[key] = true
		var v json.RawMessage
		if d.Decode(&v) != nil || bytes.Equal(v, []byte("null")) || len(v) == 0 || v[0] == '{' || v[0] == '[' {
			return ErrProtocol
		}
	}
	if _, err = d.Token(); err != nil {
		return ErrProtocol
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrProtocol
	}
	if len(seen) != len(allowed) {
		return ErrProtocol
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil {
		return ErrProtocol
	}
	return nil
}

type invocation func(context.Context, Config, Mode, []byte, bool) (Report, error)
type Preflight struct {
	config   Config
	resolver ports.CredentialResolver
	run      invocation
	slot     chan struct{}
}

func New(c Config, resolver ports.CredentialResolver) (*Preflight, error) {
	if c.Validate() != nil || resolver == nil {
		return nil, ErrConfig
	}
	return &Preflight{config: c, resolver: resolver, run: runPreflight, slot: make(chan struct{}, 1)}, nil
}

// Check never runs automatically. ReadOnly is an explicit network/credential opt-in,
// not a dispatch permit. Local needs neither credentials nor network access.
// One invocation at a time per instance; a 60-second budget includes
// waiting for the slot, credential resolution and the two read-only API requests.
func (p *Preflight) Check(ctx context.Context, mode Mode) (Report, error) {
	return p.check(ctx, mode, true)
}

func (p *Preflight) check(ctx context.Context, mode Mode, checkQuota bool) (report Report, retErr error) {
	defer func() {
		if retErr != nil {
			if errors.Is(retErr, context.Canceled) || errors.Is(retErr, context.DeadlineExceeded) {
				retErr = contextStep(retErr, "account_verification")
			} else {
				reason := "account_response_invalid"
				if errors.Is(retErr, ErrUnavailable) {
					reason = "account_read_failed"
				}
				retErr = provider.WithReason(retErr, reason)
			}
		}
	}()
	if p == nil {
		return Report{}, ErrConfig
	}
	if mode != Local && mode != ReadOnly {
		return Report{}, ErrMode
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	select {
	case p.slot <- struct{}{}:
		defer func() { <-p.slot }()
	case <-ctx.Done():
		return Report{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	r := baseline(mode)
	if mode == Local {
		return r, nil
	}
	called, invoked := false, false
	violated := false
	err := p.resolver.WithCredential(ctx, p.config.CredentialRef, func(secret []byte) error {
		if called {
			violated = true
			return ErrProtocol
		}
		called = true
		if len(secret) == 0 || len(secret) > credentials.MaxBytes {
			return credentials.ErrUnavailable
		}
		for _, b := range secret {
			if b < 33 || b > 126 {
				return credentials.ErrUnavailable
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		invoked = true
		var runErr error
		r, runErr = p.run(ctx, p.config, ReadOnly, secret, checkQuota)
		return runErr
	})
	if provider.IsLocalStateError(err) {
		return Report{}, err
	}
	if ctx.Err() != nil {
		return Report{}, ctx.Err()
	}
	if err != nil {
		// A resolver or child error may embed a secret; never wrap or reflect it.
		if invoked {
			return Report{}, ErrUnavailable
		}
		r = baseline(ReadOnly)
		r.Authentication = "failed"
		r.Problem = "credential_unavailable"
		return r, nil
	}
	// Validate the private binding-only result without inventing quota evidence.
	validated := r
	if !checkQuota && validated.Quota == "not_checked" && validated.Problem == "none" {
		validated.Quota = "unknown"
	}
	if !invoked || violated || !validated.valid(ReadOnly) {
		return Report{}, ErrProtocol
	}
	return r, nil
}
