// Package kaggle contains a read-only preflight, not a dispatch-capable Provider.
// Account authentication is not evidence of private staging, GPU or batch readiness.
package kaggle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

const (
	Local         Mode = "local"
	ReadOnly      Mode = "read_only"
	PythonSeries       = "3.11"
	ClientVersion      = "2.2.4"
	SDKVersion         = "0.1.35"
)

type Mode string

var (
	ErrConfig      = errors.New("invalid Kaggle preflight configuration")
	ErrMode        = errors.New("explicit local or read-only preflight mode is required")
	ErrProcess     = errors.New("Kaggle preflight process unavailable")
	ErrProtocol    = errors.New("invalid Kaggle preflight response")
	idPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	accountPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{1,49}$`)
)

// Config is immutable, non-secret instance configuration. AccountName is a
// conservative canonical subset, not a claim about all upstream username rules.
// The interpreter must be the absolute path in the pinned client environment.
// No arbitrary command, endpoint, environment override or credential value exists.
type Config struct {
	InstanceID       string              `json:"instance_id"`
	Revision         string              `json:"revision"`
	AccountName      string              `json:"account_name"`
	CredentialRef    ports.CredentialRef `json:"credential_ref"`
	PythonExecutable string              `json:"python_executable"`
}

func (c Config) Validate() error {
	_, validRef := credentials.EnvironmentName(c.CredentialRef)
	if !idPattern.MatchString(c.InstanceID) || !idPattern.MatchString(c.Revision) ||
		!accountPattern.MatchString(c.AccountName) || !validRef ||
		!filepath.IsAbs(c.PythonExecutable) || strings.ContainsAny(c.PythonExecutable, "\x00\r\n\ufffd") {
		return ErrConfig
	}
	switch strings.ToLower(filepath.Ext(c.PythonExecutable)) {
	case ".cmd", ".bat", ".ps1", ".sh":
		return ErrConfig
	}
	return nil
}
func ParseConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) == 0 || len(raw) > 8192 || closedObject(raw, &c) != nil || c.Validate() != nil {
		return Config{}, ErrConfig
	}
	return c, nil
}

// Report contains only validated enums; no provider response, token, username,
// process output or exception text is returned. Local="ready" means exact pins
// matched. BatchReady must remain false until separately evidenced integration.
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

type invocation func(context.Context, Config, Mode, []byte) (Report, error)
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
	return &Preflight{config: c, resolver: resolver, run: runPython, slot: make(chan struct{}, 1)}, nil
}

// Check never runs automatically. ReadOnly is an explicit network/credential opt-in,
// not a dispatch permit. Each call checks local pins before looking up any secret.
// One invocation at a time per instance; a 60-second budget includes local checks,
// waiting for the slot, credential resolution and the two read-only SDK requests.
func (p *Preflight) Check(ctx context.Context, mode Mode) (Report, error) {
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
	r, err := p.run(ctx, p.config, Local, nil)
	if err != nil {
		return Report{}, err
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	if !r.valid(Local) {
		return Report{}, ErrProtocol
	}
	r.Mode = mode
	if mode == Local || r.Local != "ready" {
		return r, nil
	}
	invoked := false
	err = p.resolver.WithCredential(ctx, p.config.CredentialRef, func(secret []byte) error {
		if invoked {
			return ErrProtocol
		}
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
		r, err = p.run(ctx, p.config, ReadOnly, secret)
		return err
	})
	if ctx.Err() != nil {
		return Report{}, ctx.Err()
	}
	if err != nil {
		// A resolver or child error may embed a secret; never wrap or reflect it.
		if invoked {
			return Report{}, ErrProcess
		}
		r = baseline(ReadOnly)
		r.Authentication = "failed"
		r.Problem = "credential_unavailable"
		return r, nil
	}
	if !invoked || !r.valid(ReadOnly) {
		return Report{}, ErrProtocol
	}
	return r, nil
}
