package kaggle

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type stagingTestBlobs struct {
	data  map[domain.ObjectID][]byte
	mode  string
	reads int
}

func (b *stagingTestBlobs) Open(_ context.Context, w domain.WorkspaceID, id domain.ObjectID) (io.ReadCloser, error) {
	b.reads++
	if w != "workspace" || b.data[id] == nil || b.mode == "missing" {
		return nil, errors.New("synthetic unavailable input")
	}
	data := append([]byte(nil), b.data[id]...)
	switch b.mode {
	case "short":
		data = data[:len(data)-1]
	case "long":
		data = append(data, 'x')
	case "digest":
		data[0] ^= 1
	}
	r := io.Reader(bytes.NewReader(data))
	if b.mode == "late" {
		r = io.MultiReader(r, stagingErrorReader{})
	}
	return &stagingTestReader{Reader: r, failClose: b.mode == "close"}, nil
}

type stagingTestReader struct {
	io.Reader
	failClose bool
}

func (r *stagingTestReader) Close() error {
	if r.failClose {
		return errors.New("synthetic close acknowledgement loss")
	}
	return nil
}

type stagingErrorReader struct{}

func (stagingErrorReader) Read([]byte) (int, error) {
	return 0, errors.New("synthetic late read failure")
}

type stagingCredentialFunc func(context.Context, ports.CredentialRef, func([]byte) error) error

func (f stagingCredentialFunc) WithCredential(ctx context.Context, ref ports.CredentialRef, use func([]byte) error) error {
	return f(ctx, ref, use)
}

func stageResponse(p stagingPlan, status string) stagingResponse {
	if status == "unknown" || status == "not_found" || status == "invalid" {
		return stagingResponse{Protocol: 1, Status: status}
	}
	r := stagingResponse{Protocol: 1, Status: status, DatasetID: "123", Owner: p.request.Owner, Slug: p.request.Slug, Version: 1, MarkerSHA256: p.request.MarkerSHA256, Private: true}
	if status == "ready" {
		r.VerifiedFiles = len(p.request.Files)
		for _, f := range p.request.Files {
			r.VerifiedBytes += f.Bytes
		}
	}
	return r
}

func newTestStager(t *testing.T, allow bool) (*Stager, provider.Plan, *stagingTestBlobs, *int) {
	t.Helper()
	c, plan := stagingFixture(t)
	b := &stagingTestBlobs{data: map[domain.ObjectID][]byte{"code": []byte("synthetic-code"), "input": []byte("abc")}}
	reads := new(int)
	resolver, err := credentials.NewEnvironment([]ports.CredentialRef{c.CredentialRef}, func(string) (string, bool) { *reads++; return "SYNTHETIC_TOKEN", true })
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewStager(c, DefaultStagingPolicy(), resolver, b, allow)
	if err != nil {
		t.Fatal(err)
	}
	s.local = func(context.Context, Config, Mode, []byte) (Report, error) { return baseline(Local), nil }
	s.run = func(context.Context, Config, string, []byte, stagingPlan, StagingBlobs) (stagingResponse, error) {
		t.Fatal("unexpected helper invocation")
		return stagingResponse{}, nil
	}
	return s, plan, b, reads
}

func TestStagingCreateChecksAllInputsBeforeCredentials(t *testing.T) {
	for _, mode := range []string{"missing", "short", "long", "digest", "late", "close"} {
		t.Run(mode, func(t *testing.T) {
			s, plan, b, reads := newTestStager(t, true)
			b.mode = mode
			if _, err := s.Prepare(context.Background(), plan, "prep"); !errors.Is(err, ErrStagingInput) || *reads != 0 {
				t.Fatal("unsafe preflight", err, *reads)
			}
		})
	}
}

func TestStagingLocalFailureAndOptInMakeNoSideEffects(t *testing.T) {
	for _, mode := range []string{"disabled", "pins", "cancelled", "remapped"} {
		t.Run(mode, func(t *testing.T) {
			s, plan, b, reads := newTestStager(t, mode != "disabled")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "pins":
				s.local = func(context.Context, Config, Mode, []byte) (Report, error) {
					r := baseline(Local)
					r.Local = "version_mismatch"
					r.Problem = "version_mismatch"
					return r, nil
				}
			case "cancelled":
				cancel()
			case "remapped":
				plan.Job.Binding.ConfigurationRevision = "replacement"
			}
			if _, err := s.Prepare(ctx, plan, "prep"); err == nil || *reads != 0 || b.reads != 0 {
				t.Fatal("unsafe early failure", err)
			}
		})
	}
}

func TestStagingRecoveryReadsRemoteOnlyAndPreservesReadiness(t *testing.T) {
	for _, status := range []string{"pending", "ready", "unknown", "not_found", "invalid"} {
		t.Run(status, func(t *testing.T) {
			s, plan, b, reads := newTestStager(t, false)
			b.mode = "missing" // Recovery does not need local payloads or reupload them.
			calls := 0
			s.run = func(_ context.Context, c Config, mode string, token []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
				calls++
				if mode != "observe" || string(token) != "SYNTHETIC_TOKEN" || c != s.config {
					t.Fatal("changed recovery scope")
				}
				return stageResponse(p, status), nil
			}
			result, err := s.ReconcilePreparation(context.Background(), plan, "prep")
			if *reads != 1 || b.reads != 0 || calls != 1 {
				t.Fatal("recovery reread or repeated work")
			}
			if status == "invalid" {
				if !errors.Is(err, ErrStagingIdentity) {
					t.Fatal(err)
				}
				return
			}
			if err != nil || result.Validate(plan, "prep") != nil {
				t.Fatal(result, err)
			}
			if status == "pending" || status == "ready" {
				if result.Prepared == nil || result.Prepared.Ready != (status == "ready") || !result.Prepared.Private {
					t.Fatal("false readiness", result)
				}
			} else if result.Prepared != nil || string(result.Status) != status {
				t.Fatal("unknown became usable", result)
			}
		})
	}
}

func TestStagingRejectsFalseOrChangedHelperIdentity(t *testing.T) {
	for _, mode := range []string{"protocol", "id", "owner", "slug", "version", "marker", "private", "files", "bytes", "pending-count", "unknown-fields"} {
		t.Run(mode, func(t *testing.T) {
			s, plan, _, _ := newTestStager(t, false)
			s.run = func(_ context.Context, _ Config, _ string, _ []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
				r := stageResponse(p, "ready")
				switch mode {
				case "protocol":
					r.Protocol++
				case "id":
					r.DatasetID = "0123"
				case "owner":
					r.Owner = "another_account"
				case "slug":
					r.Slug += "0"
				case "version":
					r.Version++
				case "marker":
					r.MarkerSHA256 = provider.Digest(nil)
				case "private":
					r.Private = false
				case "files":
					r.VerifiedFiles--
				case "bytes":
					r.VerifiedBytes--
				case "pending-count":
					r.Status = "pending"
				case "unknown-fields":
					r.Status = "unknown"
				}
				return r, nil
			}
			if result, err := s.ReconcilePreparation(context.Background(), plan, "prep"); err == nil || result.Prepared != nil {
				t.Fatal("invalid observation accepted", result, err)
			}
		})
	}
}

func TestStagingCredentialAndHelperErrorsAreSanitized(t *testing.T) {
	for _, mode := range []string{"resolver", "token", "helper", "double-callback", "no-callback"} {
		t.Run(mode, func(t *testing.T) {
			s, plan, _, _ := newTestStager(t, false)
			calls := 0
			s.credentials = stagingCredentialFunc(func(ctx context.Context, ref ports.CredentialRef, use func([]byte) error) error {
				if mode == "resolver" {
					return errors.New("SYNTHETIC_SECRET")
				}
				if mode == "no-callback" {
					return nil
				}
				token := []byte("SYNTHETIC_TOKEN")
				if mode == "token" {
					token = []byte("SYNTHETIC_SECRET\n")
				}
				if err := use(token); err != nil {
					return err
				}
				if mode == "double-callback" {
					return use(token)
				}
				return nil
			})
			s.run = func(_ context.Context, _ Config, _ string, _ []byte, p stagingPlan, _ StagingBlobs) (stagingResponse, error) {
				calls++
				if mode == "helper" {
					return stagingResponse{}, errors.New("SYNTHETIC_SECRET")
				}
				return stageResponse(p, "ready"), nil
			}
			result, err := s.ReconcilePreparation(context.Background(), plan, "prep")
			if err == nil || strings.Contains(err.Error(), "SYNTHETIC_SECRET") || result.Prepared != nil || calls > 1 {
				t.Fatal("unsafe error/callback", result, err, calls)
			}
		})
	}
}
