package provider_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

type clock struct{}

func (clock) Now() time.Time { return time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC) }
func base(t *testing.T) *fake.Basic {
	t.Helper()
	b, err := fake.NewBackend(fake.DefaultScenario())
	if err != nil {
		t.Fatal(err)
	}
	p, err := fake.New(b, clock{}, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func reference() provider.RemoteReference {
	return provider.RemoteReference{Identity: fake.ExampleJob("fixture").Identity, Resource: "resource_fixture", Version: "fixture-1"}
}

func TestRegistryRejectsDuplicateMissingNilAndOptimisticProvider(t *testing.T) {
	r := provider.NewRegistry()
	p := base(t)
	if err := r.Register(p); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(p); err == nil {
		t.Fatal("duplicate registered")
	}
	if _, err := r.Lookup("missing"); err == nil {
		t.Fatal("missing instance silently fell back")
	}
	if err := r.Register(nil); err == nil {
		t.Fatal("nil registered")
	}
	var typedNil *fake.Basic
	if err := r.Register(typedNil); err == nil {
		t.Fatal("typed nil registered")
	}
	if got, err := r.Lookup("fixture"); err != nil || got != p {
		t.Fatal("explicit lookup failed")
	}
	ids := r.Instances()
	ids[0] = "mutated"
	if r.Instances()[0] != "fixture" {
		t.Fatal("registry aliases list")
	}
	if err := r.Register(optimistic{p}); err == nil {
		t.Fatal("missing cancellation implementation registered")
	}
}

type optimistic struct{ provider.Provider }

func (p optimistic) Describe() provider.Descriptor {
	d := p.Provider.Describe()
	d.InstanceID = "other"
	for i := range d.Capabilities {
		if d.Capabilities[i].Name == domain.CapabilityRemoteCancellation {
			d.Capabilities[i].Support = domain.CapabilitySupportSupported
		}
	}
	return d
}

func TestSubmissionValidationPreservesAmbiguity(t *testing.T) {
	ref := reference()
	id := ref.Identity
	rejection := provider.Problem(domain.CodeProviderRejected, domain.FailureStageSubmission, "rejected")
	unknown := provider.Problem(domain.CodeProviderSubmissionUnknown, domain.FailureStageSubmission, "unknown").WithRetrySemantics(false, true).WithRecommendedAction(domain.RecommendedActionReconcile)
	for _, out := range []provider.SubmissionOutcome{
		{Status: provider.SubmissionAccepted, Remote: &ref},
		{Status: provider.SubmissionRejected, Problem: &rejection},
		{Status: provider.SubmissionUnknown, Problem: &unknown},
	} {
		if err := out.Validate(id); err != nil {
			t.Fatal(err)
		}
	}
	badRef := ref
	badRef.Identity.AttemptID = "wrong_attempt"
	unsafe := unknown.WithRetrySemantics(true, true)
	for _, out := range []provider.SubmissionOutcome{
		{}, {Status: provider.SubmissionAccepted}, {Status: provider.SubmissionAccepted, Remote: &badRef},
		{Status: provider.SubmissionRejected, Problem: &unknown}, {Status: provider.SubmissionUnknown, Problem: &rejection},
		{Status: provider.SubmissionUnknown, Problem: &unsafe},
	} {
		if err := out.Validate(id); err == nil {
			t.Fatalf("invalid submission accepted: %+v", out)
		}
	}
	for _, result := range []provider.Reconciliation{{}, {Status: provider.ReconciliationFound}, {Status: provider.ReconciliationFound, Remote: &badRef}, {Status: provider.ReconciliationUnknown, Remote: &ref}} {
		if err := result.Validate(id); err == nil {
			t.Fatal("invalid reconciliation accepted")
		}
	}
	if err := (provider.Reconciliation{Status: provider.ReconciliationNotFound}).Validate(id); err != nil {
		t.Fatal(err)
	}
}
func TestCopyVerifiedEnforcesBoundsDigestAndCancellation(t *testing.T) {
	data := []byte("fixture bytes")
	artifact := provider.Artifact{Remote: reference(), Path: "data.json", Bytes: int64(len(data)), SHA256: provider.Digest(data)}
	tests := []struct {
		name  string
		data  []byte
		meta  provider.Artifact
		limit int64
		want  bool
	}{
		{"exact", data, artifact, 100, true},
		{"too short", data[:2], artifact, 100, false},
		{"too long", append(append([]byte{}, data...), 1), artifact, int64(len(data)), false},
		{"budget", data, artifact, 1, false},
		{"digest", []byte("differentbyte"), artifact, 100, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var dst bytes.Buffer
			receipt, err := provider.CopyVerified(context.Background(), bytes.NewReader(tc.data), &dst, tc.meta, tc.limit)
			if (err == nil) != tc.want {
				t.Fatalf("unexpected result: %+v %v", receipt, err)
			}
			if int64(dst.Len()) > tc.limit {
				t.Fatal("writer exceeded byte budget")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var dst bytes.Buffer
	if _, err := provider.CopyVerified(ctx, bytes.NewReader(data), &dst, artifact, 100); !errors.Is(err, context.Canceled) || dst.Len() != 0 {
		t.Fatal("cancelled transfer wrote bytes")
	}
	empty := artifact
	empty.Bytes = 0
	empty.SHA256 = provider.Digest(nil)
	if _, err := provider.CopyVerified(context.Background(), strings.NewReader(""), io.Discard, empty, 100); err != nil {
		t.Fatal(err)
	}
}
func TestManifestIdentityAndPathGuards(t *testing.T) {
	id := reference().Identity
	if err := provider.ValidateManifestIdentity(id, []byte(`{"manifest_version":"1","job_id":"wrong"}`)); err == nil {
		t.Fatal("wrong manifest accepted")
	}
	for _, path := range []string{"", ".", "..", "../output", "a/../b", "/absolute", `C:\x`, `a\b`, "a//b", "a/", "a\x00b", "a\x7fb"} {
		if provider.SafeArtifactPath(path) {
			t.Fatalf("unsafe path accepted: %q", path)
		}
	}
	if !provider.SafeArtifactPath("outputs/result.json") {
		t.Fatal("valid path rejected")
	}
}
func TestOptionalResultValidation(t *testing.T) {
	for _, out := range []provider.CancellationOutcome{{Status: domain.CancellationAccepted, TerminationConfirmed: true}, {Status: domain.CancellationConfirmed}, {Status: domain.CancellationNotRequested}} {
		if err := out.Validate(); err == nil {
			t.Fatal("false cancellation accepted")
		}
	}
	if err := (provider.LogPage{Source: "runtime", Availability: "live"}).Validate(provider.PageRequest{Limit: 1}); err == nil {
		t.Fatal("runtime logs substituted for provider logs")
	}
	if err := (provider.LogPage{Source: "provider", Availability: "unavailable", Lines: []string{"invented"}}).Validate(provider.PageRequest{Limit: 1}); err == nil {
		t.Fatal("invented logs accepted")
	}
	if err := (provider.QuotaObservation{Status: provider.QuotaKnown}).Validate(); err == nil {
		t.Fatal("missing quota represented as known")
	}
	nan := math.NaN()
	if err := (provider.QuotaObservation{Status: provider.QuotaUnknown, Remaining: &nan}).Validate(); err == nil {
		t.Fatal("NaN quota accepted")
	}
	if err := (provider.QuotaObservation{Status: provider.QuotaUnknown}).Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestResolvedJobValidation(t *testing.T) {
	good := fake.ExampleJob("fixture")
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*provider.ResolvedJob){
		func(j *provider.ResolvedJob) { j.Identity.Nonce = "short" },
		func(j *provider.ResolvedJob) { j.WallSeconds = 0 },
		func(j *provider.ResolvedJob) { j.Binding.ProviderInstanceID = "other" },
		func(j *provider.ResolvedJob) { j.Specification = []byte(`{}`) },
		func(j *provider.ResolvedJob) { j.Required = append(j.Required, j.Required[0]) },
	} {
		bad := good.Clone()
		mutate(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatal("invalid resolved job accepted")
		}
	}
}
