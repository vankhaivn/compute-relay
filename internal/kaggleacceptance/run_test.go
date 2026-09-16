package kaggleacceptance

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
)

type acceptanceClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *acceptanceClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}
func (c *acceptanceClock) advance() {
	c.mu.Lock()
	c.at = c.at.Add(20 * time.Second)
	c.mu.Unlock()
}

type acceptanceFixture struct {
	t             *testing.T
	options       Options
	clock         *acceptanceClock
	backend       *fake.Backend
	constructed   int
	quota         string
	transferFault bool
	files         map[string][]byte
}

func newAcceptanceFixture(t *testing.T, mode fake.SubmitMode) *acceptanceFixture {
	t.Helper()
	scenario := fake.DefaultScenario()
	scenario.Mode, scenario.GPU = mode, domain.CapabilitySupportUnknown
	scenario.States = []domain.ExecutionState{domain.ExecutionSucceeded}
	backend, err := fake.NewBackend(scenario)
	if err != nil {
		t.Fatal(err)
	}
	return &acceptanceFixture{t: t, backend: backend, quota: "known", clock: &acceptanceClock{at: time.Now().UTC().Add(2 * time.Second)}, options: Options{
		Mode: "prepare", Root: filepath.Join(t.TempDir(), "experiment"), ProgramSHA256: provider.Digest([]byte("fixture binary")), MaxWait: time.Minute,
		Config: kaggle.Config{InstanceID: "fixture", Revision: "1", AccountName: "fixture_user", CredentialRef: "env:DO_NOT_READ", PythonExecutable: filepath.Join(t.TempDir(), "python.exe")}, MachineShape: "NvidiaTeslaT4",
	}}
}
func (f *acceptanceFixture) deps(process string) dependencies {
	return dependencies{clock: f.clock, process: process, fixture: true, pause: func(ctx context.Context) error { f.clock.advance(); return ctx.Err() }, makeProvider: func(s *session, allow bool, clock ports.Clock) (provider.Provider, error) {
		f.constructed++
		p, err := fake.NewBound(f.backend, clock, s.record.binding())
		if err != nil {
			return nil, err
		}
		return &acceptanceFake{Bound: p, f: f, s: s, allow: allow}, nil
	}}
}
func (f *acceptanceFixture) run(mode, process string) (Report, error) {
	o := f.options
	o.Mode = mode
	o.AllowPrivateStaging, o.AllowGPU, o.AllowReadOnly = mode == "submit", mode == "submit", mode == "resume" || mode == "collect"
	if mode == "collect" {
		o.CollectionKey = "explicit-collection-retry"
	}
	return runWith(context.Background(), o, f.deps(process))
}
func (f *acceptanceFixture) must(mode, process, status string) Report {
	f.t.Helper()
	report, err := f.run(mode, process)
	if err != nil || report.Status != status {
		f.t.Fatalf("%s: status=%s report=%+v err=%v", mode, report.Status, report, err)
	}
	if report.Evidence == "passed-live" || report.FullM1Acceptance {
		f.t.Fatal("fixture became live evidence")
	}
	return report
}

type acceptanceFake struct {
	*fake.Bound
	f     *acceptanceFixture
	s     *session
	allow bool
}

func (p *acceptanceFake) Describe() provider.Descriptor {
	d := p.Bound.Describe()
	for i := range d.Capabilities {
		if d.Capabilities[i].Name == domain.CapabilityQuotaReporting {
			d.Capabilities[i].Support = domain.CapabilitySupportSupported
		}
	}
	return d
}
func (p *acceptanceFake) ReadQuota(context.Context) (provider.QuotaObservation, error) {
	remaining := float64(600)
	if p.f.quota == "zero" {
		remaining = 0
	}
	q := provider.QuotaObservation{Status: provider.QuotaKnown, Resource: "gpu", Unit: "seconds", Remaining: &remaining, ObservedAt: p.f.clock.Now(), Source: "offline-fixture", Precision: "exact"}
	if p.f.quota == "unknown" {
		q.Status = provider.QuotaUnknown
		q.Remaining = nil
		q.Precision = "unknown"
	}
	return q, nil
}
func (p *acceptanceFake) Prepare(ctx context.Context, plan provider.Plan, id domain.OperationID) (provider.Prepared, error) {
	if !p.allow {
		p.f.t.Fatal("read-only recovery attempted staging")
	}
	j, err := p.s.journal(ctx)
	if err != nil || j.Plan == nil || j.Plan.Digest() != plan.Digest() {
		p.f.t.Fatal("staging preceded durable ownership", err)
	}
	return p.Bound.Prepare(ctx, plan, id)
}
func (p *acceptanceFake) Submit(ctx context.Context, prepared provider.Prepared) provider.SubmissionOutcome {
	if !p.allow {
		p.f.t.Fatal("read-only recovery attempted compute")
	}
	j, err := p.s.journal(ctx)
	var mark submissionMark
	if err != nil || !j.SubmitStarted || j.Plan == nil || readRecord(filepath.Join(p.s.root.Path, "submission-process.json"), &mark) != nil || !validMark(mark, j.Plan.Digest(), j.Plan.Job.Identity.IntentID) {
		p.f.t.Fatal("submit preceded durable intent and process marker", err)
	}
	return p.Bound.Submit(ctx, prepared)
}
func (p *acceptanceFake) evidence(ref provider.RemoteReference) map[string][]byte {
	if p.f.files != nil {
		return p.f.files
	}
	r := p.s.record
	prefix, _ := strconv.ParseInt(r.Challenge[:2], 16, 64)
	scale := int(prefix%7) + 1
	sum := int64(64 * 64 * 64 * scale)
	calculation := calculationEvidence{1, r.Receipt.JobID, r.Receipt.AttemptID, r.Challenge, r.InputSHA256, "cuda:0", 64, scale, 4096, float64(sum), sum, true, true}
	hardware := hardwareEvidence{1, "fixture-torch", "fixture-cuda", "fixture-python", "Fixture T4", "cuda:0"}
	files := map[string][]byte{"outputs/result.json": mustJSON(p.f.t, calculation), "outputs/hardware.json": mustJSON(p.f.t, hardware), "control/stdout.log": []byte("fixture stdout\n"), "control/stderr.log": {}, "control/environment.json": []byte(`{"fixture":true}`)}
	artifacts := []any{}
	for _, path := range []string{"outputs/hardware.json", "outputs/result.json"} {
		data := files[path]
		artifacts = append(artifacts, map[string]any{"path": strings.TrimPrefix(path, "outputs/"), "bytes": len(data), "sha256": provider.Digest(data)})
	}
	id := ref.Identity
	files["control/execution-result.json"] = mustJSON(p.f.t, map[string]any{"manifest_version": "1", "runner_version": "offline-fixture", "job_id": id.JobID, "attempt_id": id.AttemptID, "attempt_nonce": id.Nonce, "bundle_sha256": id.BundleSHA256, "input_manifest_sha256": id.InputManifestSHA256, "started_at": p.f.clock.Now().Add(-time.Second).Format(time.RFC3339), "finished_at": p.f.clock.Now().Format(time.RFC3339), "phase": "completed", "exit_code": 0, "timed_out": false, "resource_check": map[string]bool{"gpu_required": true, "gpu_verified": true}, "artifacts": artifacts, "error": nil})
	p.f.files = files
	return files
}
func (p *acceptanceFake) ListArtifacts(ctx context.Context, ref provider.RemoteReference, page provider.PageRequest) (provider.ArtifactPage, error) {
	if _, err := p.Bound.Observe(ctx, ref); err != nil {
		return provider.ArtifactPage{}, err
	}
	files := p.evidence(ref)
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	offset := 0
	if page.Cursor != "" {
		var err error
		offset, err = strconv.Atoi(page.Cursor)
		if err != nil || offset < 0 || offset >= len(names) {
			return provider.ArtifactPage{}, ErrEvidence
		}
	}
	end := min(offset+2, len(names))
	out := provider.ArtifactPage{}
	for _, path := range names[offset:end] {
		data := files[path]
		out.Artifacts = append(out.Artifacts, provider.Artifact{Remote: ref, Path: path, Bytes: int64(len(data)), SHA256: provider.Digest(data)})
	}
	if end < len(names) {
		out.NextCursor = strconv.Itoa(end)
	}
	return out, nil
}
func (p *acceptanceFake) FetchArtifact(ctx context.Context, ref provider.RemoteReference, file provider.Artifact, dst io.Writer, limit int64) (provider.TransferResult, error) {
	data := p.evidence(ref)[file.Path]
	if file.Remote != ref || int64(len(data)) > limit || provider.Digest(data) != file.SHA256 {
		return provider.TransferResult{}, ErrEvidence
	}
	n, err := dst.Write(data)
	if err != nil || n != len(data) {
		return provider.TransferResult{}, io.ErrShortWrite
	}
	if p.f.transferFault && strings.HasPrefix(file.Path, "outputs/") {
		return provider.TransferResult{}, errors.New("synthetic late transfer failure")
	}
	return provider.TransferResult{Bytes: int64(n), SHA256: provider.Digest(data)}, nil
}
func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

var processA = string(provider.Digest([]byte("process-a")))
var processB = string(provider.Digest([]byte("process-b")))

func TestAcceptancePrepareSubmitReopenAndVerifyOffline(t *testing.T) {
	for _, mode := range []fake.SubmitMode{fake.Accept, fake.AcceptLoseResponse} {
		t.Run(string(mode), func(t *testing.T) {
			f := newAcceptanceFixture(t, mode)
			prepared := f.must("prepare", processA, "prepared-local")
			if f.constructed != 0 {
				t.Fatal("prepare constructed provider")
			}
			if _, err := f.run("prepare", processA); err == nil {
				t.Fatal("prepare reused existing directory")
			}
			submitted := f.must("submit", processA, "resume-required")
			if submitted.JobID != prepared.JobID || submitted.AttemptID != prepared.AttemptID {
				t.Fatal("changed admitted identity")
			}
			before := f.constructed
			f.must("resume", processA, "restart-required")
			f.must("submit", processB, "resume-required")
			if f.constructed != before {
				t.Fatal("same-process or repeated submit constructed provider")
			}
			report := f.must("resume", processB, "passed-offline")
			if !report.GPUVerified || !report.RestartVerified || len(report.Artifacts) != 6 || report.AttemptNumber != 1 || report.ReleaseEvidence != domain.ReleaseEvidenceNotObservable {
				t.Fatal("missing or inflated qualification", report)
			}
			before = f.constructed
			f.must("status", processB, "passed-offline")
			if f.constructed != before {
				t.Fatal("local status constructed provider")
			}
			stats := f.backend.Stats()
			if stats.PrepareCalls != 1 || stats.SubmitCalls != 1 || stats.Executions != 1 || stats.CleanupCalls != 0 {
				t.Fatal("recovery repeated compute/cleanup", stats)
			}
		})
	}
}
func TestAcceptanceQuotaAndMissingSubmissionBlockWithoutNewEffects(t *testing.T) {
	for _, quota := range []string{"zero", "unknown"} {
		t.Run(quota, func(t *testing.T) {
			f := newAcceptanceFixture(t, fake.Accept)
			f.quota = quota
			f.must("prepare", processA, "prepared-local")
			f.must("submit", processA, "quota-blocked")
			if _, err := f.run("resume", processB); err == nil {
				t.Fatal("resume invented submission")
			}
			if stats := f.backend.Stats(); stats.PrepareCalls != 0 || stats.SubmitCalls != 0 {
				t.Fatal("quota block allowed mutation", stats)
			}
		})
	}
}
func TestAcceptanceLateCollectionFailureNeedsExplicitRecovery(t *testing.T) {
	f := newAcceptanceFixture(t, fake.Accept)
	f.must("prepare", processA, "prepared-local")
	f.must("submit", processA, "resume-required")
	f.transferFault = true
	if report, err := f.run("resume", processB); err == nil || report.Evidence == "passed-offline" {
		t.Fatal("late error qualified")
	}
	f.transferFault = false
	f.must("resume", processB, "collection-retry-required")
	f.must("collect", processB, "passed-offline")
	if f.backend.Stats().SubmitCalls != 1 {
		t.Fatal("collection resubmitted compute")
	}
}
func TestAcceptanceJournalReadRequiresCurrentOperateAuthority(t *testing.T) {
	f := newAcceptanceFixture(t, fake.Accept)
	f.must("prepare", processA, "prepared-local")
	o := f.options
	o.Mode = "status"
	s, err := openSession(context.Background(), o, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	original, err := s.journal(context.Background())
	if err != nil || original.SubmitStarted {
		t.Fatal(err)
	}
	secret, _, err := s.access.Issue(context.Background(), Workspace, []auth.Scope{auth.Read}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	readonly, err := s.access.Authenticate(context.Background(), secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.store.InspectDispatch(context.Background(), Workspace, readonly.TokenID(), s.record.Receipt.JobID, s.record.Receipt.AttemptID); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("read scope exposed private journal", err)
	}
	if _, err := s.store.InspectDispatch(context.Background(), Workspace, s.tokenID, s.record.Receipt.JobID, "foreign"); err == nil {
		t.Fatal("foreign attempt accepted")
	}
	if err := s.store.RevokeToken(context.Background(), s.tokenID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.journal(context.Background()); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("revoked authority read journal", err)
	}
}
