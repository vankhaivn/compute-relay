package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
)

const admissionJSON = `{"api_version":"compute-connector/v1alpha1","name":"offline-admission","profile":"default-gpu","bundle":{"object_id":"code"},"execution":{"kind":"python","command":["python","never-executed.py"]},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":2,"finalization_grace_seconds":2}}`

type admissionFixture struct {
	s       *Store
	root    string
	service *admission.Service
	access  *auth.Service
	p       auth.Principal
	other   auth.Principal
	secret  string
	profile admission.Profile
}

func newAdmission(t *testing.T) admissionFixture {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state")
	s, err := Open(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	for _, w := range []string{"a", "b"} {
		if err = s.PutWorkspace(ctx, auth.Workspace{ID: domain.WorkspaceID(w), Enabled: true, AllowedProfiles: []string{"default-gpu"}}); err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"code", "data"} {
			if err = s.CommitObject(ctx, domain.ObjectMetadata{WorkspaceID: domain.WorkspaceID(w), ID: domain.ObjectID(id), Bytes: 7, SHA256: domain.SHA256Digest(strings.Repeat("a", 64))}); err != nil {
				t.Fatal(err)
			}
		}
	}
	profile := admission.DefaultProfile(domain.ProviderBinding{Profile: "default-gpu", ProviderInstanceID: "fake_1", ConfigurationRevision: "rev_1"}, "account_1")
	if err = s.PutProfile(ctx, profile, true); err != nil {
		t.Fatal(err)
	}
	access, err := auth.New(s, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := access.Issue(ctx, "a", []auth.Scope{auth.Read, auth.Write}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := access.Authenticate(ctx, token.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	other, _, err := access.Issue(ctx, "b", []auth.Scope{auth.Read, auth.Write}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	p2, err := access.Authenticate(ctx, other.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	svc, err := admission.New(access, s, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	return admissionFixture{s, path, svc, access, p, p2, token.Reveal(), profile}
}
func tableCount(t *testing.T, s *Store, table string) int {
	t.Helper()
	switch table {
	case "jobs", "attempts", "events", "idempotency", "job_objects":
	default:
		t.Fatal("unsafe test table")
	}
	var n int
	if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func submit(t *testing.T, f admissionFixture, key string) admission.Receipt {
	t.Helper()
	r, err := f.service.Submit(context.Background(), f.p, "a", key, []byte(admissionJSON))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestAdmissionReplaySurvivesRemapDisableAndRestart(t *testing.T) {
	f := newAdmission(t)
	ctx := context.Background()
	first := submit(t, f, "lost-response-key")
	before, err := f.service.Get(ctx, f.p, "a", first.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if first.Replay || first.Status != "queued" || before.Attempt.Number != 1 || before.Attempt.State != domain.InitialAttemptState() || len(before.AttemptNonce) != 64 || len(before.Objects) != 2 {
		t.Fatal("bad initial admission")
	}
	revised := f.profile
	revised.Binding.ProviderInstanceID = "fake_2"
	revised.Binding.ConfigurationRevision = "rev_2"
	revised.AccountScope = "account_2"
	if err = f.s.PutProfile(ctx, revised, true); err != nil {
		t.Fatal(err)
	}
	if err = f.s.PutProfile(ctx, revised, false); err != nil {
		t.Fatal(err)
	}
	replay, err := f.service.Submit(ctx, f.p, "a", "lost-response-key", []byte(strings.Replace(admissionJSON, ":10", ":1e1", 1)))
	if err != nil {
		t.Fatal(err)
	}
	expected := first
	expected.Replay = true
	if replay != expected {
		t.Fatal("replay changed original result")
	}
	record, err := f.service.Get(ctx, f.p, "a", first.JobID)
	if err != nil || record.Profile != f.profile || record.AttemptNonce != before.AttemptNonce {
		t.Fatal("profile/nonce changed", err)
	}
	if _, err = f.service.Submit(ctx, f.p, "a", "new-request-key", []byte(admissionJSON)); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("disabled profile used", err)
	}
	if err = f.s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, f.root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	access, _ := auth.New(s, s, nil)
	p, err := access.Authenticate(ctx, f.secret)
	if err != nil {
		t.Fatal(err)
	}
	svc, _ := admission.New(access, s, admission.DefaultLimits())
	replay, err = svc.Submit(ctx, p, "a", "lost-response-key", []byte(admissionJSON))
	if err != nil || replay != expected {
		t.Fatal("restart lost receipt", err)
	}
	if tableCount(t, s, "jobs") != 1 || tableCount(t, s, "attempts") != 1 || tableCount(t, s, "idempotency") != 1 || tableCount(t, s, "events") != 1 {
		t.Fatal("replay created rows")
	}
	for _, suffix := range []string{"", "-wal"} {
		b, _ := os.ReadFile(filepath.Join(f.root, databaseName+suffix))
		if bytes.Contains(b, []byte(f.secret)) || bytes.Contains(b, []byte("lost-response-key")) {
			t.Fatal("token/key persisted in plaintext")
		}
	}
}
func TestAdmissionConcurrentKeysAndConflicts(t *testing.T) {
	f := newAdmission(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan admission.Receipt, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := f.service.Submit(ctx, f.p, "a", "concurrent-key", []byte(admissionJSON))
			if err != nil {
				t.Error(err)
				return
			}
			results <- r
		}()
	}
	wg.Wait()
	close(results)
	var first admission.Receipt
	newCount, count := 0, 0
	for r := range results {
		count++
		if !r.Replay {
			newCount++
			first = r
		}
	}
	if count != 20 || newCount != 1 {
		t.Fatalf("new=%d total=%d", newCount, count)
	}
	if _, err := f.service.Submit(ctx, f.p, "a", "concurrent-key", []byte(strings.Replace(admissionJSON, "offline-admission", "changed", 1))); !errors.Is(err, admission.ErrConflict) {
		t.Fatal("different request did not conflict", err)
	}
	second, err := f.service.Submit(ctx, f.other, "b", "concurrent-key", []byte(admissionJSON))
	if err != nil || second.JobID == first.JobID || second.Replay {
		t.Fatal("workspace key collision", err)
	}
	if _, err = f.service.Get(ctx, f.p, "b", second.JobID); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("cross workspace route", err)
	}
	if _, err = f.service.Get(ctx, f.p, "a", second.JobID); !errors.Is(err, admission.ErrNotFound) {
		t.Fatal("foreign job leaked", err)
	}
	if tableCount(t, f.s, "jobs") != 2 || tableCount(t, f.s, "attempts") != 2 {
		t.Fatal("wrong admission count")
	}
}
func TestAdmissionAtomicRollbackAtEveryInsert(t *testing.T) {
	for _, table := range []string{"jobs", "attempts", "job_objects", "events", "idempotency"} {
		t.Run(table, func(t *testing.T) {
			f := newAdmission(t)
			// Identifiers are the static allowlist above, never input data.
			if _, err := f.s.db.Exec("CREATE TEMP TRIGGER injected_failure BEFORE INSERT ON " + table + " BEGIN SELECT RAISE(ABORT,'injected'); END;"); err != nil {
				t.Fatal(err)
			}
			r, err := f.service.Submit(context.Background(), f.p, "a", "rollback-key", []byte(admissionJSON))
			if err == nil || r.JobID != "" {
				t.Fatal("failed transaction exposed success")
			}
			for _, name := range []string{"jobs", "attempts", "job_objects", "events", "idempotency"} {
				if tableCount(t, f.s, name) != 0 {
					t.Fatalf("partial %s survived", name)
				}
			}
			if _, err = f.s.db.Exec("DROP TRIGGER injected_failure"); err != nil {
				t.Fatal(err)
			}
			if r = submit(t, f, "rollback-key"); r.Replay {
				t.Fatal("failed transaction consumed key")
			}
		})
	}
}
func TestAdmissionPinsAndRevisionImmutability(t *testing.T) {
	f := newAdmission(t)
	r := submit(t, f, "pin-input-key")
	ctx := context.Background()
	for _, query := range []string{"DELETE FROM objects WHERE workspace_id='a' AND object_id='data'", "UPDATE objects SET bytes=8 WHERE workspace_id='a' AND object_id='data'", "UPDATE job_objects SET sha256='changed'", "UPDATE jobs SET profile_revision='rev_other'", "UPDATE attempts SET nonce='changed'", "UPDATE idempotency SET request_sha256='changed'", "UPDATE events SET type='changed'"} {
		if _, err := f.s.db.Exec(query); err == nil {
			t.Fatalf("immutable reference changed by %s", query)
		}
	}
	changed := f.profile
	changed.MaxRemoteWallSeconds = 20
	if err := f.s.PutProfile(ctx, changed, true); !errors.Is(err, ErrConflict) {
		t.Fatal("revision contents overwritten", err)
	}
	if _, err := f.service.Get(ctx, f.p, "a", r.JobID); err != nil {
		t.Fatal(err)
	}
}
func TestAdmissionLimitsAndNoComputeValidation(t *testing.T) {
	f := newAdmission(t)
	svc, _ := admission.New(f.access, f.s, admission.Limits{MaxOutstandingTotal: 1, MaxOutstandingWorkspace: 1})
	ctx := context.Background()
	report, err := svc.Validate(ctx, f.p, "a", []byte(admissionJSON))
	if err != nil || !report.Valid || len(report.Requirements) == 0 || tableCount(t, f.s, "jobs") != 0 {
		t.Fatal("validation mutated admission", err)
	}
	r, err := svc.Submit(ctx, f.p, "a", "limit-first", []byte(admissionJSON))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = svc.Submit(ctx, f.p, "a", "limit-second", []byte(admissionJSON)); !errors.Is(err, admission.ErrLimit) {
		t.Fatal("limit bypassed", err)
	}
	repeat, err := svc.Submit(ctx, f.p, "a", "limit-first", []byte(admissionJSON))
	if err != nil || !repeat.Replay || repeat.JobID != r.JobID {
		t.Fatal("limit blocked replay", err)
	}
}
func TestPendingHTTPSIsDurableWithoutFetching(t *testing.T) {
	f := newAdmission(t)
	ctx := context.Background()
	raw := strings.Replace(admissionJSON, `"kind":"object","object_id":"data"`, `"kind":"https","url":"https://example.com/public?version=1"`, 1)
	r, err := f.service.Submit(ctx, f.p, "a", "https-pending", []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	record, err := f.service.Get(ctx, f.p, "a", r.JobID)
	if err != nil || record.PendingHTTPS != 1 || len(record.Objects) != 1 || record.Attempt.State.Execution != domain.ExecutionNotSubmitted {
		t.Fatal("pending URL made optimistic ready/compute claim", err)
	}
	if record.Request.Spec().Inputs[0].Source.URL != "https://example.com/public?version=1" {
		t.Fatal("source lost")
	}
	again, err := f.service.Submit(ctx, f.p, "a", "https-pending", []byte(raw))
	if err != nil || !again.Replay {
		t.Fatal(err)
	}
	// There is intentionally no downloader/provider dependency in Service or Repository.
}
func TestMissingForeignInputsAndRevokedActorsFailClosed(t *testing.T) {
	f := newAdmission(t)
	ctx := context.Background()
	foreign := domain.ObjectMetadata{ID: "foreign_only", WorkspaceID: "b", Bytes: 7, SHA256: domain.SHA256Digest(strings.Repeat("b", 64))}
	if err := f.s.CommitObject(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"foreign_only", "missing"} {
		raw := strings.Replace(admissionJSON, `"object_id":"data"`, `"object_id":"`+id+`"`, 1)
		if _, err := f.service.Submit(ctx, f.p, "a", "missing-key-"+id, []byte(raw)); !errors.Is(err, admission.ErrInputs) {
			t.Fatal("missing/foreign input", err)
		}
	}
	req, err := admission.Parse([]byte(admissionJSON))
	if err != nil {
		t.Fatal(err)
	}
	key, _ := admission.KeyDigest("revoked-transaction")
	if err = f.s.RevokeToken(ctx, f.p.TokenID()); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.AdmitJob(ctx, "a", f.p.TokenID(), key, req, admission.DefaultLimits()); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("transaction trusted stale authority", err)
	}
	if tableCount(t, f.s, "jobs") != 0 {
		t.Fatal("rejected request committed")
	}
}
func TestAttemptStateAndEventCommitAtomically(t *testing.T) {
	f := newAdmission(t)
	ctx := context.Background()
	r := submit(t, f, "state-event-key")
	before, err := f.s.LoadAttempt(ctx, "a", r.JobID, r.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	state := before.State
	state.Orchestration = domain.OrchestrationPreparing
	after, err := before.Transition(state, before.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	change := ports.AttemptChange{WorkspaceID: "a", Before: before, After: after, Event: domain.Event{ID: "evt_next", Sequence: 2, WorkspaceID: "a", JobID: r.JobID, AttemptID: r.AttemptID, Type: domain.EventInputsReady, OccurredAt: after.UpdatedAt}}
	if err = f.s.CommitAttempt(ctx, change); err != nil {
		t.Fatal(err)
	}
	if err = f.s.CommitAttempt(ctx, change); !errors.Is(err, ErrConflict) {
		t.Fatal("stale revision accepted", err)
	}
	latest, err := f.s.LoadAttempt(ctx, "a", r.JobID, r.AttemptID)
	if err != nil || latest != after || tableCount(t, f.s, "events") != 2 {
		t.Fatal("state/event not paired", err)
	}
	state = after.State
	state.Orchestration = domain.OrchestrationState("blocked")
	next, err := after.Transition(state, after.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	change.Before = after
	change.After = next
	change.Event.Sequence = 3
	change.Event.OccurredAt = next.UpdatedAt
	// Reusing evt_next violates event uniqueness AFTER the state UPDATE and must roll it back.
	if err = f.s.CommitAttempt(ctx, change); !errors.Is(err, ErrConflict) {
		t.Fatal("event uniqueness failure accepted", err)
	}
	latest, err = f.s.LoadAttempt(ctx, "a", r.JobID, r.AttemptID)
	if err != nil || latest != after || tableCount(t, f.s, "events") != 2 {
		t.Fatal("event failure left state update", err)
	}
	replay := submit(t, f, "state-event-key")
	if !replay.Replay || replay.Status != "queued" {
		t.Fatal("original receipt rewritten with current state")
	}
}
func TestDiskFullAdmissionLeavesNoPartialRows(t *testing.T) {
	f := newAdmission(t)
	var pages int
	if err := f.s.db.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.db.Exec("PRAGMA max_page_count=" + strconv.Itoa(pages)); err != nil {
		t.Fatal(err)
	}
	var request map[string]any
	if err := json.Unmarshal([]byte(admissionJSON), &request); err != nil {
		t.Fatal(err)
	}
	environment := map[string]string{}
	for i := 0; i < 64; i++ {
		environment["FIELD_"+strconv.Itoa(i)] = strings.Repeat("x", 4096)
	}
	request["execution"].(map[string]any)["environment"] = environment
	raw, _ := json.Marshal(request)
	r, err := f.service.Submit(context.Background(), f.p, "a", "disk-full-key", raw)
	if !errors.Is(err, ErrDiskFull) || r.JobID != "" {
		t.Fatalf("disk failure: %v", err)
	}
	for _, table := range []string{"jobs", "attempts", "events", "idempotency", "job_objects"} {
		if tableCount(t, f.s, table) != 0 {
			t.Fatal("disk-full left partial rows")
		}
	}
	if _, err = f.s.db.Exec("PRAGMA max_page_count=1073741823"); err != nil {
		t.Fatal(err)
	}
	if r = submit(t, f, "disk-full-key"); r.Replay {
		t.Fatal("disk failure consumed key")
	}
}
