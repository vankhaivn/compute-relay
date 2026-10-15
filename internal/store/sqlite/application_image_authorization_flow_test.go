package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/scheduler"
)

// These tests exercise durable control-plane authorization against an offline
// provider. The image bytes are not a Docker archive and do not qualify image loading.
func applicationImageFlow(t *testing.T, separate bool) (*dispatchFixture, scheduler.Identity, domain.ObjectMetadata) {
	t.Helper()
	var f *dispatchFixture
	if separate {
		f = gateFixture(t, fake.Accept)
	} else {
		f = managedFixture(t, fake.DefaultScenario())
	}
	ctx := context.Background()
	f.profile.Binding.Profile = "image-fixture"
	f.profile.Binding.ConfigurationRevision = "image_flow"
	f.profile.ResourceScope = "resource_" + cacheMachine
	f.profile.CostClass = admission.CostOperatorManaged
	requireNoError(t, f.s.PutProfile(ctx, f.profile, true))
	requireNoError(t, f.s.PutWorkspace(ctx, auth.Workspace{ID: "a", Enabled: true, AllowedProfiles: []string{f.profile.Binding.Profile}}))
	_, err := f.s.db.Exec(`UPDATE managed_connections SET provider_type='ssh',current_profile=? WHERE connection_id='con_fixture'`, f.profile.Binding.Profile)
	requireNoError(t, err)
	_, err = f.s.db.Exec(`INSERT INTO managed_profile_runtime(profile,revision,config) VALUES(?,?,?)`, f.profile.Binding.Profile, f.profile.Binding.ConfigurationRevision,
		[]byte(`{"schema_version":3,"image":"sha256:`+strings.Repeat("a", 64)+`","architecture":"amd64","recipe_sha256":"`+strings.Repeat("b", 64)+`","machine_id":"`+cacheMachine+`","engine_id":"`+cacheEngine+`"}`))
	requireNoError(t, err)
	_, err = f.s.db.Exec(`INSERT INTO managed_resource_accounts(account_scope,resource_kind,resource_scope) VALUES(?,'machine',?)`, f.profile.AccountScope, f.profile.ResourceScope)
	requireNoError(t, err)
	attachGate(t, f)
	image, err := f.blobs.Put(ctx, domain.ObjectMetadata{WorkspaceID: "a", ID: "image", Bytes: -1}, bytes.NewReader([]byte("offline-image-archive")))
	requireNoError(t, err)
	requireNoError(t, f.s.CommitObject(ctx, image))
	raw := strings.Replace(dispatchJSON, `"profile":"fixture"`, `"profile":"image-fixture"`, 1)
	if separate {
		raw = strings.Replace(raw, `"execution":`, `"execution_control":"prepare_then_start","execution":`, 1)
	}
	imageJSON, err := json.Marshal(admission.ApplicationImage{ObjectID: image.ID, SHA256: image.SHA256, Bytes: image.Bytes})
	requireNoError(t, err)
	raw = strings.Replace(raw, `"execution":`, `"application_image":`+string(imageJSON)+`,"execution":`, 1)
	access, err := auth.New(f.s, f.s)
	requireNoError(t, err)
	token, _, err := access.Issue(ctx, "a", []auth.Scope{auth.Read, auth.Write}, time.Time{})
	requireNoError(t, err)
	principal, err := access.Authenticate(ctx, token.Reveal())
	requireNoError(t, err)
	service, err := admission.New(access, f.s, admission.DefaultLimits())
	requireNoError(t, err)
	receipt, err := service.Submit(ctx, principal, "a", "image-flow-admission", []byte(raw))
	requireNoError(t, err)
	id := scheduler.Identity{WorkspaceID: "a", JobID: receipt.JobID, AttemptID: receipt.AttemptID}
	if f.backend.Stats().PrepareCalls != 0 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("admission performed remote work")
	}
	pin, err := f.s.PinnedImage(ctx, id.WorkspaceID, id.JobID, id.AttemptID)
	requireNoError(t, err)
	if pin == nil || pin.Released || pin.ObjectID != image.ID || pin.Key.ArchiveSHA256 != image.SHA256 || pin.Key.ArchiveBytes != image.Bytes {
		t.Fatalf("admission lost exact image pin: %+v", pin)
	}
	return f, id, image
}

func TestApplicationImageAdmissionAuthorizationDispatchFlow(t *testing.T) {
	f, id, image := applicationImageFlow(t, false)
	grant, _, _ := grantAuthorization(t, f, id)
	applicationImageSubmitted(t, f, id)
	assertApplicationImagePlan(t, f, id, image)
	var consumed int
	requireNoError(t, f.s.db.QueryRow(`SELECT count(*) FROM attempt_grants WHERE authorization_id=? AND consumed_at IS NOT NULL`, grant.AuthorizationID).Scan(&consumed))
	if consumed != 1 || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 1 {
		t.Fatalf("image attempt did not consume one grant and dispatch once: consumed=%d stats=%+v", consumed, f.backend.Stats())
	}
}

func TestApplicationImageAdmissionPrepareReadyStartFlow(t *testing.T) {
	f, id, image := applicationImageFlow(t, true)
	service, principal, ready := readyGate(t, f, id)
	assertApplicationImagePlan(t, f, id, image)
	if f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 0 {
		t.Fatalf("readiness started compute or repeated preparation: %+v", f.backend.Stats())
	}
	start, err := service.Start(t.Context(), principal, "a", id.JobID, "image-flow-start", startGrantRaw(f, id, ready))
	requireNoError(t, err)
	if worked, err := gateStep(t, f); err != nil || !worked {
		t.Fatal("start phase", worked, err)
	}
	var consumed int
	requireNoError(t, f.s.db.QueryRow(`SELECT count(*) FROM attempt_grants WHERE kind='start' AND authorization_id=? AND consumed_at IS NOT NULL`, start.AuthorizationID).Scan(&consumed))
	if consumed != 1 || f.backend.Stats().SubmitCalls != 1 {
		t.Fatalf("image start did not consume exact grant: consumed=%d stats=%+v", consumed, f.backend.Stats())
	}
}

func TestApplicationImageExplicitRetryPinsAndRequiresNewGrant(t *testing.T) {
	f, id, image := applicationImageFlow(t, false)
	originalGrant, service, principal := grantAuthorization(t, f, id)
	originalPin, err := f.s.PinnedImage(t.Context(), "a", id.JobID, id.AttemptID)
	requireNoError(t, err)
	controls, operator, _ := controlService(t, f, "a", auth.Read, auth.Operate)
	cancelControl(t, f, controls, operator, id)
	retry, err := controls.Submit(t.Context(), operator, "a", id.JobID, domain.OperationRetryCompute, "image-flow-retry", controlRequest(id.AttemptID))
	requireNoError(t, err)
	if retry.NewAttemptID == "" || retry.NewAttemptID == id.AttemptID {
		t.Fatal("explicit retry did not create a new attempt")
	}
	retried := scheduler.Identity{WorkspaceID: "a", JobID: id.JobID, AttemptID: retry.NewAttemptID}
	pin, err := f.s.PinnedImage(t.Context(), "a", id.JobID, retried.AttemptID)
	requireNoError(t, err)
	if pin == nil || pin.Released || pin.ObjectID != image.ID || pin.Key != originalPin.Key {
		t.Fatalf("retry lost exact frozen application image: original=%+v retried=%+v", originalPin, pin)
	}
	if worked, err := f.engine.RunOnce(t.Context(), "image-no-retry-consent"); err != nil || worked {
		t.Fatal("retry ran without a fresh grant", worked, err)
	}
	fresh, err := service.Authorize(t.Context(), principal, "a", id.JobID, "image-retry-grant", authorizationRequest(retried.AttemptID, 10))
	requireNoError(t, err)
	if fresh.AuthorizationID == originalGrant.AuthorizationID {
		t.Fatal("retry reused original authorization")
	}
	applicationImageSubmitted(t, f, retried)
	assertApplicationImagePlan(t, f, retried, image)
	var oldConsumed, freshConsumed int
	requireNoError(t, f.s.db.QueryRow(`SELECT count(*) FROM attempt_grants WHERE authorization_id=? AND consumed_at IS NOT NULL`, originalGrant.AuthorizationID).Scan(&oldConsumed))
	requireNoError(t, f.s.db.QueryRow(`SELECT count(*) FROM attempt_grants WHERE authorization_id=? AND consumed_at IS NOT NULL`, fresh.AuthorizationID).Scan(&freshConsumed))
	if oldConsumed != 0 || freshConsumed != 1 {
		t.Fatalf("retry consumed the wrong authority: original=%d fresh=%d", oldConsumed, freshConsumed)
	}
	if f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 1 {
		t.Fatalf("retry did not dispatch exactly once after fresh consent: %+v", f.backend.Stats())
	}
	replay, err := controls.Submit(t.Context(), operator, "a", id.JobID, domain.OperationRetryCompute, "image-flow-retry", controlRequest(id.AttemptID))
	requireNoError(t, err)
	if !replay.Replay || replay.NewAttemptID != retried.AttemptID || replay.Operation.ID != retry.Operation.ID {
		t.Fatal("retry replay changed attempt identity")
	}
}

func applicationImageSubmitted(t *testing.T, f *dispatchFixture, id scheduler.Identity) {
	t.Helper()
	for range 8 {
		if f.journal(t, id).Phase == dispatch.Submitted {
			return
		}
		if worked, err := gateStep(t, f); err != nil || !worked {
			t.Fatalf("dispatch phase %s: worked=%v err=%v", f.journal(t, id).Phase, worked, err)
		}
	}
	t.Fatal("image attempt never reached submitted")
}

func assertApplicationImagePlan(t *testing.T, f *dispatchFixture, id scheduler.Identity, image domain.ObjectMetadata) {
	t.Helper()
	journal := f.journal(t, id)
	if journal.Plan == nil || journal.Plan.Job.Inputs == nil || journal.Plan.Job.Inputs.ApplicationImage == nil || *journal.Plan.Job.Inputs.ApplicationImage != image {
		t.Fatalf("dispatch plan lost exact frozen image metadata: %+v", journal.Plan)
	}
}
