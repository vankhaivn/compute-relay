package runtimehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/cleanup"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/dispatch"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/executionauth"
	"github.com/vankhaivn/compute-relay/internal/joblogs"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
	"github.com/vankhaivn/compute-relay/internal/retention"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
	"github.com/vankhaivn/compute-relay/internal/testsupport"
	"github.com/vankhaivn/compute-relay/internal/workloadsecret"
)

func newSystemFixture(t *testing.T, scenario fake.Scenario) *managedTestFixture {
	t.Helper()
	f := newManagedTestFixtureAt(t, true, &fixedClock{now: time.Now().UTC().Truncate(time.Second)})
	f.adapter.config.MachineShape = "cpu"
	f.useFake(t, scenario)
	return f
}

func systemJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	requireNoError(t, err)
	return raw
}

func (f *managedTestFixture) advance(t *testing.T, duration time.Duration) {
	t.Helper()
	c, ok := f.clock.(*fixedClock)
	if !ok {
		t.Fatal("fixture needs a controllable clock")
	}
	c.advance(duration)
}

func (f *managedTestFixture) jobSpec(t *testing.T, connection connections.Connection) admission.Spec {
	t.Helper()
	return f.jobSpecFor(t, f.actor, "app", connection)
}

func (f *managedTestFixture) principal(t *testing.T, workspace domain.WorkspaceID, scopes ...auth.Scope) (auth.Principal, string) {
	t.Helper()
	if _, err := f.host.store.LookupWorkspace(t.Context(), workspace); errors.Is(err, auth.ErrNotFound) {
		if _, err := f.host.CreateWorkspace(t.Context(), workspace); err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	secret, _, err := f.host.access.Issue(t.Context(), workspace, scopes, time.Time{})
	requireNoError(t, err)
	actor, err := f.host.access.Authenticate(t.Context(), secret.Reveal())
	requireNoError(t, err)
	return actor, secret.Reveal()
}

func (f *managedTestFixture) connectionFor(t *testing.T, actor auth.Principal, workspace domain.WorkspaceID, key, token string) connections.Connection {
	t.Helper()
	op, err := f.services.connections.Submit(t.Context(), actor, workspace, "", key,
		[]byte(fmt.Sprintf(`{"provider_type":"kaggle","label":"Fixture","credentials":{"api_token":%q}}`, token)))
	if err != nil {
		t.Fatal("accept connection", err)
	}
	if worked, err := f.services.connections.RunOnce(t.Context()); err != nil || !worked {
		t.Fatal("verify connection", worked, err)
	}
	connection, err := f.services.connections.Get(t.Context(), actor, workspace, op.ConnectionID)
	if err != nil || connection.Selection == nil || connection.Authentication != "verified" {
		t.Fatal("verified connection unavailable", err)
	}
	return connection
}

func (f *managedTestFixture) jobSpecFor(t *testing.T, actor auth.Principal, workspace domain.WorkspaceID, connection connections.Connection) admission.Spec {
	t.Helper()
	inputs, err := objects.New(f.host.access, f.host.inputs, f.host.store)
	requireNoError(t, err)
	bundle := systemBundle(t)
	object, err := inputs.Upload(t.Context(), actor, workspace, int64(len(bundle)), provider.Digest(bundle), bytes.NewReader(bundle))
	requireNoError(t, err)
	spec := admission.Spec{APIVersion: "compute-connector/v1alpha1", Name: "system fixture", Profile: connection.Selection.Profile,
		Inputs: []admission.Input{}, Outputs: []admission.Output{{Path: "result.json", Required: true}}}
	spec.Bundle.ObjectID = object.ID
	spec.Execution.Kind, spec.Execution.Command = "python", []string{"python", "main.py"}
	spec.Resources.Accelerator = connection.Selection.Accelerator
	if spec.Resources.Accelerator == "gpu" {
		spec.Resources.MinimumGPUCount = 1
	}
	spec.Network.RemoteInternet = "disabled"
	spec.Timeouts.RemoteWallSeconds, spec.Timeouts.SetupSeconds, spec.Timeouts.FinalizationGraceSeconds = 10, 5, 2
	return spec
}

func (f *managedTestFixture) submitJob(t *testing.T, key string, spec admission.Spec) admission.Receipt {
	t.Helper()
	jobs, err := admission.New(f.host.access, f.host.store, admission.DefaultLimits())
	requireNoError(t, err)
	receipt, err := jobs.Submit(t.Context(), f.actor, "app", key, systemJSON(t, spec))
	if err != nil {
		t.Fatal("admit system job", err)
	}
	return receipt
}

func (f *managedTestFixture) authorizeJob(t *testing.T, job admission.Receipt) executionauth.Receipt {
	t.Helper()
	record, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
	requireNoError(t, err)
	raw := systemJSON(t, executionauth.Request{AttemptID: job.AttemptID, MaxRemoteWallSeconds: record.Request.Spec().Timeouts.RemoteWallSeconds, AuthorizePrivateStaging: true, AuthorizeCompute: true})
	receipt, err := f.services.authorizations.Authorize(t.Context(), f.actor, "app", job.JobID, "authorize-"+string(job.JobID), raw)
	if err != nil {
		t.Fatal("authorize system job", err)
	}
	return receipt
}

func (f *managedTestFixture) dispatchOnce(t *testing.T) {
	t.Helper()
	f.advance(t, 30*time.Second)
	if worked, err := f.services.dispatcher.RunOnce(t.Context(), "system-worker"); !worked || err != nil {
		t.Fatal("dispatch system job", worked, err)
	}
}

func (f *managedTestFixture) runToCollection(t *testing.T, job admission.Receipt) {
	t.Helper()
	for range 104 {
		record, err := f.host.store.ReadJob(t.Context(), "app", f.actor.TokenID(), job.JobID)
		requireNoError(t, err)
		if record.Attempt.State.Execution.Terminal() {
			return
		}
		attempt, err := f.host.store.ReadLogAttempt(t.Context(), "app", f.actor.TokenID(), job.JobID, job.AttemptID)
		requireNoError(t, err)
		if attempt.Remote != nil {
			control, err := fake.NewBound(f.backend, f.fixtureClock(), attempt.Binding)
			requireNoError(t, err)
			requireNoError(t, control.Advance(*attempt.Remote))
		}
		f.dispatchOnce(t)
	}
	t.Fatal("fake execution did not become terminal")
}

func (f *managedTestFixture) controls(t *testing.T) *operations.Service {
	t.Helper()
	service, err := operations.New(f.host.access, f.host.store, f.host.inputs, admission.DefaultLimits())
	requireNoError(t, err)
	return service
}

func (f *managedTestFixture) collectibleJob(t *testing.T, key, mode string) admission.Receipt {
	t.Helper()
	connection := f.connection(t, key+"-connection", "SYNTHETIC_FIRST")
	spec := f.jobSpec(t, connection)
	spec.ResultCollection = mode
	job := f.submitJob(t, key, spec)
	f.authorizeJob(t, job)
	f.runToCollection(t, job)
	return job
}

func (f *managedTestFixture) collectOnce(t *testing.T) {
	t.Helper()
	if worked, err := f.services.collector.RunOnce(t.Context()); !worked || err != nil {
		t.Fatal("collect system job", worked, err)
	}
}

func (f *managedTestFixture) cleanupEngine(t *testing.T, repo cleanup.WorkRepository) *cleanup.Engine {
	t.Helper()
	if repo == nil {
		repo = f.host.store
	}
	engine, err := cleanup.New(repo, f.services.resolver, cleanup.DefaultConfig())
	requireNoError(t, err)
	return engine
}

func systemControlBody(t *testing.T, attempt domain.AttemptID) []byte {
	t.Helper()
	return systemJSON(t, operations.Request{AttemptID: attempt, Reason: "explicit system fixture request"})
}

func (f *managedTestFixture) fixtureClock() ports.Clock {
	if f.clock != nil {
		return f.clock
	}
	return clock{}
}

func (f *managedTestFixture) compose(t *testing.T) {
	t.Helper()
	var vault credentials.Vault
	if f.vault != nil {
		vault = f.vault
	}
	var err error
	f.services, err = f.host.composeManaged(context.Background(), f.config, vault, append([]connections.Adapter{f.adapter}, f.extra...)...)
	if err != nil {
		t.Fatal("compose fixture", err)
	}
	if f.backend != nil {
		f.bindFake(t)
	}
	if f.clock != nil || f.backend != nil {
		f.clockedServices(t)
	}
}

// A fixed store clock must also govern consent, ephemeral material and engine polls.
func (f *managedTestFixture) clockedServices(t *testing.T) {
	t.Helper()
	now := f.fixtureClock()
	var err error
	f.services.authorizations, err = executionauth.New(f.host.access, f.host.store, now)
	requireNoError(t, err)
	if f.services.resolver == nil {
		return
	}
	f.services.authorizations.WithSecrets(f.host.secretPool)
	f.services.secrets, err = workloadsecret.New(f.host.access, f.host.store, f.host.secretPool, now)
	requireNoError(t, err)
	var repo dispatch.Repository = f.host.store
	if f.wrapDispatch != nil {
		repo = f.wrapDispatch(f.host.store)
	}
	cfg := dispatch.DefaultConfig()
	cfg.Workers, cfg.PollDelay = f.config.MaxWorkers, time.Millisecond
	f.services.dispatcher, err = dispatch.New(repo, f.services.resolver, f.host.inputs, now, cfg)
	requireNoError(t, err)
	f.services.dispatcher.WithSecrets(f.host.secretPool)
	collect := collection.DefaultConfig()
	collect.Workers = f.config.MaxWorkers
	f.services.collector, err = collection.New(f.host.store, f.services.resolver, f.host.results, now, collect)
	requireNoError(t, err)
}

func (f *managedTestFixture) useFake(t *testing.T, scenario fake.Scenario) {
	t.Helper()
	var err error
	f.backend, err = fake.NewBackend(scenario)
	requireNoError(t, err)
	f.bindFake(t)
	f.clockedServices(t)
}

func (f *managedTestFixture) bindFake(t *testing.T) {
	t.Helper()
	if f.services.resolver == nil {
		t.Fatal("fake provider requires the fixture vault")
	}
	f.services.resolver.make = func(binding connections.RuntimeBinding) (provider.Provider, error) {
		snapshot := provider.BindingSnapshot{Binding: binding.Profile.Binding, AccountScope: binding.Profile.AccountScope, CredentialRef: binding.Profile.CredentialRef}
		bound, err := fake.NewBound(f.backend, f.fixtureClock(), snapshot)
		if err != nil {
			return nil, err
		}
		var result provider.Provider = &systemProvider{Bound: bound}
		if f.wrapProvider != nil {
			result = f.wrapProvider(result)
		}
		return result, nil
	}
}

// Engines are stepped synchronously; join any separately started workers first.
func (f *managedTestFixture) reopen(t *testing.T) {
	t.Helper()
	f.stopHTTP(t)
	path := f.host.root.Path
	if err := f.host.Close(); err != nil {
		t.Fatal("close before reopen", err)
	}
	var err error
	f.host, err = Open(context.Background(), path)
	if err != nil {
		t.Fatal("reopen fixture", err)
	}
	if f.clock != nil {
		injectHostClock(t, f.host, f.clock.Now)
	}
	f.actor, err = f.host.access.Authenticate(context.Background(), f.token)
	if err != nil {
		t.Fatal("reauthenticate fixture", err)
	}
	f.compose(t)
}

func systemBundle(t *testing.T) []byte {
	t.Helper()
	return testsupport.Bundle(t, map[string][]byte{"main.py": []byte("raise RuntimeError('fixture must never execute on the host')\n")})
}

// Keep the fake's synthetic manifest and transfer verification; only map logical paths.
type systemProvider struct{ *fake.Bound }

func (p *systemProvider) ListArtifacts(ctx context.Context, remote provider.RemoteReference) ([]provider.Artifact, error) {
	page, err := p.Bound.ListArtifacts(ctx, remote)
	for i := range page {
		if page[i].Path == "execution-result.json" {
			page[i].Path = collection.ManifestPath
		} else {
			page[i].Path = "outputs/" + page[i].Path
		}
	}
	return page, err
}
func (p *systemProvider) Fetch(ctx context.Context, remote provider.RemoteReference, artifact provider.Artifact, dst io.Writer, limit int64, progress func(int64) error) (provider.TransferResult, error) {
	if artifact.Path == collection.ManifestPath {
		artifact.Path = "execution-result.json"
	} else {
		artifact.Path = strings.TrimPrefix(artifact.Path, "outputs/")
	}
	return p.Bound.Fetch(ctx, remote, artifact, dst, limit, progress)
}

// The callback sees a real committed result and may withhold its acknowledgement.
type systemDispatchRepository struct {
	*sqlite.Store
	afterCommit func(dispatch.Work, dispatch.Action) error
}

func (r *systemDispatchRepository) CommitDispatch(ctx context.Context, handle dispatch.Handle, action dispatch.Action) (dispatch.Work, error) {
	work, err := r.Store.CommitDispatch(ctx, handle, action)
	if err == nil && r.afterCommit != nil {
		if err = r.afterCommit(work, action); err != nil {
			return dispatch.Work{}, err
		}
	}
	return work, err
}

type systemHTTP struct {
	url    string
	cancel context.CancelFunc
	done   <-chan error
}

func (f *managedTestFixture) startHTTP(t *testing.T) {
	t.Helper()
	f.stopHTTP(t)
	cfg := api.DefaultConfig()
	var err error
	cfg.Jobs, err = admission.New(f.host.access, f.host.store, admission.DefaultLimits())
	requireNoError(t, err)
	cfg.Operations, err = operations.New(f.host.access, f.host.store, f.host.inputs, admission.DefaultLimits())
	requireNoError(t, err)
	cfg.Results, err = collection.NewReader(f.host.access, f.host.store, f.host.results)
	requireNoError(t, err)
	sweeper, err := retention.NewSweeper(f.host.store, f.host.inputs, f.host.results)
	requireNoError(t, err)
	cfg.ResultReleases, err = retention.NewReleaser(f.host.access, f.host.store, sweeper)
	requireNoError(t, err)
	cfg.Cleanup, err = cleanup.NewService(f.host.access, f.host.store, nil)
	requireNoError(t, err)
	cfg.Cleanup = cfg.Cleanup.WithPreviewResolver(f.services.resolver)
	cfg.Logs, err = joblogs.NewReader(f.host.access, f.host.store, f.services.resolver)
	requireNoError(t, err)
	cfg.Connections, cfg.Authorizations, cfg.Secrets = f.services.connections, f.services.authorizations, f.services.secrets
	cfg.GlobalRate = api.Rate{PerSecond: 10000, Burst: 10000}
	cfg.WorkspaceRate = cfg.GlobalRate
	inputs, err := objects.New(f.host.access, f.host.inputs, f.host.store)
	requireNoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	requireNoError(t, err)
	cfg.Listen = listener.Addr().String()
	server, err := api.NewServer(cfg, f.host.access, inputs, f.host.store.Ready)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready, done := make(chan struct{}), make(chan error, 1)
	f.http = &systemHTTP{url: "http://" + cfg.Listen, cancel: cancel, done: done}
	go func() {
		done <- serveHTTPMode(ctx, server, listener, time.Second, "managed-workers", func() error { close(ready); return nil })
	}()
	select {
	case <-ready:
	case err := <-done:
		f.http = nil
		cancel()
		t.Fatal("serve fixture", err)
	case <-time.After(5 * time.Second):
		t.Fatal("fixture HTTP did not start")
	}
}

func (f *managedTestFixture) stopHTTP(t *testing.T) {
	t.Helper()
	if f.http == nil {
		return
	}
	active := f.http
	f.http = nil
	active.cancel()
	select {
	case err := <-active.done:
		if err != nil {
			t.Error("stop fixture HTTP", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fixture HTTP did not join")
	}
}

func (f *managedTestFixture) request(t *testing.T, method, path, key, contentType string, body []byte, want int) (*http.Response, []byte) {
	t.Helper()
	if f.http == nil {
		t.Fatal("fixture HTTP is not running")
	}
	request, err := http.NewRequest(method, f.http.url+path, bytes.NewReader(body))
	requireNoError(t, err)
	request.Header.Set("Authorization", "Bearer "+f.token)
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if contentType == "application/octet-stream" {
		request.Header.Set("X-Content-SHA256", string(provider.Digest(body)))
	}
	response, err := boundedClient().Do(request)
	requireNoError(t, err)
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	requireNoError(t, err)
	if response.StatusCode != want {
		t.Fatalf("%s %s: status %d want %d: %s", method, path, response.StatusCode, want, raw)
	}
	return response, raw
}

func TestManagedSystemFixtureHTTPProviderAndRestart(t *testing.T) {
	ctx := context.Background()
	now := &fixedClock{now: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)}
	f := newManagedTestFixtureAt(t, true, now)
	f.adapter.config.MachineShape = "cpu"
	connection := f.connection(t, "system-connection", "SYNTHETIC_FIRST")
	binding := f.binding(t, connection)
	preparationACK, lost := false, false
	f.wrapDispatch = func(store *sqlite.Store) dispatch.Repository {
		return &systemDispatchRepository{Store: store, afterCommit: func(work dispatch.Work, action dispatch.Action) error {
			if action.Kind == dispatch.BeginPreparation {
				preparationACK = work.Journal.Plan != nil && work.Journal.PreparationID.Valid()
			}
			if action.Kind == dispatch.PreparationSeen && !lost {
				lost = true
				return sqlite.ErrUnavailable
			}
			return nil
		}}
	}
	scenario := fake.DefaultScenario()
	scenario.SeparateStart, scenario.EphemeralSecrets = true, true
	scenario.States = []domain.ExecutionState{domain.ExecutionSucceeded}
	f.useFake(t, scenario)
	f.startHTTP(t)
	call := func(method, path, key string, body []byte, target any, want int) {
		t.Helper()
		contentType := ""
		if body != nil {
			contentType = "application/json"
		}
		_, raw := f.request(t, method, path, key, contentType, body, want)
		if target != nil {
			requireNoError(t, json.Unmarshal(raw, target))
		}
	}
	marshal := func(value any) []byte {
		t.Helper()
		raw, err := json.Marshal(value)
		requireNoError(t, err)
		return raw
	}
	_, raw := f.request(t, "POST", "/v1/workspaces/app/objects", "", "application/octet-stream", systemBundle(t), http.StatusCreated)
	var bundle domain.ObjectMetadata
	if err := json.Unmarshal(raw, &bundle); err != nil || !bundle.Valid() {
		t.Fatal("bundle upload", err)
	}
	spec := []byte(fmt.Sprintf(`{"api_version":"compute-connector/v1alpha1","name":"system fixture","profile":%q,"bundle":{"object_id":%q},"execution_control":"prepare_then_start","workload_secrets":[{"name":"ingest","exposure":"file"}],"execution":{"kind":"python","command":["python","main.py"]},"inputs":[],"outputs":[{"path":"result.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":5,"finalization_grace_seconds":2}}`, connection.Selection.Profile, bundle.ID))
	var receipt admission.Receipt
	call("POST", "/v1/workspaces/app/jobs", "system-job", spec, &receipt, http.StatusAccepted)
	if receipt.CreatedAt != now.Now() || f.backend.Stats().PrepareCalls != 0 {
		t.Fatal("admission ignored the clock or performed provider effects")
	}
	prepare := executionauth.PrepareRequest{AttemptID: receipt.AttemptID, MaxPreparationSeconds: 60, ReadyUntil: now.Now().Add(10 * time.Minute).Format(time.RFC3339), MaxStagedBytes: 1 << 20, AuthorizePrivateStaging: true}
	call("POST", receipt.Links.Self+"/prepare", "system-prepare", marshal(prepare), nil, http.StatusAccepted)
	if worked, err := f.services.dispatcher.RunOnce(ctx, "system-worker"); !worked || !errors.Is(err, sqlite.ErrUnavailable) || !preparationACK || !lost || f.backend.Stats().PrepareCalls != 1 {
		t.Fatal("preparation ACK fault", worked, err, f.backend.Stats())
	}
	var ready executionauth.Readiness
	call("GET", receipt.Links.Self+"/readiness", "", nil, &ready, http.StatusOK)
	if ready.State != "ready" || !ready.Revision.Valid() {
		t.Fatal("durable preparation was not ready", ready)
	}
	supply := workloadsecret.Supply{AttemptID: receipt.AttemptID, ReadinessRevision: ready.Revision, ExpiresAt: now.Now().Add(2 * time.Minute).Format(time.RFC3339), Values: map[string][]byte{"ingest": []byte("SYNTHETIC_SYSTEM_MATERIAL")}}
	supplyRaw := marshal(supply)
	var secret workloadsecret.Receipt
	call("POST", receipt.Links.Self+"/workload-secrets", "system-material", supplyRaw, &secret, http.StatusAccepted)
	oldPool, oldStore, vault, backend := f.host.secretPool, f.host.store, f.vault, f.backend
	if !oldPool.Available(secret.Key(), secret.AuthorizationID, now.Now()) {
		t.Fatal("explicit material was not held")
	}
	f.reopen(t)
	now.advance(time.Minute) // Lost ACK cannot release a lease through the stale handle.
	if f.vault != vault || f.backend != backend || f.host.secretPool == oldPool || oldPool.Available(secret.Key(), secret.AuthorizationID, now.Now()) || f.host.secretPool.Available(secret.Key(), secret.AuthorizationID, now.Now()) {
		t.Fatal("reopen changed remote/vault identity or restored ephemeral material")
	}
	if _, err := oldStore.ReadJob(ctx, "app", f.actor.TokenID(), receipt.JobID); err == nil {
		t.Fatal("old store remained usable")
	}
	adapter, err := f.services.resolver.Resolve(binding)
	if err != nil {
		t.Fatal("original binding did not resolve", err)
	}
	changed := binding
	changed.AccountScope += "_different"
	if err := adapter.Verify(ctx, changed); err == nil {
		t.Fatal("fake accepted another account binding")
	}
	f.startHTTP(t)
	var replay admission.Receipt
	call("POST", "/v1/workspaces/app/jobs", "system-job", spec, &replay, http.StatusAccepted)
	if !replay.Replay || replay.JobID != receipt.JobID || replay.AttemptID != receipt.AttemptID || replay.CreatedAt != receipt.CreatedAt {
		t.Fatal("restart changed original admission receipt")
	}
	call("GET", receipt.Links.Self+"/readiness", "", nil, &ready, http.StatusOK)
	if ready.Secrets == nil || ready.Secrets.Status != "material_lost" {
		t.Fatal("reopen concealed lost material", ready)
	}
	var resupplied workloadsecret.Receipt
	call("POST", receipt.Links.Self+"/workload-secrets", "system-material", supplyRaw, &resupplied, http.StatusAccepted)
	if !resupplied.Replay || resupplied.AuthorizationID != secret.AuthorizationID {
		t.Fatal("resupply changed original authority")
	}
	start := executionauth.StartRequest{AttemptID: receipt.AttemptID, ReadinessRevision: ready.Revision, SecretAuthorizationID: secret.AuthorizationID, MaxRemoteWallSeconds: 10, ExecutionDeadline: now.Now().Add(9 * time.Second).Format(time.RFC3339), AuthorizeCompute: true}
	call("POST", receipt.Links.Self+"/start", "system-start", marshal(start), nil, http.StatusAccepted)
	now.advance(time.Second)
	if worked, err := f.services.dispatcher.RunOnce(ctx, "system-worker"); !worked || err != nil {
		t.Fatal("start after reopen", worked, err)
	}
	now.advance(20 * time.Second) // Submitted polling includes bounded per-attempt jitter.
	if worked, err := f.services.dispatcher.RunOnce(ctx, "system-worker"); !worked || err != nil {
		t.Fatal("terminal observation", worked, err)
	}
	if worked, err := f.services.collector.RunOnce(ctx); !worked || err != nil {
		t.Fatal("collection", worked, err)
	}
	var published struct {
		Artifacts []struct {
			ID   string `json:"artifact_id"`
			Path string `json:"path"`
		} `json:"artifacts"`
	}
	call("GET", receipt.Links.Artifacts+"?attempt_id="+string(receipt.AttemptID), "", nil, &published, http.StatusOK)
	var output string
	for _, artifact := range published.Artifacts {
		if artifact.Path == "outputs/result.json" {
			output = artifact.ID
		}
	}
	if output == "" {
		t.Fatal("verified output was not published")
	}
	_, content := f.request(t, "GET", receipt.Links.Artifacts+"/"+output+"/content?attempt_id="+string(receipt.AttemptID), "", "", nil, http.StatusOK)
	if !bytes.Equal(content, scenario.Files[0].Content) || f.backend.Stats().PrepareCalls != 1 || f.backend.Stats().SubmitCalls != 1 || f.backend.Stats().Executions != 1 || f.host.secretPool.Available(secret.Key(), secret.AuthorizationID, now.Now()) {
		t.Fatal("restart repeated compute, retained used material or changed bytes", f.backend.Stats())
	}
	call("GET", receipt.Links.Self+"/cleanup?attempt_id="+string(receipt.AttemptID), "", nil, nil, http.StatusOK)
}
