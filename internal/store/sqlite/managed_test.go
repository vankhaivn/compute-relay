package sqlite

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/credentials"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/ports"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

const managedStateSecret = "synthetic-state-secret-canary-initial"
const managedStateRotated = "synthetic-state-secret-canary-rotated"
const managedStateOther = "synthetic-state-secret-canary-other-account"

type stateVault struct {
	mu           sync.Mutex
	values       map[string][]byte
	failCreate   error
	failDeleteAt int
	deletes      int
	createHook   func(string, bool) error
}

func (v *stateVault) Create(ctx context.Context, key string, value []byte) error {
	v.mu.Lock()
	hook := v.createHook
	fail := v.failCreate
	v.mu.Unlock()
	if key != "request_fingerprint_v1" && hook != nil {
		if err := hook(key, false); err != nil {
			return err
		}
	}
	if fail != nil {
		return fail
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	v.mu.Lock()
	prior, exists := v.values[key]
	if exists && !bytes.Equal(prior, value) {
		v.mu.Unlock()
		return credentials.ErrConflict
	}
	v.values[key] = append([]byte(nil), value...)
	v.mu.Unlock()
	if key != "request_fingerprint_v1" && hook != nil {
		return hook(key, true)
	}
	return nil
}
func (v *stateVault) WithSecret(ctx context.Context, key string, use func([]byte) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v.mu.Lock()
	value, found := v.values[key]
	owned := append([]byte(nil), value...)
	v.mu.Unlock()
	defer clear(owned)
	if !found {
		return credentials.ErrUnconfigured
	}
	return use(owned)
}
func (v *stateVault) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.deletes++
	if v.failDeleteAt > 0 && v.deletes == v.failDeleteAt {
		return credentials.ErrUnavailable
	}
	clear(v.values[key])
	delete(v.values, key)
	return nil
}

type stateAdapter struct {
	mu    sync.Mutex
	clock *dispatchClock
	calls int
	fail  error
}

func (*stateAdapter) Descriptor() connections.Descriptor {
	return connections.Descriptor{Type: "fixture", Label: "Synthetic provider", Fields: []connections.Field{{Name: "api_token", Label: "Token", Required: true, WriteOnly: true, MaxBytes: 8192}}, Capabilities: connections.Capabilities{Accelerators: []string{"cpu", "gpu"}, RemoteCancel: "unsupported", Quota: "supported"}}
}
func (*stateAdapter) Encode(fields map[string]string) ([]byte, error) {
	return []byte(fields["api_token"]), nil
}
func (a *stateAdapter) Verify(_ context.Context, value []byte) (connections.Verification, error) {
	a.mu.Lock()
	a.calls++
	fail := a.fail
	a.mu.Unlock()
	if fail != nil {
		return connections.Verification{}, fail
	}
	account := "fixture_account_alpha"
	if string(value) == managedStateOther {
		account = "fixture_account_beta"
	}
	limit, used, left := 60.0, 0.0, 60.0
	return connections.Verification{CanonicalAccount: account, Quota: provider.QuotaObservation{Status: provider.QuotaKnown, Resource: "gpu", Unit: "seconds", Limit: &limit, Used: &used, Remaining: &left, ObservedAt: a.clock.Now(), Source: "synthetic", Precision: "lower_bound"}}, nil
}
func (*stateAdapter) Profile(binding domain.ProviderBinding, account string) admission.Profile {
	return admission.DefaultProfile(binding, account)
}
func (a *stateAdapter) count() int { a.mu.Lock(); defer a.mu.Unlock(); return a.calls }

type managedStateFixture struct {
	*dispatchFixture
	access     *auth.Service
	service    *connections.Service
	vault      *stateVault
	discovery  *stateAdapter
	principals map[domain.WorkspaceID]auth.Principal
	tokens     map[domain.WorkspaceID]string
}

func newManagedStateFixture(t testing.TB) *managedStateFixture {
	t.Helper()
	f := &managedStateFixture{dispatchFixture: newDispatchFixture(t, fake.DefaultScenario()), vault: &stateVault{values: map[string][]byte{}}, principals: map[domain.WorkspaceID]auth.Principal{}, tokens: map[domain.WorkspaceID]string{}}
	f.discovery = &stateAdapter{clock: f.clock}
	f.bind(t)
	for _, w := range []domain.WorkspaceID{"a", "b"} {
		secret, _, err := f.access.Issue(context.Background(), w, []auth.Scope{auth.Read, auth.Write, auth.Operate, auth.Manage, auth.Execute}, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		f.tokens[w] = secret.Reveal()
		p, err := f.access.Authenticate(context.Background(), secret.Reveal())
		if err != nil {
			t.Fatal(err)
		}
		f.principals[w] = p
	}
	return f
}
func (f *managedStateFixture) bind(t testing.TB) {
	t.Helper()
	var err error
	f.s.options.Clock = f.clock.Now
	f.access, err = auth.New(f.s, f.s)
	if err != nil {
		t.Fatal(err)
	}
	f.service, err = connections.New(f.access, f.s, f.vault, []connections.Adapter{f.discovery})
	if err != nil {
		t.Fatal(err)
	}
}
func (f *managedStateFixture) restartManaged(t testing.TB) {
	t.Helper()
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(context.Background(), f.root, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	f.s = s
	f.bind(t)
	for w, token := range f.tokens {
		p, err := f.access.Authenticate(context.Background(), token)
		if err != nil {
			t.Fatal(err)
		}
		f.principals[w] = p
	}
}
func managedCreateBody(secret string) []byte {
	raw, _ := json.Marshal(map[string]any{"provider_type": "fixture", "label": "Synthetic connection", "credentials": map[string]string{"api_token": secret}})
	return raw
}
func managedActionBody(action string, revision int64, secret string) []byte {
	body := map[string]any{"action": action, "expected_revision": revision}
	if secret != "" {
		body["credentials"] = map[string]string{"api_token": secret}
	}
	raw, _ := json.Marshal(body)
	return raw
}
func (f *managedStateFixture) run(t testing.TB) {
	t.Helper()
	worked, err := f.service.RunOnce(context.Background())
	if err != nil || !worked {
		t.Fatal("expected one managed operation", worked, err)
	}
}
func (f *managedStateFixture) get(t testing.TB, w domain.WorkspaceID, id string) connections.Connection {
	t.Helper()
	r, err := f.service.Get(context.Background(), f.principals[w], w, id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func (f *managedStateFixture) create(t testing.TB, w domain.WorkspaceID, key, secret string) connections.Connection {
	t.Helper()
	op, err := f.service.Submit(context.Background(), f.principals[w], w, "", key, managedCreateBody(secret))
	if err != nil {
		t.Fatal(err)
	}
	f.run(t)
	r := f.get(t, w, op.ConnectionID)
	if r.Authentication != "verified" || r.Selection == nil {
		t.Fatal("no verified selection", r.Authentication)
	}
	return r
}
func (f *managedStateFixture) action(t testing.TB, w domain.WorkspaceID, c connections.Connection, key, action, secret string) connections.Operation {
	t.Helper()
	op, err := f.service.Submit(context.Background(), f.principals[w], w, c.ID, key, managedActionBody(action, c.Revision, secret))
	if err != nil {
		t.Fatal(err)
	}
	return op
}
func (f *managedStateFixture) admit(t testing.TB, w domain.WorkspaceID, c connections.Connection, key string) (admission.Receipt, admission.Record) {
	t.Helper()
	service, err := admission.New(f.access, f.s, admission.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	raw := strings.Replace(dispatchJSON, `"profile":"fixture"`, `"profile":"`+c.Selection.Profile+`"`, 1)
	raw = strings.Replace(raw, `"accelerator":"cpu"`, `"accelerator":"gpu","minimum_gpu_count":1`, 1)
	r, err := service.Submit(context.Background(), f.principals[w], w, key, []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	job, err := service.Get(context.Background(), f.principals[w], w, r.JobID)
	if err != nil {
		t.Fatal(err)
	}
	return r, job
}
func (f *managedStateFixture) resolvedSecret(t testing.TB, ref string) string {
	t.Helper()
	var result string
	err := f.service.WithCredential(context.Background(), ports.CredentialRef(ref), func(value []byte) error { result = string(value); return nil })
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestManagedConcurrentAcceptanceStoresOneConnection(t *testing.T) {
	f := newManagedStateFixture(t)
	const count = 12
	receipts := make(chan connections.Operation, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", "concurrent-create-key", managedCreateBody(managedStateSecret))
			receipts <- r
			errs <- err
		}()
	}
	wg.Wait()
	close(receipts)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	fresh := 0
	var first connections.Operation
	for r := range receipts {
		if first.ID == "" {
			first = r
		}
		if r.ID != first.ID {
			t.Fatal("concurrent replay changed operation")
		}
		if !r.Replay {
			fresh++
		}
	}
	if fresh != 1 || controlCount(t, f.s, "SELECT count(*) FROM managed_connections") != 1 {
		t.Fatal("concurrent create duplicated", fresh)
	}
	if _, err := f.service.Submit(context.Background(), f.principals["a"], "a", "", "concurrent-create-key", managedCreateBody(managedStateOther)); !errors.Is(err, connections.ErrConflict) {
		t.Fatal("changed secret reused key", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM receipts WHERE namespace='connection'") != 1 {
		t.Fatal("connection acceptance stored duplicate receipts")
	}
}

func TestManagedAliasesDoNotExpandStaticWorkspaceAllowlist(t *testing.T) {
	f := newManagedStateFixture(t)
	a := f.create(t, "a", "connection-a-key", managedStateSecret)
	f.create(t, "b", "connection-b-key", managedStateRotated)
	var allowed string
	if err := f.s.db.QueryRow("SELECT allowed_profiles FROM workspaces WHERE workspace_id='a'").Scan(&allowed); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(allowed, a.Selection.Profile) {
		t.Fatal("managed aliases expanded static workspace allowlist")
	}
}

func TestManagedCredentialMaterialAbsentFromDurableFiles(t *testing.T) {
	f := newManagedStateFixture(t)
	c := f.create(t, "a", "canary-connection-key", managedStateSecret)
	f.action(t, "a", c, "canary-rotation-key", "replace_credential", managedStateRotated)
	f.run(t)
	if err := filepath.WalkDir(f.root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, secret := range []string{managedStateSecret, managedStateRotated} {
			if bytes.Contains(data, []byte(secret)) {
				t.Fatal("SQLite state contains credential canary")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestManagedConcurrentPayloadStoresOneFingerprint(t *testing.T) {
	f := newManagedStateFixture(t)
	second, err := connections.New(f.access, f.s, f.vault, []connections.Adapter{f.discovery})
	if err != nil {
		t.Fatal(err)
	}
	const count = 12
	type result struct {
		operation connections.Operation
		err       error
	}
	results := make(chan result, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			service, secret := f.service, managedStateSecret
			if i%2 == 1 {
				service, secret = second, managedStateOther
			}
			op, err := service.Submit(context.Background(), f.principals["a"], "a", "", "mixed-payload-key", managedCreateBody(secret))
			results <- result{op, err}
		}(i)
	}
	wg.Wait()
	close(results)
	fresh, replay, conflicts := 0, 0, 0
	var id domain.OperationID
	for r := range results {
		if errors.Is(r.err, connections.ErrConflict) {
			conflicts++
			continue
		}
		if r.err != nil {
			t.Fatal(r.err)
		}
		if id == "" {
			id = r.operation.ID
		}
		if r.operation.ID != id {
			t.Fatal("changed payload created another operation")
		}
		if r.operation.Replay {
			replay++
		} else {
			fresh++
		}
	}
	if fresh != 1 || replay != count/2-1 || conflicts != count/2 || controlCount(t, f.s, "SELECT count(*) FROM managed_connections") != 1 {
		t.Fatal("changed-payload concurrency lost one fingerprint", fresh, replay, conflicts)
	}
	if controlCount(t, f.s, "SELECT count(DISTINCT request_sha256) FROM receipts WHERE namespace='connection'") != 1 {
		t.Fatal("mixed payload stored multiple fingerprints")
	}
}
