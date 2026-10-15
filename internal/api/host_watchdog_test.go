package api_test

import (
	"context"
	"fmt"
	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/hostwatchdog"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost/watchdog"
	"github.com/vankhaivn/compute-relay/internal/store/sqlite"
	"strings"
	"testing"
)

type watchdogHTTPAdapter struct{ managedAdapter }

func (a *watchdogHTTPAdapter) Descriptor() connections.Descriptor {
	d := a.managedAdapter.Descriptor()
	d.Type = "ssh"
	return d
}
func (a *watchdogHTTPAdapter) Verify(ctx context.Context, raw []byte) (connections.Verification, error) {
	v, err := a.managedAdapter.Verify(ctx, raw)
	v.Resource = &connections.ResourceIdentity{Kind: "machine", ID: "resource_" + strings.Repeat("b", 64)}
	v.RuntimeConfig = []byte(`{"engine_id":"` + strings.Repeat("e", 64) + `"}`)
	return v, err
}

type watchdogHTTPRemote struct{ effects int }

func (f *watchdogHTTPRemote) Plan(_ context.Context, t hostwatchdog.Target) (hostwatchdog.Plan, error) {
	return hostwatchdog.Plan{UID: "501", Config: watchdog.Config{Home: "/Users/synthetic", InstallationID: t.Installation, DockerPath: "/usr/local/bin/docker", DockerHost: "unix:///var/run/docker.sock", EngineID: "synthetic-engine-01", CriticalThermalLevel: 2, MinimumCPUSpeedPercent: 50, CriticalDieTemperatureCelsius: 100, CriticalDurationSeconds: 60, LowBatteryPercent: 10, SampleIntervalSeconds: 30}}, nil
}
func (f *watchdogHTTPRemote) Apply(context.Context, hostwatchdog.Target, hostwatchdog.Plan, string) error {
	f.effects++
	return nil
}
func (f *watchdogHTTPRemote) Observe(context.Context, hostwatchdog.Target, hostwatchdog.Plan, string) (bool, error) {
	return true, nil
}
func (f *watchdogHTTPRemote) Status(context.Context, hostwatchdog.Target, hostwatchdog.Plan) (*watchdog.Status, error) {
	return nil, nil
}
func TestWatchdogHTTPDedicatedReceiptsScopesAndReplay(t *testing.T) {
	var managed *connections.Service
	var service *hostwatchdog.Service
	remote := &watchdogHTTPRemote{}
	f := setupJobHTTPWith(t, jobHTTPOptions{extraScopes: map[string][]auth.Scope{"manage": {auth.Manage}}, configure: func(cfg *api.Config, access *auth.Service, store *sqlite.Store) {
		var err error
		managed, err = connections.New(access, store, &managedVault{values: map[string][]byte{}}, []connections.Adapter{&watchdogHTTPAdapter{}})
		requireNoError(t, err)
		service, err = hostwatchdog.New(access, store, remote)
		requireNoError(t, err)
		cfg.Connections = managed
		cfg.HostWatchdog = service
	}})
	_, raw := f.call(t, "POST", "/v1/workspaces/a/connections", "manage", "watchdog-http-create", strings.Replace(managedBody, `"fixture"`, `"ssh"`, 1), 202)
	created := readManagedOperation(t, raw)
	_, err := managed.RunOnce(context.Background())
	requireNoError(t, err)
	_, raw = f.call(t, "GET", "/v1/workspaces/a/connections/"+created.ConnectionID, "read", "", "", 200)
	c := decodeHTTPJSON[connections.Connection](t, raw)
	path := "/v1/workspaces/a/connections/" + c.ID + "/host-watchdog/actions/install_host_watchdog"
	body := fmt.Sprintf(`{"expected_revision":%d}`, c.Revision)
	f.call(t, "POST", path, "read", "watchdog-http-key", body, 403)
	headers, raw := f.call(t, "POST", path, "manage", "watchdog-http-key", body, 202)
	wireSchema(t, "host-watchdog-operation", raw)
	op := decodeHTTPJSON[hostwatchdog.Operation](t, raw)
	if op.Status != "queued" || remote.effects != 0 || headers.Get("Location") == "" {
		t.Fatal("admission mutated host or omitted receipt")
	}
	managedSafeResponse(t, raw)
	requireNoError(t, service.RunOnce(context.Background()))
	_, raw = f.call(t, "POST", path, "manage", "watchdog-http-key", body, 202)
	replay := decodeHTTPJSON[hostwatchdog.Operation](t, raw)
	if !replay.Replay || replay.ID != op.ID || replay.Status != "queued" || remote.effects != 1 {
		t.Fatal("replay changed receipt or retried mutation")
	}
	f.call(t, "GET", headers.Get("Location"), "read", "", "", 403)
	_, raw = f.call(t, "GET", headers.Get("Location"), "manage", "", "", 200)
	if decodeHTTPJSON[hostwatchdog.Operation](t, raw).Status != "succeeded" {
		t.Fatal("completion missing")
	}
	_, raw = f.call(t, "GET", "/v1/workspaces/a/connections/"+c.ID+"/host-watchdog", "read", "", "", 200)
	wireSchema(t, "host-watchdog-status", raw)
	managedSafeResponse(t, raw)
	f.call(t, "POST", path, "manage", "watchdog-http-key", `{"expected_revision":999}`, 409)
	f.call(t, "POST", path, "manage", "watchdog-http-duplicate", fmt.Sprintf(`{"expected_revision":%d,"expected_revision":%d}`, c.Revision, c.Revision), 400)
	f.call(t, "POST", path, "manage", "watchdog-http-extra", fmt.Sprintf(`{"expected_revision":%d,"extra":true}`, c.Revision), 400)
}
