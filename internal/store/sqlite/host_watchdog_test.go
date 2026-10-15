package sqlite

import (
	"context"
	"errors"
	"fmt"
	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/hostwatchdog"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost/watchdog"
	"strings"
	"testing"
	"time"
)

type watchdogFake struct {
	apply, observe int
	ambiguous      bool
}

func (f *watchdogFake) Plan(_ context.Context, t hostwatchdog.Target) (hostwatchdog.Plan, error) {
	return hostwatchdog.Plan{UID: "501", Config: watchdog.Config{Home: "/Users/synthetic", InstallationID: t.Installation, DockerPath: "/usr/local/bin/docker", DockerHost: "unix:///var/run/docker.sock", EngineID: strings.Repeat("e", 64), CriticalThermalLevel: 2, MinimumCPUSpeedPercent: 50.5, CriticalDieTemperatureCelsius: 100, CriticalDurationSeconds: 60, LowBatteryPercent: 10.5, SampleIntervalSeconds: 30}}, nil
}
func (f *watchdogFake) Apply(context.Context, hostwatchdog.Target, hostwatchdog.Plan, string) error {
	f.apply++
	if f.ambiguous {
		return errors.New("ambiguous")
	}
	return nil
}
func (f *watchdogFake) Observe(context.Context, hostwatchdog.Target, hostwatchdog.Plan, string) (bool, error) {
	f.observe++
	return !f.ambiguous, nil
}
func (f *watchdogFake) Status(context.Context, hostwatchdog.Target, hostwatchdog.Plan) (*watchdog.Status, error) {
	return nil, nil
}
func TestWatchdogDurableIntentReplayAliasesAndRemoval(t *testing.T) {
	f, _ := healthStateFixture(t)
	ctx := context.Background()
	c := createHealthConnection(t, f, "a", "watchdog-create", "SYNTHETIC_LAN")
	alias := createHealthConnection(t, f, "b", "watchdog-alias", "SYNTHETIC_VPN")
	remote := &watchdogFake{ambiguous: true}
	service, err := hostwatchdog.New(f.access, f.s, remote)
	requireNoError(t, err)
	op, err := service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-key", "install_host_watchdog", c.Revision)
	requireNoError(t, err)
	if op.Status != "queued" || remote.apply != 0 {
		t.Fatal("HTTP submission mutated host")
	}
	_, err = service.Submit(ctx, f.principals["b"], "b", alias.ID, "watchdog-other-key", "install_host_watchdog", alias.Revision)
	if !errors.Is(err, connections.ErrConflict) {
		t.Fatal("alias concurrent mutation accepted", err)
	}
	requireNoError(t, service.RunOnce(ctx))
	if remote.apply != 1 {
		t.Fatal(remote.apply)
	}
	requireNoError(t, service.RunOnce(ctx))
	if remote.apply != 1 || remote.observe != 1 {
		t.Fatal("ambiguous mutation replayed")
	}
	replay, err := service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-key", "install_host_watchdog", c.Revision)
	requireNoError(t, err)
	if replay.ID != op.ID || !replay.Replay || replay.Status != "queued" {
		t.Fatal("receipt changed")
	}
	remote.ambiguous = false
	requireNoError(t, service.RunOnce(ctx))
	read, err := service.Operation(ctx, f.principals["a"], "a", op.ID)
	requireNoError(t, err)
	if read.Status != "succeeded" || remote.apply != 1 {
		t.Fatal(read)
	}
	_, err = service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-remove", "remove_host_watchdog", c.Revision)
	requireNoError(t, err)
	requireNoError(t, service.RunOnce(ctx))
	_, err = service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-reinstall", "install_host_watchdog", c.Revision)
	requireNoError(t, err)
	requireNoError(t, service.RunOnce(ctx))
	if remote.apply != 3 {
		t.Fatal("confirmed removal failed to unlock install")
	}
}

func TestWatchdogStaleQueuedAuthorityFailsWithoutHostMutation(t *testing.T) {
	f, _ := healthStateFixture(t)
	ctx := context.Background()
	c := createHealthConnection(t, f, "a", "watchdog-stale-create", "SYNTHETIC_LAN")
	remote := &watchdogFake{}
	service, err := hostwatchdog.New(f.access, f.s, remote)
	requireNoError(t, err)
	op, err := service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-stale-key", "install_host_watchdog", c.Revision)
	requireNoError(t, err)
	configureConnection(t, f, c, "watchdog-stale-config", 2400)
	requireNoError(t, service.RunOnce(ctx))
	read, err := service.Operation(ctx, f.principals["a"], "a", op.ID)
	requireNoError(t, err)
	if read.Status != "failed" || remote.apply != 0 {
		t.Fatal("stale authority mutated host", read, remote.apply)
	}
}
func TestWatchdogExplicitRemovalAfterAmbiguousInstallDoesNotReplay(t *testing.T) {
	f, _ := healthStateFixture(t)
	ctx := context.Background()
	c := createHealthConnection(t, f, "a", "watchdog-partial-create", "SYNTHETIC_LAN")
	remote := &watchdogFake{ambiguous: true}
	service, err := hostwatchdog.New(f.access, f.s, remote)
	requireNoError(t, err)
	op, err := service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-partial-key", "install_host_watchdog", c.Revision)
	requireNoError(t, err)
	requireNoError(t, service.RunOnce(ctx))
	removal, err := service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-partial-remove", "remove_host_watchdog", c.Revision)
	requireNoError(t, err)
	remote.ambiguous = false
	requireNoError(t, service.RunOnce(ctx))
	read, err := service.Operation(ctx, f.principals["a"], "a", op.ID)
	requireNoError(t, err)
	if read.Status != "unknown" {
		t.Fatal("partial install falsely succeeded")
	}
	done, err := service.Operation(ctx, f.principals["a"], "a", removal.ID)
	requireNoError(t, err)
	if done.Status != "succeeded" || remote.apply != 2 {
		t.Fatal("cleanup did not complete once", done, remote.apply)
	}
}

func TestWatchdogRetainsCredentialAndDisabledConnectionCanRemove(t *testing.T) {
	f, _ := healthStateFixture(t)
	ctx := context.Background()
	c := createHealthConnection(t, f, "a", "watchdog-retain-create", "SYNTHETIC_LAN")
	remote := &watchdogFake{}
	service, err := hostwatchdog.New(f.access, f.s, remote)
	requireNoError(t, err)
	_, err = service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-retain-install", "install_host_watchdog", c.Revision)
	requireNoError(t, err)
	requireNoError(t, service.RunOnce(ctx))
	for _, action := range []string{"remove", "replace_credential"} {
		body := fmt.Sprintf(`{"action":%q,"expected_revision":%d}`, action, c.Revision)
		if action == "replace_credential" {
			body = fmt.Sprintf(`{"action":%q,"expected_revision":%d,"credentials":{"api_token":"SYNTHETIC_VPN"}}`, action, c.Revision)
		}
		_, err := f.service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-block-"+action, []byte(body))
		if !errors.Is(err, connections.ErrActiveWork) {
			t.Fatal("lost only watchdog recovery credential", action, err)
		}
	}
	op := f.action(t, "a", c, "watchdog-disable", "disable", "")
	f.run(t)
	c = f.get(t, "a", op.ConnectionID)
	_, err = service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-disabled-remove", "remove_host_watchdog", c.Revision)
	requireNoError(t, err)
	requireNoError(t, service.RunOnce(ctx))
	f.action(t, "a", c, "watchdog-connection-remove", "remove", "")
}

func TestWatchdogRevokedQueuedTokenFailsAndInterruptedLeaseFencesRemoval(t *testing.T) {
	t.Run("revoked", func(t *testing.T) {
		f, _ := healthStateFixture(t)
		ctx := context.Background()
		c := createHealthConnection(t, f, "a", "watchdog-revoked-create", "SYNTHETIC_LAN")
		remote := &watchdogFake{}
		service, err := hostwatchdog.New(f.access, f.s, remote)
		requireNoError(t, err)
		op, err := service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-revoked-install", "install_host_watchdog", c.Revision)
		requireNoError(t, err)
		requireNoError(t, f.access.Revoke(ctx, f.principals["a"].TokenID()))
		requireNoError(t, service.RunOnce(ctx))
		var status, problem string
		requireNoError(t, f.s.db.QueryRow(`SELECT status,problem FROM host_watchdog_operations WHERE operation_id=?`, op.ID).Scan(&status, &problem))
		if status != "failed" || problem != "authority_revoked" || remote.apply != 0 {
			t.Fatal(status, problem, remote.apply)
		}
	})
	t.Run("interrupted lease", func(t *testing.T) {
		f, _ := healthStateFixture(t)
		ctx := context.Background()
		c := createHealthConnection(t, f, "a", "watchdog-lease-create", "SYNTHETIC_LAN")
		remote := &watchdogFake{}
		service, err := hostwatchdog.New(f.access, f.s, remote)
		requireNoError(t, err)
		op, err := service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-lease-install", "install_host_watchdog", c.Revision)
		requireNoError(t, err)
		claimed, err := f.s.ClaimWatchdog(ctx, op.ID)
		requireNoError(t, err)
		if !claimed {
			t.Fatal("intent not committed")
		}
		_, err = service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-lease-remove", "remove_host_watchdog", c.Revision)
		if !errors.Is(err, connections.ErrConflict) {
			t.Fatal("lease did not fence concurrent removal", err)
		}
		f.clock.advance(hostwatchdog.Timeout + 6*time.Second)
		_, err = service.Submit(ctx, f.principals["a"], "a", c.ID, "watchdog-lease-remove", "remove_host_watchdog", c.Revision)
		requireNoError(t, err)
		requireNoError(t, service.RunOnce(ctx))
		if remote.apply != 1 {
			t.Fatal("interrupted install reapplied", remote.apply)
		}
	})
}
