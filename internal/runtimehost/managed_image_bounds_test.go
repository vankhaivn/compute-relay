package runtimehost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/sshhost"
)

func TestManagedRunnerImageLimitReservesAliasesAndUnknownAcrossRestart(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	builds, recoveries := 0, 0
	adapter := &managedSSHAdapter{config: ManagedServeConfig{MaxRemoteWallSeconds: 600}, verify: sshFixtureVerify}
	adapter.install = func(ctx context.Context, raw []byte, account, machine, architecture string, operation domain.OperationID, recoverOnly bool) (sshhost.HostVerification, error) {
		mu.Lock()
		defer mu.Unlock()
		if recoverOnly {
			recoveries++
			return sshhost.HostVerification{}, sshhost.ErrImageInstallationUnknown
		}
		builds++
		return sshFixtureVerify(ctx, raw, account)
	}
	f := newManagedTestFixture(t, true, adapter)
	_, first := f.sshConnection(t, "image-cap-first", "SYNTHETIC_SSH")
	_, second := f.sshConnection(t, "image-cap-second", "SYNTHETIC_SSH_REKEYED")
	_, third := f.sshConnection(t, "image-cap-third", "SYNTHETIC_SSH")
	if *first.AccountID == *second.AccountID {
		t.Fatal("fixture did not exercise distinct account aliases of one physical host")
	}
	for i := 0; i < provider.RunnerImageCountLimit-1; i++ {
		current, err := f.services.connections.Get(ctx, f.actor, "app", first.ID)
		requireNoError(t, err)
		_, err = f.services.connections.Submit(ctx, f.actor, "app", first.ID, fmt.Sprintf("image-cap-fill-%d", i),
			[]byte(fmt.Sprintf(`{"action":"install_image","expected_revision":%d}`, current.Revision)))
		requireNoError(t, err)
		worked, err := f.services.connections.RunOnce(ctx)
		if err != nil || !worked {
			t.Fatal("initial image reservation failed", worked, err)
		}
	}
	type result struct {
		operation connections.Operation
		err       error
		body      []byte
		key       string
	}
	results := make(chan result, 2)
	for _, connection := range []connections.Connection{second, third} {
		go func(c connections.Connection) {
			body := []byte(fmt.Sprintf(`{"action":"install_image","expected_revision":%d}`, c.Revision))
			key := "image-cap-race-" + c.ID
			op, err := f.services.connections.Submit(ctx, f.actor, "app", c.ID, key, body)
			results <- result{op, err, body, key}
		}(connection)
	}
	var accepted result
	denied := 0
	for i := 0; i < 2; i++ {
		got := <-results
		if got.err == nil {
			if accepted.operation.ID != "" {
				t.Fatal("concurrent aliases exceeded the installation bound")
			}
			accepted = got
		} else if errors.Is(got.err, provider.ErrResourceCountLimit) && provider.Reason(got.err, "") == "runner_image_count_limit" {
			denied++
		} else {
			t.Fatal("unexpected admission failure", got.err)
		}
	}
	if denied != 1 || accepted.operation.ID == "" || builds != provider.RunnerImageCountLimit-1 {
		t.Fatal("reservation raced provider mutation", denied, accepted.operation.ID, builds)
	}
	// Simulate loss immediately after the durable intent, before any build reply.
	started, err := f.host.store.BeginConnectionInstall(ctx, accepted.operation.ID)
	if err != nil || !started {
		t.Fatal("reserved final slot could not start", started, err)
	}
	f.reopen(t)
	worked, err := f.services.connections.RunOnce(ctx)
	if err != nil || !worked || builds != provider.RunnerImageCountLimit-1 || recoveries != 1 {
		t.Fatal("restart rearmed an unresolved image build", worked, err, builds, recoveries)
	}
	replay, err := f.services.connections.Submit(ctx, f.actor, "app", accepted.operation.ConnectionID, accepted.key, accepted.body)
	if err != nil || !replay.Replay || replay.ID != accepted.operation.ID {
		t.Fatal("full cap rejected exact receipt replay", replay, err)
	}
	current, err := f.services.connections.Get(ctx, f.actor, "app", first.ID)
	requireNoError(t, err)
	_, err = f.services.connections.Submit(ctx, f.actor, "app", first.ID, "image-cap-after-restart",
		[]byte(fmt.Sprintf(`{"action":"install_image","expected_revision":%d}`, current.Revision)))
	if !errors.Is(err, provider.ErrResourceCountLimit) || provider.Reason(err, "") != "runner_image_count_limit" {
		t.Fatal("restart freed completed or unknown image reservations", err)
	}
	// Removing the old alias does not erase its image receipts or remote objects.
	_, err = f.services.connections.Submit(ctx, f.actor, "app", first.ID, "image-cap-remove-alias",
		[]byte(fmt.Sprintf(`{"action":"remove","expected_revision":%d}`, current.Revision)))
	requireNoError(t, err)
	if worked, err = f.services.connections.RunOnce(ctx); err != nil || !worked {
		t.Fatal("remove alias", worked, err)
	}
	_, fresh := f.sshConnection(t, "image-cap-fresh-alias", "SYNTHETIC_SSH")
	_, err = f.services.connections.Submit(ctx, f.actor, "app", fresh.ID, "image-cap-after-remove",
		[]byte(fmt.Sprintf(`{"action":"install_image","expected_revision":%d}`, fresh.Revision)))
	if !errors.Is(err, provider.ErrResourceCountLimit) || builds != provider.RunnerImageCountLimit-1 {
		t.Fatal("alias removal freed retained runner images", err, builds)
	}
}
