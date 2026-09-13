// provider-smoke is a developer-only, fixture-only executable. It is not a job runner.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC) }
func main() {
	if len(os.Args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: provider-smoke (fixture-only; no arguments)")
		os.Exit(2)
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	ctx := context.Background()
	backend, err := fake.NewBackend(fake.DefaultScenario())
	if err != nil {
		return err
	}
	p, err := fake.New(backend, fixedClock{}, "fixture")
	if err != nil {
		return err
	}
	registry := provider.NewRegistry()
	if err := registry.Register(p); err != nil {
		return err
	}
	adapter, err := registry.Lookup("fixture")
	if err != nil {
		return err
	}
	job := fake.ExampleJob("fixture")
	plan, err := adapter.Validate(ctx, job)
	if err != nil {
		return err
	}
	prepared, err := adapter.Prepare(ctx, plan, "prepare_fixture")
	if err != nil {
		return err
	}
	out := adapter.Submit(ctx, prepared)
	if err := out.Validate(job.Identity); err != nil {
		return err
	}
	if out.Status != provider.SubmissionAccepted {
		return fmt.Errorf("fixture submission was not accepted")
	}
	remote := *out.Remote
	for i := 0; i < 10; i++ {
		observation, err := adapter.Observe(ctx, remote)
		if err != nil {
			return err
		}
		if err := observation.Validate(remote); err != nil {
			return err
		}
		if observation.Execution.Terminal() {
			break
		}
		if err := p.Advance(remote); err != nil {
			return err
		}
	}
	verified := 0
	manifest := false
	page := provider.PageRequest{Limit: 1}
	for pages := 0; ; pages++ {
		if pages > 100 {
			return fmt.Errorf("pagination did not terminate")
		}
		result, err := adapter.ListArtifacts(ctx, remote, page)
		if err != nil {
			return err
		}
		for _, artifact := range result.Artifacts {
			var dst bytes.Buffer
			if _, err := adapter.FetchArtifact(ctx, remote, artifact, &dst, 1<<20); err != nil {
				return err
			}
			if artifact.Path == "execution-result.json" {
				if err := provider.ValidateManifestIdentity(job.Identity, dst.Bytes()); err != nil {
					return err
				}
				manifest = true
			}
			verified++
		}
		if result.NextCursor == "" {
			break
		}
		page.Cursor = result.NextCursor
	}
	quota, err := provider.ReadAvailableQuota(ctx, adapter)
	if err != nil {
		return err
	}
	cancel, err := provider.RequestCancellation(ctx, adapter, remote, "cancel_fixture")
	if err != nil {
		return err
	}
	if !manifest || verified < 2 || backend.Stats().Executions != 1 || quota.Status != provider.QuotaUnknown || cancel.Status != domain.CancellationManual {
		return fmt.Errorf("fixture invariant failed")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"evidence": "implemented_offline", "provider": "fake", "artifacts_verified": verified,
		"simulated_executions": backend.Stats().Executions, "quota": quota.Status, "cancellation": cancel.Status,
		"workload_executed": false, "provider_network_calls": 0,
	})
}
