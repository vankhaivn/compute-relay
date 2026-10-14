package sshhost

import (
	"context"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/providertest"
)

type contractClock struct{}

func (contractClock) Now() time.Time { return time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC) }

// The shared contract runs the real Adapter and native Docker argument protocol.
// Only the SSH/Docker transport is simulated; no daemon or operator account is used.
func TestProviderContract(t *testing.T) {
	providertest.Run(t, func(t *testing.T, mode providertest.Mode) providertest.Harness {
		h := newHarness(t)
		h.adapter.clock = contractClock{}
		beforeStart := func() {
			switch mode {
			case providertest.Rejected:
				h.dialErr = ErrUnavailable
			case providertest.LostResponse, providertest.Unresolved:
				h.host.fail = func(args []string) (Result, bool) {
					if len(args) < 3 || args[0] != "container" || args[1] != "start" {
						return Result{}, false
					}
					if mode == providertest.LostResponse {
						// Run holds the host mutex. Simulate acceptance followed by a lost acknowledgement.
						c := h.host.find(args[len(args)-1])
						if c == nil {
							t.Fatal("start target missing")
						}
						c.status, c.started = "running", contractClock{}.Now()
						h.host.starts++
					}
					return Result{ExitCode: 1}, true
				}
			}
		}
		return providertest.Harness{
			Provider: h.adapter, Binding: h.adapter.binding, Job: h.job, BeforeStart: beforeStart,
			Preparations: func() int { h.host.mu.Lock(); defer h.host.mu.Unlock(); return h.host.created },
			Executions:   func() int { h.host.mu.Lock(); defer h.host.mu.Unlock(); return h.host.starts },
			Advance: func(provider.RemoteReference) error {
				h.host.finish(0, map[string]string{
					"control/stdout.log": "fixture output\n", "control/stderr.log": "", "control/environment.json": "{}",
					"control/execution-result.json": resultManifest(h.job.Identity, map[string]string{"answer.txt": "42"}), "outputs/answer.txt": "42",
				})
				return nil
			},
			Reconnect: func() provider.Provider {
				fresh, err := NewAdapter(h.adapter.config, h.adapter.binding, h.vault, h.blobs, contractClock{})
				if err != nil {
					t.Fatal(err)
				}
				fresh.dial = func(context.Context, Target, Credential) (session, error) { return h.host, nil }
				return fresh
			},
		}
	})
}
