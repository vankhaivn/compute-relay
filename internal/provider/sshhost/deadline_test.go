package sshhost

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

type deadlineClock struct{ at time.Time }

func (c *deadlineClock) Now() time.Time { return c.at }

func deadlineHarness(t *testing.T) (*harness, *deadlineClock) {
	t.Helper()
	h := newHarness(t)
	c := &deadlineClock{at: time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)}
	h.adapter.clock = c
	h.host.engineTime = c.at
	h.job = testJob(t, strings.Replace(testSpec, `"timeouts":{`, `"timeouts":{"execution_deadline":"2026-10-12T00:01:00Z",`, 1), h.blobs)
	h.job.Required = append(h.job.Required, domain.CapabilityAbsoluteDeadline)
	return h, c
}

func TestDeadlinePreparationAndStartGates(t *testing.T) {
	t.Run("expired before preparation has no host effects", func(t *testing.T) {
		h, c := deadlineHarness(t)
		plan, err := h.adapter.Validate(context.Background(), h.job)
		if err != nil {
			t.Fatal(err)
		}
		c.at = c.at.Add(time.Minute)
		if _, err = h.adapter.Prepare(context.Background(), plan, testOperation); !errors.Is(err, admission.ErrExpired) || h.dials != 0 {
			t.Fatal("expired preparation contacted the host", err, h.dials)
		}
	})
	for _, kind := range []string{"expired", "remote_expired", "clock_behind", "clock_ahead", "normal"} {
		t.Run(kind, func(t *testing.T) {
			h, c := deadlineHarness(t)
			_, prepared := h.prepare(t)
			container := h.host.container(t)
			if !slices.Contains(container.args, "--supervise") || slices.Contains(container.args, "timeout") || container.labels[labelPrefix+"execution-deadline"] != "2026-10-12T00:01:00Z" {
				t.Fatal("absolute watchdog was not frozen into the container")
			}
			switch kind {
			case "expired":
				c.at = c.at.Add(time.Minute)
			case "remote_expired":
				c.at = c.at.Add(58 * time.Second)
				h.host.engineTime = c.at.Add(2 * time.Second)
			case "clock_behind":
				h.host.engineTime = c.at.Add(-6 * time.Second)
			case "clock_ahead":
				h.host.engineTime = c.at.Add(6 * time.Second)
			}
			out := h.adapter.Start(context.Background(), prepared, nil, nil)
			if out.Validate(h.job.Identity) != nil {
				t.Fatal("invalid submission outcome")
			}
			if kind == "normal" {
				if out.Status != provider.SubmissionAccepted || h.host.starts != 1 {
					t.Fatal("valid start refused", out.Status)
				}
				// Even after expiry an earlier Docker start is possible execution, never
				// a fresh rejection that could release capacity or authorize replacement.
				c.at = c.at.Add(2 * time.Minute)
				again := h.adapter.Start(context.Background(), prepared, nil, nil)
				seen, err := h.adapter.Reconcile(context.Background(), h.job.Identity)
				if again.Status != provider.SubmissionUnknown || err != nil || seen.Status != provider.ReconciliationFound || *seen.Remote != *out.Remote || h.host.starts != 1 {
					t.Fatal("deadline rearmed or obscured the original start", err)
				}
			} else if out.Status != provider.SubmissionRejected || h.host.starts != 0 || out.Problem == nil {
				t.Fatal("unsafe deadline/clock start", kind, out.Status)
			} else if (kind == "expired" || kind == "remote_expired") && out.Problem.Code != domain.CodeRemoteTimeout {
				t.Fatal("expiry reason lost")
			}
		})
	}
}
