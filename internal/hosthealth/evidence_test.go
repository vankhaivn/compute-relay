package hosthealth_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/hosthealth"
)

func TestHealthEvidenceRequiresExactLatestProcessObservation(t *testing.T) {
	f := newServiceFixture(t)
	ctx := context.Background()
	view, err := f.service.Refresh(ctx, f.lan)
	if err != nil {
		t.Fatal(err)
	}
	proof := f.service.Evidence(f.lan.ResourceID)
	if got := proof.View(f.lan.ResourceID, view.History, f.clock.Now()); !got.Fresh || !got.ReobservedSince.Equal(f.clock.Now()) {
		t.Fatal("current observation was not confirmed", got)
	}
	changed := append([]hosthealth.Snapshot{}, view.History...)
	changed[0] = healthSample(f.lan.ResourceID, f.clock.Now(), "Linux x86_64")
	if proof.View(f.lan.ResourceID, changed, f.clock.Now()).Fresh || proof.View(f.vpn.ConnectionID, view.History, f.clock.Now()).Fresh || (hosthealth.Evidence{}).View(f.lan.ResourceID, view.History, f.clock.Now()).Fresh {
		t.Fatal("proof accepted another snapshot, resource or process")
	}
	f.clock.advance(hosthealth.DemandCache)
	view, err = f.service.Refresh(ctx, f.vpn)
	if err != nil || proof.View(f.lan.ResourceID, view.History, f.clock.Now()).Fresh {
		t.Fatal("newer observation retained old proof", err)
	}
	proof = f.service.Evidence(f.lan.ResourceID)
	if !proof.View(f.lan.ResourceID, view.History, f.clock.Now()).Fresh {
		t.Fatal("alias did not share current evidence")
	}
	restarted := f.newService(t)
	if restarted.Evidence(f.lan.ResourceID).View(f.lan.ResourceID, view.History, f.clock.Now()).Fresh {
		t.Fatal("restart reconstructed proof from history")
	}
	f.clock.advance(hosthealth.StaleAfter + time.Second)
	if proof.View(f.lan.ResourceID, view.History, f.clock.Now()).Fresh {
		t.Fatal("stale proof was accepted")
	}
}

func TestHealthEvidenceFailsClosedAfterReadWriteFailureAndClockRollback(t *testing.T) {
	for _, fault := range []string{"probe", "write", "clock"} {
		t.Run(fault, func(t *testing.T) {
			f := newServiceFixture(t)
			ctx := context.Background()
			view, err := f.service.Refresh(ctx, f.lan)
			if err != nil {
				t.Fatal(err)
			}
			proof := f.service.Evidence(f.lan.ResourceID)
			switch fault {
			case "probe":
				f.clock.advance(hosthealth.DemandCache)
				f.sample = func(context.Context, hosthealth.Target) (hosthealth.Snapshot, error) {
					return hosthealth.Snapshot{}, errors.New("synthetic read failure")
				}
				_, _ = f.service.Refresh(ctx, f.lan)
			case "write":
				f.clock.advance(hosthealth.DemandCache)
				f.repo.fail = errors.New("synthetic persistence failure")
				if _, err := f.service.Refresh(ctx, f.lan); err == nil {
					t.Fatal("persistence failure was hidden")
				}
			case "clock":
				f.clock.advance(-time.Second)
				if proof.View(f.lan.ResourceID, view.History, f.clock.Now()).Fresh {
					t.Fatal("rollback accepted proof")
				}
				f.clock.advance(time.Second)
			}
			if proof.View(f.lan.ResourceID, view.History, f.clock.Now()).Fresh || f.service.Evidence(f.lan.ResourceID).View(f.lan.ResourceID, view.History, f.clock.Now()).Fresh {
				t.Fatal("failed observation retained process authority")
			}
		})
	}
}
