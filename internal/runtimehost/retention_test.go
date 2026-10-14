package runtimehost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/retention"
)

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }

type sweepStep struct {
	examined int
	next     int64
	err      error
}

type scriptedPass struct {
	steps []sweepStep
	calls []int64
}

func (p *scriptedPass) SweepOnce(_ context.Context, _ retention.Policy, after int64, _ int) (retention.SweepReport, int64, error) {
	p.calls = append(p.calls, after)
	step := p.steps[0]
	p.steps = p.steps[1:]
	return retention.SweepReport{Examined: step.examined}, step.next, step.err
}

func TestSweepRetentionFollowsTheScanAndStopsWithoutProgress(t *testing.T) {
	failed := errors.New("deletion failed")
	// A page with failed deletions still advances; an empty page restarts the tombstone
	// cursor until the inventory scan wraps.
	pass := &scriptedPass{steps: []sweepStep{{2, 4, failed}, {1, 0, nil}, {0, 0, nil}}}
	sweepRetention(context.Background(), pass)
	if fmt.Sprint(pass.calls) != "[0 4 0]" || len(pass.steps) != 0 {
		t.Fatal("pass did not follow the scan", pass.calls)
	}
	early := &scriptedPass{steps: []sweepStep{{0, 0, errors.New("expiry failed")}}}
	sweepRetention(context.Background(), early)
	if len(early.calls) != 1 {
		t.Fatal("a failure before progress did not end the pass", early.calls)
	}
}

func TestRunRetentionReturnsCleanlyOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ran := make(chan struct{}, 1)
	result := make(chan error, 1)
	go func() {
		result <- runRetention(ctx, passFunc(func() { ran <- struct{}{} }), time.Hour)
	}()
	<-ran
	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal("shutdown reported as a runtime failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retention did not stop")
	}
}

type passFunc func()

func (f passFunc) SweepOnce(context.Context, retention.Policy, int64, int) (retention.SweepReport, int64, error) {
	f()
	return retention.SweepReport{}, 0, nil
}

func TestRetentionServiceDeletesOnlyEligibleInputs(t *testing.T) {
	ctx := context.Background()
	h, err := Open(ctx, initialized(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if _, err := h.CreateWorkspace(ctx, "app"); err != nil {
		t.Fatal(err)
	}
	tokenPath := privateTokenPath(t)
	if _, err := h.IssueToken(ctx, "app", []auth.Scope{auth.Read, auth.Write}, time.Hour, tokenPath); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSuffix(string(raw), "\n")
	url, stop := startHost(t, h)
	upload := func(body string) domain.ObjectMetadata {
		sum := sha256.Sum256([]byte(body))
		receipt := requestHost(t, url+"/v1/workspaces/app/objects", "POST", token, body, map[string]string{"Content-Type": "application/octet-stream", "X-Content-SHA256": hex.EncodeToString(sum[:])}, 201)
		var object domain.ObjectMetadata
		if json.Unmarshal(receipt, &object) != nil || !object.Valid() {
			t.Fatal("invalid object receipt")
		}
		return object
	}
	referenced, unreferenced := upload("bundle bytes of an admitted job"), upload("bytes no job references")
	profile := createLocalManagedProfile(t, h)
	spec := fmt.Sprintf(`{"api_version":"compute-connector/v1alpha1","name":"retained fixture","profile":"local-test","bundle":{"object_id":%q},"execution":{"kind":"python","command":["python","main.py"]},"inputs":[],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":5,"finalization_grace_seconds":2}}`, referenced.ID)
	spec = strings.Replace(spec, `"profile":"local-test"`, `"profile":"`+profile+`"`, 1)
	requestHost(t, url+"/v1/workspaces/app/jobs", "POST", token, spec, map[string]string{"Content-Type": "application/json", "Idempotency-Key": "retention-fixture-001"}, 202)
	stop()

	clock := &fixedClock{now: time.Now().UTC().Add(time.Hour)}
	sweeper, err := h.retentionSweeper(clock)
	if err != nil {
		t.Fatal(err)
	}
	pass := func() { sweepRetention(ctx, sweeper) }
	present := func(object domain.ObjectMetadata) bool {
		stream, err := h.inputs.Open(ctx, "app", object.ID)
		if err != nil {
			return false
		}
		_ = stream.Close()
		return true
	}
	pass()
	if !present(referenced) || !present(unreferenced) {
		t.Fatal("an object younger than the unreferenced window was deleted")
	}
	clock.now = clock.now.Add(24 * time.Hour)
	pass()
	if !present(referenced) || present(unreferenced) {
		t.Fatal("expected only the unreferenced object past its window to be deleted", present(referenced), present(unreferenced))
	}
}
