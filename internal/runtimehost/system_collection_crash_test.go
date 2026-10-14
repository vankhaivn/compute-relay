package runtimehost

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestFaultMatrixLargeTransferFailureRecoversWithoutCompute(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "large-transfer", "automatic")
	source := f.collectionSource(t)
	// This deliberately exceeds the fake scenario's 1 MiB file limit. The transfer
	// port streams a real 16 MiB payload while the shared fake owns the execution.
	data := bytes.Repeat([]byte("0123456789abcdef"), 1<<20)
	digest := provider.Digest(data)
	source.files["outputs/result.json"] = data
	source.mode = "partial"
	e := f.collectionEngine(t, nil)
	worked, err := e.RunOnce(ctx)
	var failure collection.Failure
	if !worked || !errors.As(err, &failure) || source.fetches["outputs/result.json"] != 1 {
		t.Fatal("large partial transfer accepted", err)
	}
	if _, err = f.host.store.ReadCollection(ctx, "app", f.actor.TokenID(), job.JobID, job.AttemptID); !errors.Is(err, collection.ErrNotFound) {
		t.Fatal("partial bytes published", err)
	}
	if worked, err = e.RunOnce(ctx); worked || err != nil {
		t.Fatal("automatic retry", worked, err)
	}
	lists, _ := source.counts()
	f.reopen(t)
	f.advance(t, time.Minute)
	source.mode = ""
	if _, err = f.controls(t).Submit(ctx, f.actor, "app", job.JobID, domain.OperationCollect, "large-explicit-recovery", systemControlBody(t, job.AttemptID)); err != nil {
		t.Fatal(err)
	}
	f.collectOnce(t)
	if after, _ := source.counts(); after != lists || source.fetches["outputs/result.json"] != 2 {
		t.Fatal("recovery refreshed pin or repeated other transfers")
	}
	reader, err := collection.NewReader(f.host.access, f.host.store, f.host.results)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Read(ctx, f.actor, "app", job.JobID, job.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range result.Files {
		if file.Object.SHA256 != digest {
			continue
		}
		found = true
		_, stream, err := reader.Open(ctx, f.actor, "app", job.JobID, job.AttemptID, file.ID)
		if err != nil {
			t.Fatal(err)
		}
		actual, readErr := io.ReadAll(io.LimitReader(stream, int64(len(data))+1))
		closeErr := stream.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(actual, data) || file.Object.Bytes != int64(len(data)) {
			t.Fatal("recovered bytes differ")
		}
	}
	if !found {
		t.Fatal("large output missing")
	}
	f.assertSingleCompute(t, job)
}

func TestCollectionProcessKillAfterDurablePin(t *testing.T) {
	ctx := t.Context()
	f := newSystemFixture(t, fake.DefaultScenario())
	job := f.collectibleJob(t, "collection-process-kill", "automatic")
	source := f.collectionSource(t)
	// Build the child input from a legitimate public claim and the exact provider
	// catalog. Let that short claim expire; the child alone commits the durable pin.
	w, err := f.host.store.ClaimCollection(ctx, time.Second, 1)
	if err != nil || w == nil {
		t.Fatal(err)
	}
	p, err := f.services.resolver.Resolve(w.Binding)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := p.ListArtifacts(ctx, w.Observation.Remote)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := collection.BuildSnapshot(*w, source.files[collection.ManifestPath], catalog, collection.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "pin.json")
	if err = os.WriteFile(path, systemJSON(t, pin), 0o600); err != nil {
		t.Fatal(err)
	}
	root := f.host.root.Path
	f.stopHTTP(t)
	if err = f.host.Close(); err != nil {
		t.Fatal(err)
	}
	f.advance(t, 2*time.Second)
	call, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(call, os.Args[0], "-test.run=^TestCollectionCrashChild$")
	cmd.Env = append(os.Environ(), "SYSTEM_COLLECTION_CRASH_ROOT="+root, "SYSTEM_COLLECTION_CRASH_PIN="+path, "SYSTEM_COLLECTION_CRASH_TIME="+f.fixtureClock().Now().Format(time.RFC3339Nano))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "collection-pin-durable\n" {
		t.Fatal("child missed durable pin", line, err)
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("child was not killed")
	}
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("child did not die by SIGKILL", cmd.ProcessState)
	}
	lists, _ := source.counts()
	f.reopen(t)
	f.advance(t, time.Minute)
	f.collectOnce(t)
	after, _ := source.counts()
	status := f.collectionStatus(t, job)
	if after != lists || status.State != "available" || status.OperationID == nil || *status.OperationID != w.Lease.OperationID {
		t.Fatal("process recovery discarded original pin or ticket", status)
	}
	op, err := f.controls(t).Get(ctx, f.actor, "app", w.Lease.OperationID)
	if err != nil || op.Effect != operations.ResultsAvailable {
		t.Fatal(op, err)
	}
	f.assertSingleCompute(t, job)
}

func TestCollectionCrashChild(t *testing.T) {
	root := os.Getenv("SYSTEM_COLLECTION_CRASH_ROOT")
	if root == "" {
		return
	}
	at, err := time.Parse(time.RFC3339Nano, os.Getenv("SYSTEM_COLLECTION_CRASH_TIME"))
	if err != nil {
		t.Fatal(err)
	}
	h, err := Open(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	injectHostClock(t, h, func() time.Time { return at })
	w, err := h.store.ClaimCollection(context.Background(), 30*time.Second, 1)
	if err != nil || w == nil {
		t.Fatal("child claim", err)
	}
	raw, err := os.ReadFile(os.Getenv("SYSTEM_COLLECTION_CRASH_PIN"))
	if err != nil {
		t.Fatal(err)
	}
	var pin collection.Snapshot
	if err = json.Unmarshal(raw, &pin); err != nil {
		t.Fatal(err)
	}
	if err = h.store.PinCollection(context.Background(), *w, pin); err != nil {
		t.Fatal(err)
	}
	fmt.Println("collection-pin-durable")
	time.Sleep(time.Hour)
}
