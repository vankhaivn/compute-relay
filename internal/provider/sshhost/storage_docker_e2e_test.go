package sshhost

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

// This opt-in fixture uses only a disposable container on the local Docker
// engine. No image is pulled and no installed Relay or provider is contacted.
func TestRealDockerStorageDuringArchive(t *testing.T) {
	if os.Getenv("COMPUTE_RELAY_SSH_DOCKER_E2E") != "1" {
		t.Skip("set COMPUTE_RELAY_SSH_DOCKER_E2E=1 to use the local Docker engine")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	server := localDockerSSHServer(t, os.Environ())
	client, err := Dial(ctx, server.target(), Credential{Password: []byte(server.Password)})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	binary, err := exec.LookPath("docker")
	if err != nil {
		t.Fatal(err)
	}
	d, err := (docker{exec: client, binary: binary}).localResource(ctx)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := d.engineID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	installation := "storage_test_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	scope := hoststorage.Scope{InstallationID: domain.RuntimeInstallationID(installation), MachineID: domain.SHA256Digest(strings.Repeat("b", 64)), EngineID: domain.SHA256Digest(resourceDigest("docker-engine-id", engine))}
	var created bytes.Buffer
	result, err := d.run(ctx, nil, &created, "container", "create", "--pull=never", "--network=none", "--cpus=1", "--memory=128m", "--pids-limit=32", "--cap-drop=ALL", "--security-opt=no-new-privileges",
		"--label", storageLabel+"="+installation, "--label", labelPrefix+"engine="+engine, StandardImage,
		"python3", "-c", "with open('/tmp/output', 'wb') as f: f.write(b'x' * (64 * 1024 * 1024))")
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("create storage fixture: %v, %s", err, result.Stderr)
	}
	id := strings.TrimSpace(created.String())
	if !containerID.MatchString(id) {
		t.Fatalf("invalid fixture identity: %q", id)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		result, err := d.simple(cleanup, "container", "rm", "-f", id)
		if err != nil || result.ExitCode != 0 {
			t.Errorf("remove storage fixture: %v, %s", err, result.Stderr)
		}
	}()
	if result, err = d.simple(ctx, "container", "start", "--attach", id); err != nil || result.ExitCode != 0 {
		t.Fatalf("write storage fixture: %v, %s", err, result.Stderr)
	}
	baseline, err := scanDockerStorage(ctx, d, scope)
	if err != nil || len(baseline.Objects) != 1 || baseline.Objects[0].SizeBytes < 64<<20 {
		t.Fatalf("baseline observation: %+v, %v", baseline, err)
	}
	transfer, stopTransfer := context.WithCancel(ctx)
	receive, output := io.Pipe()
	finished := make(chan error, 1)
	go func() {
		_, err := d.copyOut(transfer, id, "/tmp/output", output)
		_ = output.CloseWithError(err)
		finished <- err
	}()
	defer func() {
		stopTransfer()
		_ = receive.Close()
		<-finished
	}()
	if _, err := io.ReadFull(receive, make([]byte, 512)); err != nil {
		t.Fatal(err)
	}
	// Leaving the archive unread holds Docker's container lock. The complete
	// storage observation must still return the same exact writable byte count.
	observe, stopObserve := context.WithTimeout(ctx, 5*time.Second)
	defer stopObserve()
	started := time.Now()
	scan, err := scanDockerStorage(observe, d, scope)
	if err != nil || len(scan.Objects) != 1 || !scan.Objects[0].SizeKnown || scan.Objects[0].SizeBytes != baseline.Objects[0].SizeBytes {
		t.Fatalf("transfer blocked or changed accounting: %+v, %v", scan, err)
	}
	select {
	case err := <-finished:
		finished <- err
		t.Fatalf("archive completed before observation: %v", err)
	default:
	}
	t.Logf("storage scan during blocked archive: %s; exact bytes: %d", time.Since(started), scan.Objects[0].SizeBytes)
}
