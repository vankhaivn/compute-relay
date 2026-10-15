package sshhost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostcache"
	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

type storageExecutor struct {
	foreignID    string
	inspectCalls []string
	listCalls    []string
	truncate     bool
	labels       map[string]string
}

func (e *storageExecutor) Run(_ context.Context, _ map[string]string, argv []string, _ io.Reader, stdout io.Writer) (Result, error) {
	command := strings.Join(argv[1:], " ")
	var response string
	switch {
	case strings.HasPrefix(command, "info --format"):
		response = "NATIVEENGINE1234567890\n"
	case strings.HasPrefix(command, "container ls"):
		e.listCalls = append(e.listCalls, command)
		own := strings.Repeat("a", 64)
		if strings.Contains(command, "installation=installation_1") {
			response = own + "\n"
		} else {
			response = own + "\n" + e.foreignID + "\n"
		}
	case strings.HasPrefix(command, "volume ls"), strings.HasPrefix(command, "image ls"):
		if e.truncate {
			response = strings.Repeat("x", hoststorage.ScanMaxBytes+1)
		}
	case strings.HasPrefix(command, "container inspect --size"):
		e.inspectCalls = append(e.inspectCalls, command)
		if strings.Contains(command, e.foreignID) {
			return Result{ExitCode: 1}, nil
		}
		labels := e.labels
		if labels == nil {
			labels = map[string]string{storageLabel: "installation_1", labelPrefix + "engine": "NATIVEENGINE1234567890", labelPrefix + "attempt": "attempt_1", labelPrefix + "workspace": "workspace_1"}
		}
		encoded, err := json.Marshal(labels)
		if err != nil {
			return Result{}, err
		}
		response = fmt.Sprintf(`[{"Id":"%s","SizeRw":128,"State":{"Running":false},"Config":{"Labels":%s},"Mounts":[]}]`, strings.Repeat("a", 64), encoded)
	default:
		return Result{ExitCode: 1}, nil
	}
	_, err := io.WriteString(stdout, response)
	return Result{}, err
}

func TestStorageScanRecognizesCurrentCacheVerifierLabels(t *testing.T) {
	scope := hoststorage.Scope{InstallationID: "installation_1", MachineID: domain.SHA256Digest(strings.Repeat("b", 64)), EngineID: domain.SHA256Digest(strings.Repeat("c", 64))}
	g := hostcache.Generation{Key: hostcache.Key{InstallationID: scope.InstallationID, MachineID: scope.MachineID, EngineID: scope.EngineID,
		SHA256: domain.SHA256Digest(strings.Repeat("d", 64)), Bytes: 7}, ID: "hcgen_" + strings.Repeat("e", 32),
		OperationID: "op_cache_verify", CreatedAt: time.Now().UTC()}
	args := cacheHelperLabels(g, "verification")
	labels := map[string]string{}
	for i := 0; i < len(args); i += 2 {
		if args[i] != "--label" {
			t.Fatalf("unexpected helper label arguments: %v", args)
		}
		key, value, ok := strings.Cut(args[i+1], "=")
		if !ok {
			t.Fatal("helper emitted malformed label")
		}
		labels[key] = value
	}
	exec := &storageExecutor{foreignID: strings.Repeat("f", 64), labels: labels}
	scan, err := scanDockerStorage(t.Context(), docker{exec: exec, binary: "/usr/bin/docker"}, scope)
	if err != nil || len(scan.Objects) != 1 || !scan.Objects[0].SizeKnown || scan.Objects[0].SizeBytes != 128 {
		t.Fatalf("current cache verifier made storage unknown: %+v, %v", scan, err)
	}
	labels[labelPrefix+"engine"] = strings.Repeat("a", 64)
	scan, err = scanDockerStorage(t.Context(), docker{exec: exec, binary: "/usr/bin/docker"}, scope)
	if err != nil || len(scan.Objects) != 1 || scan.Objects[0].SizeKnown {
		t.Fatalf("mismatched cache verifier supplied a trusted size: %+v, %v", scan, err)
	}
	legacy := map[string]string{storageLabel: "installation_1", labelPrefix + "kind": "verification", labelPrefix + "attempt": "attempt_1", labelPrefix + "operation": "op_verifier"}
	if !validStorageLabels(hoststorage.Container, legacy, scope, "NATIVEENGINE1234567890") {
		t.Fatal("legacy staging verifier labels became unrecognized")
	}
}

func TestStorageScanFiltersForeignObjectsAndBoundsOutput(t *testing.T) {
	scope := hoststorage.Scope{InstallationID: "installation_1", MachineID: domain.SHA256Digest(strings.Repeat("b", 64)), EngineID: domain.SHA256Digest(strings.Repeat("c", 64))}
	exec := &storageExecutor{foreignID: strings.Repeat("d", 64)}
	scan, err := scanDockerStorage(t.Context(), docker{exec: exec, binary: "/usr/bin/docker"}, scope)
	if err != nil || !scan.Valid() || len(scan.Objects) != 1 || scan.Objects[0].ID != strings.Repeat("a", 64) || scan.Foreign.Containers != 1 || len(exec.inspectCalls) != 1 {
		t.Fatalf("bounded own-only scan failed: %+v, %v, inspections=%v", scan, err, exec.inspectCalls)
	}
	for _, command := range exec.listCalls {
		if !strings.Contains(command, "--filter label=io.compute-relay.installation") {
			t.Fatalf("Docker label filter missing: %s", command)
		}
	}
	exec.truncate = true
	if _, err := scanDockerStorage(t.Context(), docker{exec: exec, binary: "/usr/bin/docker"}, scope); err == nil {
		t.Fatal("truncated listing was accepted as a complete scan")
	}
}
