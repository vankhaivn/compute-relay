package sshhost

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	archiveOpen  bool
	summaryReply string
	summaryWait  bool
	summaryExit  int
	volumes      []string // cache volumes of this installation
	dropVolume   string   // a volume inspect leaves unanswered
	volumeCalls  int
}

func (e *storageExecutor) Run(ctx context.Context, _ map[string]string, argv []string, stdin io.Reader, stdout io.Writer) (Result, error) {
	command := strings.Join(argv[1:], " ")
	var response string
	labels := e.labels
	if labels == nil {
		labels = map[string]string{storageLabel: "installation_1", labelPrefix + "engine": "NATIVEENGINE1234567890", labelPrefix + "attempt": "attempt_1", labelPrefix + "workspace": "workspace_1"}
	}
	encoded, err := json.Marshal(labels)
	if err != nil {
		return Result{}, err
	}
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
		} else if strings.HasPrefix(command, "volume ls") {
			response = strings.Join(e.volumes, "\n")
		}
	case strings.HasPrefix(command, "volume inspect "):
		e.volumeCalls++
		entries := []string{}
		for _, name := range argv[3:] {
			if name == e.dropVolume {
				continue
			}
			entries = append(entries, fmt.Sprintf(`{"Name":"%s","Labels":{"%s":"installation_1","%skind":"cache","%sbytes":"10","%smachine":"%s","%sengine":"%s"}}`,
				name, storageLabel, labelPrefix, labelPrefix, labelPrefix, strings.Repeat("b", 64), labelPrefix, strings.Repeat("c", 64)))
		}
		response = "[" + strings.Join(entries, ",") + "]"
	case strings.HasPrefix(command, "container inspect --size"):
		if e.archiveOpen {
			<-ctx.Done()
			return Result{}, ctx.Err()
		}
		e.inspectCalls = append(e.inspectCalls, command)
		if strings.Contains(command, e.foreignID) {
			return Result{ExitCode: 1}, nil
		}
		response = fmt.Sprintf(`[{"Id":"%s","SizeRw":128,"State":{"Running":false},"Config":{"Labels":%s},"Mounts":[]}]`, strings.Repeat("a", 64), encoded)
	case command == "system dial-stdio":
		if e.summaryWait {
			<-ctx.Done()
			return Result{}, ctx.Err()
		}
		request, err := http.ReadRequest(bufio.NewReader(stdin))
		if err != nil {
			return Result{}, err
		}
		query := request.URL.Query()
		if request.Method != http.MethodGet || request.URL.Path != "/containers/json" || !request.Close || query.Get("all") != "1" || query.Get("size") != "1" || query.Get("filters") != `{"label":["io.compute-relay.installation=installation_1"]}` {
			return Result{ExitCode: 1}, nil
		}
		response = e.summaryReply
		if response == "" {
			body := fmt.Sprintf(`[{"Id":"%s","SizeRw":128,"State":"exited","Labels":%s,"Mounts":[]}]`, strings.Repeat("a", 64), encoded)
			response = storageHTTPReply(body)
		}
	default:
		return Result{ExitCode: 1}, nil
	}
	_, err = io.WriteString(stdout, response)
	if command == "system dial-stdio" {
		return Result{ExitCode: e.summaryExit}, err
	}
	return Result{}, err
}

func storageHTTPReply(body string) string {
	return fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
}

func TestStorageScanDoesNotWaitForContainerArchive(t *testing.T) {
	scope := hoststorage.Scope{InstallationID: "installation_1", MachineID: domain.SHA256Digest(strings.Repeat("b", 64)), EngineID: domain.SHA256Digest(strings.Repeat("c", 64))}
	exec := &storageExecutor{foreignID: strings.Repeat("f", 64), archiveOpen: true}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	scan, err := scanDockerStorage(ctx, docker{exec: exec, binary: "/usr/bin/docker"}, scope)
	if err != nil || len(scan.Objects) != 1 || !scan.Objects[0].SizeKnown || scan.Objects[0].SizeBytes != 128 {
		t.Fatalf("archive transfer blocked storage observation: %+v, %v", scan, err)
	}
}

func TestStorageContainerSummaryRejectsIncompleteEvidence(t *testing.T) {
	scope := hoststorage.Scope{InstallationID: "installation_1", MachineID: domain.SHA256Digest(strings.Repeat("b", 64)), EngineID: domain.SHA256Digest(strings.Repeat("c", 64))}
	object := fmt.Sprintf(`{"Id":"%s","SizeRw":128,"State":"exited","Labels":{"io.compute-relay.installation":"installation_1","io.compute-relay.engine":"NATIVEENGINE1234567890"},"Mounts":[]}`, strings.Repeat("a", 64))
	body := "[" + object + "]"
	valid := storageHTTPReply(body)
	for _, test := range []struct {
		name  string
		reply string
		exit  int
	}{
		{"not an HTTP response", body, 0},
		{"failed request", strings.Replace(valid, "200 OK", "500 Internal Server Error", 1), 0},
		{"wrong content type", strings.Replace(valid, "application/json", "text/html", 1), 0},
		{"encoded body", strings.Replace(valid, "Content-Type:", "Content-Encoding: gzip\r\nContent-Type:", 1), 0},
		{"truncated body", valid[:len(valid)-1], 0},
		{"extra response", valid + valid, 0},
		{"failed command", valid, 1},
		{"unknown membership", storageHTTPReply("null"), 0},
		{"duplicate container", storageHTTPReply("[" + object + "," + object + "]"), 0},
		{"unlisted container", storageHTTPReply(strings.Replace(body, strings.Repeat("a", 64), strings.Repeat("b", 64), 1)), 0},
		{"foreign installation", storageHTTPReply(strings.Replace(body, "installation_1", "installation_2", 1)), 0},
		{"unknown state", storageHTTPReply(strings.Replace(body, `"exited"`, `"future-state"`, 1)), 0},
		{"too many containers", storageHTTPReply("[" + strings.Repeat(object+",", hoststorage.ScanMaxObjects) + object + "]"), 0},
		{"oversized response", storageHTTPReply(strings.Repeat(" ", hoststorage.ScanMaxBytes) + body), 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := &storageExecutor{foreignID: strings.Repeat("f", 64), summaryReply: test.reply, summaryExit: test.exit}
			if scan, err := scanDockerStorage(t.Context(), docker{exec: exec, binary: "/usr/bin/docker"}, scope); err == nil || scan.Valid() {
				t.Fatalf("incomplete evidence established a complete scan: %+v, %v", scan, err)
			}
		})
	}
	for _, test := range []struct{ name, body string }{
		{"missing size", strings.Replace(body, `"SizeRw":128,`, "", 1)},
		{"failed size calculation", strings.Replace(body, `"SizeRw":128`, `"SizeRw":-1`, 1)},
		{"wrong engine", strings.Replace(body, "NATIVEENGINE1234567890", "OTHERENGINE1234567890", 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec := &storageExecutor{foreignID: strings.Repeat("f", 64), summaryReply: storageHTTPReply(test.body)}
			scan, err := scanDockerStorage(t.Context(), docker{exec: exec, binary: "/usr/bin/docker"}, scope)
			if err != nil || len(scan.Objects) != 1 || scan.Objects[0].SizeKnown {
				t.Fatalf("unknown size became usable accounting: %+v, %v", scan, err)
			}
		})
	}

	t.Run("chunked response", func(t *testing.T) {
		reply := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n%x\r\n%s\r\n0\r\n\r\n", len(body), body)
		exec := &storageExecutor{foreignID: strings.Repeat("f", 64), summaryReply: reply}
		scan, err := scanDockerStorage(t.Context(), docker{exec: exec, binary: "/usr/bin/docker"}, scope)
		if err != nil || len(scan.Objects) != 1 || !scan.Objects[0].SizeKnown || scan.Objects[0].SizeBytes != 128 {
			t.Fatalf("complete chunked summary rejected: %+v, %v", scan, err)
		}
	})
	t.Run("cancelled observation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		exec := &storageExecutor{foreignID: strings.Repeat("f", 64), summaryWait: true}
		if scan, err := scanDockerStorage(ctx, docker{exec: exec, binary: "/usr/bin/docker"}, scope); err == nil || scan.Valid() {
			t.Fatalf("cancelled observation established a complete scan: %+v, %v", scan, err)
		}
	})
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
	if err != nil || !scan.Valid() || len(scan.Objects) != 1 || scan.Objects[0].ID != strings.Repeat("a", 64) || scan.Foreign.Containers != 1 || len(exec.inspectCalls) != 0 {
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

func TestStorageScanInspectsManyVolumesInFewCommands(t *testing.T) {
	scope := hoststorage.Scope{InstallationID: "installation_1", MachineID: domain.SHA256Digest(strings.Repeat("b", 64)), EngineID: domain.SHA256Digest(strings.Repeat("c", 64))}
	exec := &storageExecutor{foreignID: strings.Repeat("d", 64)}
	for i := range 300 {
		exec.volumes = append(exec.volumes, fmt.Sprintf("relay-cache-%032x", i))
	}
	scan, err := scanDockerStorage(t.Context(), docker{exec: exec, binary: "/usr/bin/docker"}, scope)
	if err != nil || !scan.Valid() || len(scan.Objects) != 301 || exec.volumeCalls != 5 {
		t.Fatalf("volume inspection did not batch: objects=%d calls=%d err=%v", len(scan.Objects), exec.volumeCalls, err)
	}
	for _, o := range scan.Objects[1:] {
		if o.Kind != hoststorage.Volume || !o.SizeKnown || o.SizeBytes != 10 {
			t.Fatalf("batched volume lost its accounting: %+v", o)
		}
	}
	exec.dropVolume, exec.volumeCalls = exec.volumes[100], 0
	if _, err := scanDockerStorage(t.Context(), docker{exec: exec, binary: "/usr/bin/docker"}, scope); err == nil {
		t.Fatal("a volume missing from a batched answer was accepted")
	}
}

func TestStorageScanReasonNamesTheFailedStep(t *testing.T) {
	scope := hoststorage.Scope{InstallationID: "installation_1", MachineID: domain.SHA256Digest(strings.Repeat("b", 64)), EngineID: domain.SHA256Digest(strings.Repeat("c", 64))}
	exec := &storageExecutor{foreignID: strings.Repeat("d", 64), summaryWait: true}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err := scanDockerStorage(ctx, docker{exec: exec, binary: "/usr/bin/docker"}, scope)
	if got := StorageScanReason(err); got != "storage_container_summary_failed" {
		t.Fatalf("a scan over its deadline was reported as %q (%v)", got, err)
	}
	for err, want := range map[error]string{ErrRemoteDocker: "remote_docker_failed", errResponseLimit: "remote_response_limit",
		fmt.Errorf("dial: %w", ErrUnavailable): "host_unavailable", io.EOF: "remote_observation_failed",
		failure(ErrRemoteDocker, "storage_inspect_invalid"): "storage_inspect_invalid"} {
		if got := StorageScanReason(err); got != want {
			t.Fatalf("%v reported as %q", err, got)
		}
	}
}
