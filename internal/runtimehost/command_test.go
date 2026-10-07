package runtimehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/operatorcli"
)

func TestLocalValidationNeedsNoStateOrProviderAndRejectsInvalidFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.json")
	good := []byte(`{"api_version":"compute-connector/v1alpha1","name":"schema only","profile":"not-configured","bundle":{"object_id":"absent"},"execution":{"kind":"python","command":["python","MUST_NOT_RUN.py"]},"inputs":[],"outputs":[{"path":"answer.json","required":true}],"resources":{"accelerator":"cpu"},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":10,"setup_seconds":5,"finalization_grace_seconds":2}}`)
	if err := os.WriteFile(path, good, 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := Command(context.Background(), operatorcli.Request{Command: "validate", File: path}, &output); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Status   string `json:"status"`
		Admitted bool   `json:"admitted"`
		Provider bool   `json:"provider_checked"`
		Digest   string `json:"specification_sha256"`
	}
	if json.Unmarshal(output.Bytes(), &got) != nil || got.Status != "schema-valid-local" || got.Admitted || got.Provider || len(got.Digest) != 64 {
		t.Fatal("schema validation invented admission")
	}
	for _, raw := range [][]byte{nil, []byte(`{}`), append(append([]byte{}, good...), []byte(` {}`)...), bytes.Repeat([]byte{'x'}, (1<<20)+1)} {
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		output.Reset()
		if err := Command(context.Background(), operatorcli.Request{Command: "validate", File: path}, &output); err == nil || output.Len() != 0 {
			t.Fatal("invalid file qualified")
		}
	}
}
func TestInvalidListenerDoesNotOpenOrCreateState(t *testing.T) {
	root := filepath.Join(t.TempDir(), "absent")
	for _, address := range []string{"0.0.0.0:7331", "localhost:7331", "127.0.0.1:01"} {
		var output bytes.Buffer
		if err := Command(context.Background(), operatorcli.Request{Command: "serve", Root: root, Listen: address}, &output); err == nil || output.Len() != 0 {
			t.Fatal("invalid listener accepted")
		}
		if _, err := os.Lstat(root); !os.IsNotExist(err) {
			t.Fatal("invalid command created state")
		}
	}
}

func TestInvalidManagedCommandDoesNotOpenOrCreateState(t *testing.T) {
	python := filepath.Join(t.TempDir(), "synthetic-python3")
	if runtime.GOOS == "windows" {
		python += ".exe"
	}
	if err := os.WriteFile(python, []byte("synthetic executable fixture; must never run"), 0700); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "absent")
	base := operatorcli.Request{Command: "serve", Root: root, Listen: "127.0.0.1:0", ManagedPython: python,
		ManagedMachineShape: "NvidiaTeslaT4", ManagedMaxWallSeconds: 1800, ManagedMaxWorkers: 2}
	for _, mutate := range []func(*operatorcli.Request){
		func(r *operatorcli.Request) { r.ManagedPython = "relative-python" },
		func(r *operatorcli.Request) { r.ManagedPython = "" },
		func(r *operatorcli.Request) { r.ManagedMaxWallSeconds = 0 },
		func(r *operatorcli.Request) { r.ManagedMaxWorkers = 17 },
		func(r *operatorcli.Request) { r.ManagedMachineShape = "T4" },
		func(r *operatorcli.Request) { r.ProviderConfig = "SYNTHETIC_PRIVATE_PATH" },
		func(r *operatorcli.Request) { r.ProviderProfile = "standalone" },
		func(r *operatorcli.Request) { r.AllowGPU = true },
		func(r *operatorcli.Request) { r.AllowPrivateStaging = true },
		func(r *operatorcli.Request) { r.MaxProviderAttempts = 1 },
	} {
		request := base
		mutate(&request)
		var output bytes.Buffer
		if err := Command(context.Background(), request, &output); !errors.Is(err, ErrRequest) || output.Len() != 0 {
			t.Fatal("invalid managed command reached runtime state", err)
		}
		if _, err := os.Lstat(root); !os.IsNotExist(err) {
			t.Fatal("invalid managed command created state")
		}
	}
}

type cancelCommandAnnouncement struct {
	bytes.Buffer
	cancel context.CancelFunc
}

func (w *cancelCommandAnnouncement) Write(raw []byte) (int, error) {
	n, err := w.Buffer.Write(raw)
	w.cancel()
	return n, err
}

func TestManagedCommandComposesPolicyWithoutReadingCredentials(t *testing.T) {
	root := filepath.Join(t.TempDir(), "runtime")
	if _, err := Initialize(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	// This file is intentionally not an interpreter. Empty managed startup must
	// never execute it to discover an account, read a credential or grant compute.
	python := filepath.Join(t.TempDir(), "synthetic-python3")
	if runtime.GOOS == "windows" {
		python += ".exe"
	}
	if err := os.WriteFile(python, []byte("synthetic executable fixture; must never run"), 0700); err != nil {
		t.Fatal(err)
	}
	request, err := operatorcli.Parse([]string{"serve", "--root", root, "--listen", "127.0.0.1:0", "--managed-python", python,
		"--managed-machine-shape", "NvidiaTeslaP100", "--managed-max-wall-seconds", "120", "--managed-max-workers", "1"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output := &cancelCommandAnnouncement{cancel: cancel}
	if err := Command(ctx, request, output); err != nil {
		t.Fatal("managed command did not compose its explicit mode", err)
	}
	var announcement Listening
	if json.Unmarshal(output.Bytes(), &announcement) != nil || announcement.Status != "listening" || announcement.Address == "" {
		t.Fatal("managed command omitted its listening receipt")
	}
	wantMode, wantDispatch := "managed-connections", false
	if runtime.GOOS == "darwin" {
		wantMode, wantDispatch = "managed-workers", true
	}
	if announcement.Mode != wantMode || announcement.DispatchEnabled != wantDispatch {
		t.Fatal("managed CLI policy fell back to another serve mode")
	}
	h, err := Open(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	initialized, err := h.store.FingerprintInitialized(context.Background())
	if err != nil || initialized {
		t.Fatal("startup accessed or provisioned a protected credential identity", err)
	}
}
