package runtimehost

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

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
