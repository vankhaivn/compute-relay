package appcli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestUploadSourceGrowthAfterServerConsumptionInvalidatesReceipt(t *testing.T) {
	root, tokenFile := syntheticFiles(t)
	path := filepath.Join(root, "input")
	original := []byte("original immutable source")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(original)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		raw, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(raw, original) {
			t.Error("wrong request source", err)
		}
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Error(err)
			return
		}
		_, writeErr := f.Write([]byte("changed after consumption"))
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			t.Error("source mutation fixture failed")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = fmt.Fprintf(w, `{"object_id":"obj_original","workspace_id":"app","bytes":%d,"sha256":%q}`, len(original), hex.EncodeToString(sum[:]))
	}))
	defer server.Close()
	var out, diagnostic bytes.Buffer
	args := []string{"object", "upload", "--workspace", "app", "--token-file", tokenFile, "--file", path, "--url", server.URL}
	if Run(context.Background(), args, &out, &diagnostic) != 1 || out.Len() != 0 || calls.Load() != 1 || !strings.Contains(diagnostic.String(), `"stage":"source_acknowledgement"`) || !strings.Contains(diagnostic.String(), `"request_may_have_committed":true`) {
		t.Fatal("changed source qualified or was replayed", diagnostic.String(), calls.Load())
	}
}
