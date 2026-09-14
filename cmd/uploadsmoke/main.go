// Command uploadsmoke is a finite offline developer check, NOT a production runtime.
// It uses synthetic workspace tokens and a nondurable metadata fixture, real loopback
// HTTP, and a temporary filesystem store. It never calls a compute provider.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/api"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/objects"
	"github.com/vankhaivn/compute-relay/internal/testsupport"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "upload smoke failed:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	root, err := os.MkdirTemp("", "compute-relay-upload-smoke-")
	if err != nil {
		return errors.New("create private smoke directory")
	}
	defer os.RemoveAll(root)
	limits := blobfs.Limits{MaxObjectBytes: 8 << 20, MaxTotalBytes: 16 << 20, MaxObjects: 16}
	store, err := blobfs.New(filepath.Join(root, "blobs"), limits)
	if err != nil {
		return err
	}
	defer store.Close()
	memory := testsupport.NewMemory(auth.Workspace{ID: "smoke_a", Enabled: true}, auth.Workspace{ID: "smoke_b", Enabled: true})
	access, err := auth.New(memory, memory, nil)
	if err != nil {
		return err
	}
	secret, record, err := access.Issue(ctx, "smoke_a", []auth.Scope{auth.Read, auth.Write}, time.Time{})
	if err != nil {
		return err
	}
	other, _, err := access.Issue(ctx, "smoke_b", []auth.Scope{auth.Read}, time.Time{})
	if err != nil {
		return err
	}
	svc, err := objects.New(access, store, memory)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return errors.New("listen on loopback")
	}
	defer listener.Close()
	cfg := api.DefaultConfig()
	cfg.Listen = listener.Addr().String()
	server, err := api.NewServer(cfg, access, svc, nil)
	if err != nil {
		return err
	}
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = server.Serve(listener) }()
	defer func() { _ = server.Close(); <-stopped }()
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	base := "http://" + listener.Addr().String()
	request := func(method, path, token string, payload io.Reader, want int) ([]byte, http.Header, error) {
		r, err := http.NewRequestWithContext(ctx, method, base+path, payload)
		if err != nil {
			return nil, nil, err
		}
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/octet-stream")
		if method == http.MethodPost {
			r.ContentLength = -1
		} // Exercise unknown-length streaming.
		response, err := client.Do(r)
		if err != nil {
			return nil, nil, errors.New("local HTTP request failed")
		}
		defer response.Body.Close()
		data, err := io.ReadAll(io.LimitReader(response.Body, 8192))
		if err != nil {
			return nil, nil, err
		}
		if response.StatusCode != want {
			return nil, nil, fmt.Errorf("HTTP status %d, want %d", response.StatusCode, want)
		}
		if strings.Contains(string(data), secret.Reveal()) || strings.Contains(string(data), other.Reveal()) {
			return nil, nil, errors.New("synthetic token leaked")
		}
		return data, response.Header, nil
	}
	payload := strings.Repeat("synthetic-bytes!", 280000)
	raw, headers, err := request("POST", "/v1/workspaces/smoke_a/objects", secret.Reveal(), strings.NewReader(payload), 201)
	if err != nil {
		return err
	}
	var meta domain.ObjectMetadata
	if err = json.Unmarshal(raw, &meta); err != nil || !meta.Valid() {
		return errors.New("invalid upload receipt")
	}
	sum := sha256.Sum256([]byte(payload))
	if meta.Bytes != int64(len(payload)) || string(meta.SHA256) != hex.EncodeToString(sum[:]) {
		return errors.New("receipt digest or length mismatch")
	}
	if _, _, err = request("GET", headers.Get("Location"), secret.Reveal(), nil, 200); err != nil {
		return err
	}
	if _, _, err = request("GET", headers.Get("Location"), other.Reveal(), nil, 403); err != nil {
		return err
	}
	if _, _, err = request("GET", "/v1/workspaces/smoke_b/objects/"+string(meta.ID), other.Reveal(), nil, 404); err != nil {
		return err
	}
	if err = access.Revoke(ctx, record.ID); err != nil {
		return err
	}
	if _, _, err = request("GET", headers.Get("Location"), secret.Reveal(), nil, 401); err != nil {
		return err
	}
	if err = store.Close(); err != nil {
		return err
	}
	reopened, err := blobfs.New(filepath.Join(root, "blobs"), limits)
	if err != nil {
		return err
	}
	defer reopened.Close()
	content, err := reopened.Open(ctx, "smoke_a", meta.ID)
	if err != nil {
		return err
	}
	defer content.Close()
	hash := sha256.New()
	n, err := io.Copy(hash, content)
	if err != nil || n != meta.Bytes || hex.EncodeToString(hash.Sum(nil)) != string(meta.SHA256) {
		return errors.New("restart changed blob identity")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "passed-offline", "bytes_verified": n, "workspace_isolation": true, "revocation": true, "blob_restart": true, "metadata_store": "nondurable-test-fixture", "provider_calls": 0, "gpu_executions": 0})
}
