package credentials

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

type keychainRequest struct {
	Operation    string `json:"operation"`
	Installation string `json:"installation"`
	Key          string `json:"key"`
	Secret       []byte `json:"secret"`
}
type keychainResponse struct {
	Status string `json:"status"`
	Secret []byte `json:"secret"`
}

// This native test-binary child stands in for the fixed Python helper. It consumes
// only a synthetic scenario name and never opens the platform credential store.
func TestMain(m *testing.M) {
	if len(os.Args) == 6 && os.Args[1] == "-I" && os.Args[2] == "-X" && os.Args[3] == "utf8" && os.Args[4] == "-c" && os.Args[5] == keychainSource {
		raw, err := io.ReadAll(io.LimitReader(os.Stdin, 64))
		if err != nil {
			os.Exit(2)
		}
		switch string(raw) {
		case "failed":
			_, _ = os.Stderr.WriteString("synthetic-diagnostic-canary")
			os.Exit(1)
		case "stdout_overflow", "stderr_overflow":
			out := os.Stdout
			if string(raw) == "stderr_overflow" {
				out = os.Stderr
			}
			block := bytes.Repeat([]byte{'x'}, 4096)
			for {
				if _, err := out.Write(block); err != nil {
					os.Exit(1)
				}
			}
		case "cancelled":
			for {
				time.Sleep(time.Minute)
			}
		default:
			os.Exit(2)
		}
	}
	os.Exit(m.Run())
}

func TestKeychainWriteOnlyProtocolAndClearedBuffers(t *testing.T) {
	var requestBytes, responseBytes, callbackBytes []byte
	k := &Keychain{installation: "rt_fixture"}
	k.run = func(ctx context.Context, raw []byte) ([]byte, error) {
		requestBytes = raw
		var request keychainRequest
		if json.Unmarshal(raw, &request) != nil || request.Installation != "rt_fixture" || request.Key != "generation_1" {
			t.Fatal("invalid scoped request")
		}
		response := keychainResponse{Status: "ok"}
		if request.Operation == "create" && string(request.Secret) != "fixture-secret-canary" {
			t.Fatal("create did not forward exact bytes")
		}
		if request.Operation == "read" {
			response.Secret = []byte("fixture-secret-canary")
		}
		responseBytes, _ = json.Marshal(response)
		return responseBytes, nil
	}
	if err := k.Create(context.Background(), "generation_1", []byte("fixture-secret-canary")); err != nil {
		t.Fatal(err)
	}
	if !zeroed(requestBytes) || !zeroed(responseBytes) {
		t.Fatal("serialized credential survived create")
	}
	sentinel := errors.New("callback failed")
	if err := k.WithSecret(context.Background(), "generation_1", func(b []byte) error { callbackBytes = b; return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal("callback error lost")
	}
	if !zeroed(callbackBytes) || !zeroed(requestBytes) || !zeroed(responseBytes) {
		t.Fatal("owned credential buffer survived failed callback")
	}
	func() {
		defer func() {
			if recover() != sentinel {
				t.Fatal("callback panic lost")
			}
		}()
		_ = k.WithSecret(context.Background(), "generation_1", func(b []byte) error { callbackBytes = b; panic(sentinel) })
	}()
	if !zeroed(callbackBytes) {
		t.Fatal("owned buffer survived panic")
	}
}
func zeroed(data []byte) bool { return len(data) > 0 && bytes.Equal(data, make([]byte, len(data))) }

func TestKeychainDenialMalformedAndBoundedOutput(t *testing.T) {
	for _, tc := range []struct {
		status string
		want   error
	}{{"unavailable", ErrUnavailable}, {"missing", ErrUnconfigured}, {"conflict", ErrConflict}, {"unsupported", ErrUnsupported}} {
		t.Run(tc.status, func(t *testing.T) {
			k := &Keychain{installation: "rt_fixture", run: func(context.Context, []byte) ([]byte, error) {
				return json.Marshal(keychainResponse{Status: tc.status})
			}}
			if err := k.WithSecret(context.Background(), "item", func([]byte) error { t.Fatal("failed vault called consumer"); return nil }); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
		})
	}
	for _, raw := range []string{
		`{"status":"ok","secret":null}`,
		`{"status":"ok","secret":"ZmFrZQ==","extra":"fixture-canary"}`,
		`{"status":"ok","secret":"invalid-base64"}`,
		`{"status":"ok","secret":"ZmFrZQ==","secret":"b3RoZXI="}`,
		`{"status":"unavailable","status":"ok","secret":"ZmFrZQ=="}`,
		`{"Status":"ok","secret":"ZmFrZQ=="}`,
		`{"status":"ok","Secret":"ZmFrZQ=="}`,
		`{"status":"ok","secret":"ZmFrZQ=="}{"status":"ok","secret":null}`,
		`{"status":"ok","secret":"ZmFrZQ=="} true`,
		`{"status":"ok","secret":"ZmFrZR=="}`,
		"{\"status\":\"ok\",\"secret\":\"ZmFr\nZQ==\"}",
		strings.Repeat("x", 2*MaxVaultBytes+1),
	} {
		k := &Keychain{installation: "rt_fixture", run: func(context.Context, []byte) ([]byte, error) { return []byte(raw), nil }}
		if err := k.WithSecret(context.Background(), "item", func([]byte) error { t.Fatal("malformed output reached consumer"); return nil }); !errors.Is(err, ErrUnavailable) {
			t.Fatal("unsafe helper response accepted")
		}
	}
	cancelled := false
	out := &vaultOutput{cancel: func() { cancelled = true }}
	if _, err := out.Write(make([]byte, 2*MaxVaultBytes+1)); err == nil || !out.failed || !cancelled || len(out.data) != 0 {
		t.Fatal("output limit failed")
	}
	k := &Keychain{installation: "rt_fixture", run: func(context.Context, []byte) ([]byte, error) {
		t.Fatal("invalid request invoked helper")
		return nil, nil
	}}
	for _, key := range []string{"", "../item", "item\n", "item/key"} {
		if err := k.Create(context.Background(), key, []byte("fixture")); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid key accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := k.Delete(ctx, "item"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestKeychainCancellationAndRunFailureClearBuffers(t *testing.T) {
	for _, mode := range []string{"cancel", "failure", "denied", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var input, output []byte
			want := ErrUnavailable
			k := &Keychain{installation: "rt_fixture", run: func(_ context.Context, request []byte) ([]byte, error) {
				input = request
				output, _ = json.Marshal(keychainResponse{Status: "ok", Secret: []byte("synthetic-canary")})
				switch mode {
				case "cancel":
					cancel()
					want = context.Canceled
				case "failure":
					return output, ErrUnavailable
				case "denied":
					copy(output, `{"status":"unavailable"`)
				case "malformed":
					output[len(output)-1] = '!'
				}
				return output, nil
			}}
			if err := k.WithSecret(ctx, "item", func([]byte) error { t.Fatal("failed request called consumer"); return nil }); !errors.Is(err, want) {
				t.Fatal("unexpected error", err)
			}
			if !zeroed(input) || !zeroed(output) {
				t.Fatal("owned buffers survived helper failure")
			}
		})
	}
}

func TestKeychainProtocolBinaryBoundsAndNonReadSecrets(t *testing.T) {
	for _, size := range []int{1, MaxVaultBytes} {
		secret := bytes.Repeat([]byte{0xff}, size)
		k := &Keychain{installation: "rt_fixture", run: func(_ context.Context, input []byte) ([]byte, error) {
			var request keychainRequest
			if json.Unmarshal(input, &request) != nil {
				t.Fatal("invalid helper request")
			}
			defer clear(request.Secret)
			if request.Operation == "create" {
				if !bytes.Equal(request.Secret, secret) {
					t.Fatal("binary input changed")
				}
				return json.Marshal(keychainResponse{Status: "ok"})
			}
			return json.Marshal(keychainResponse{Status: "ok", Secret: secret})
		}}
		if err := k.Create(context.Background(), "item", secret); err != nil {
			t.Fatal(err)
		}
		if err := k.WithSecret(context.Background(), "item", func(value []byte) error {
			if !bytes.Equal(value, secret) {
				t.Fatal("binary output changed")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if secret[0] != 0xff {
			t.Fatal("caller-owned input was cleared")
		}
	}
	for _, status := range []string{"ok", "missing", "conflict", "unsupported", "unavailable"} {
		k := &Keychain{installation: "rt_fixture", run: func(context.Context, []byte) ([]byte, error) {
			return json.Marshal(keychainResponse{Status: status, Secret: []byte("synthetic-canary")})
		}}
		if err := k.Create(context.Background(), "item", []byte("synthetic-canary")); !errors.Is(err, ErrUnavailable) {
			t.Fatal("secret-bearing non-read response accepted", err)
		}
		if status != "ok" {
			if err := k.WithSecret(context.Background(), "item", func([]byte) error { t.Fatal("denial called consumer"); return nil }); !errors.Is(err, ErrUnavailable) {
				t.Fatal("secret-bearing denial response accepted", err)
			}
		}
	}
	k := &Keychain{installation: "rt_fixture", run: func(context.Context, []byte) ([]byte, error) {
		return json.Marshal(keychainResponse{Status: "ok", Secret: make([]byte, MaxVaultBytes+1)})
	}}
	if err := k.WithSecret(context.Background(), "item", func([]byte) error { t.Fatal("oversized secret reached consumer"); return nil }); !errors.Is(err, ErrUnavailable) {
		t.Fatal("oversized helper secret accepted", err)
	}
}

func TestKeychainOutputNeverAbandonsOwnedBuffers(t *testing.T) {
	for _, discard := range []bool{false, true} {
		cancelled := false
		out := &vaultOutput{cancel: func() { cancelled = true }, discard: discard}
		if _, err := out.Write([]byte("synthetic-canary")); err != nil {
			t.Fatal(err)
		}
		first := out.data
		if _, err := out.Write(make([]byte, 2*MaxVaultBytes-out.size)); err != nil {
			t.Fatal("valid output was truncated", err)
		}
		if !discard && &first[0] != &out.data[0] {
			t.Fatal("growth abandoned an uncleared credential buffer")
		}
		if _, err := out.Write([]byte("x")); err == nil || !out.failed || !cancelled {
			t.Fatal("overflow failed to cancel process")
		}
		clear(out.data)
		if !discard && !zeroed(first) || discard && len(out.data) != 0 {
			t.Fatal("output buffer was retained")
		}
	}
}

func TestKeychainProcessFailuresAreBoundedAndSanitized(t *testing.T) {
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		want    error
	}{
		{"failed", 5 * time.Second, ErrUnavailable},
		{"stdout_overflow", 5 * time.Second, context.Canceled},
		{"stderr_overflow", 5 * time.Second, context.Canceled},
		{"cancelled", 500 * time.Millisecond, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := &Keychain{python: path}
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			output, err := k.invoke(ctx, []byte(tc.name))
			if err == nil || len(output) != 0 {
				t.Fatal("failed helper returned output")
			}
			if !errors.Is(err, tc.want) {
				t.Fatal("process failure was not bounded and sanitized", err)
			}
		})
	}
}

// Explicit local qualification; ordinary CI must never touch a native credential store.
// The random installation namespace and exact deletion target contain synthetic bytes.
func TestNativeKeychainCreateReplayRestartAndRemove(t *testing.T) {
	if os.Getenv("COMPUTE_RELAY_TEST_KEYCHAIN") != "1" {
		t.Skip("explicit native Keychain qualification only")
	}
	if runtime.GOOS != "darwin" {
		t.Fatal("native qualification requires macOS")
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("Python interpreter unavailable")
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	installation := "test_" + hex.EncodeToString(nonce[:])
	k, err := NewKeychain(installation, python)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := "fixture_credential"
	// Exercise full-sized binary values, including NUL bytes, across the pipe and CFData boundary.
	synthetic := bytes.Repeat([]byte{0, 0xff, 'x', 'y'}, MaxVaultBytes/4)
	defer clear(synthetic)
	defer func() {
		if err := k.Delete(ctx, key); err != nil {
			t.Error("exact test-entry cleanup failed", err)
		}
	}()
	if err = k.Create(ctx, key, synthetic); err != nil {
		t.Fatal("native create unavailable", err)
	}
	if err = k.Create(ctx, key, synthetic); err != nil {
		t.Fatal("same-content replay failed", err)
	}
	if err = k.Create(ctx, key, []byte("different-fixture")); !errors.Is(err, ErrConflict) {
		t.Fatal("create overwrote existing entry")
	}
	restarted, err := NewKeychain(installation, python)
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.WithSecret(ctx, key, func(b []byte) error {
		if !bytes.Equal(b, synthetic) {
			t.Fatal("restart lost immutable value")
		}
		return nil
	}); err != nil {
		t.Fatal("native read unavailable", err)
	}
	if err = restarted.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err = restarted.WithSecret(ctx, key, func([]byte) error { t.Fatal("removed entry returned"); return nil }); !errors.Is(err, ErrUnconfigured) {
		t.Fatal("missing entry not distinguished", err)
	}
}
