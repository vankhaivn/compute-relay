package credentials

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/base64"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

//go:embed keychain.py
var keychainSource string

// Keychain uses the system Security framework through a fixed standard-library-only
// helper in the explicitly installed Python interpreter. Secrets travel over anonymous
// pipes, never arguments, environment, temporary files or diagnostic output. No Python
// packages are needed. Constructing the adapter does not open the keychain.
type Keychain struct {
	installation string
	python       string
	run          func(context.Context, []byte) ([]byte, error)
}

func NewKeychain(installation, python string) (*Keychain, error) {
	if runtime.GOOS != "darwin" {
		return nil, ErrUnsupported
	}
	if !vaultKey.MatchString(installation) || !filepath.IsAbs(python) || strings.ContainsAny(python, "\x00\r\n") {
		return nil, ErrInvalid
	}
	info, err := os.Stat(python)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return nil, ErrUnavailable
	}
	k := &Keychain{installation: installation, python: python}
	k.run = k.invoke
	return k, nil
}

func (k *Keychain) Create(ctx context.Context, key string, secret []byte) error {
	if len(secret) == 0 || len(secret) > MaxVaultBytes {
		return ErrInvalid
	}
	_, err := k.call(ctx, "create", key, secret)
	return err
}
func (k *Keychain) Delete(ctx context.Context, key string) error {
	_, err := k.call(ctx, "delete", key, nil)
	return err
}
func (k *Keychain) WithSecret(ctx context.Context, key string, use func([]byte) error) error {
	if use == nil {
		return ErrInvalid
	}
	secret, err := k.call(ctx, "read", key, nil)
	if err != nil {
		return err
	}
	defer clear(secret)
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = use(secret); err != nil {
		return err
	}
	return ctx.Err()
}
func (k *Keychain) call(ctx context.Context, operation, key string, secret []byte) ([]byte, error) {
	if k == nil || k.run == nil || !vaultKey.MatchString(k.installation) || !vaultKey.MatchString(key) {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if operation != "create" && operation != "read" && operation != "delete" {
		return nil, ErrInvalid
	}
	input := keychainInput(operation, k.installation, key, secret)
	defer clear(input)
	raw, err := k.run(ctx, input)
	defer clear(raw)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	// Helper output is an internal fixed protocol, bounded independently of its process.
	if len(raw) > 2*MaxVaultBytes {
		return nil, ErrUnavailable
	}
	return keychainResult(operation, raw)
}

// The fixed ASCII protocol avoids JSON encoder pools and decoder scratch/string
// copies of secrets that cannot be cleared. IDs are validated before encoding.
func keychainInput(operation, installation, key string, secret []byte) []byte {
	size := len(operation) + len(installation) + len(key) + base64.StdEncoding.EncodedLen(len(secret)) + 64
	input := make([]byte, 0, size)
	input = append(input, `{"operation":"`...)
	input = append(input, operation...)
	input = append(input, `","installation":"`...)
	input = append(input, installation...)
	input = append(input, `","key":"`...)
	input = append(input, key...)
	input = append(input, `","secret":`...)
	if secret == nil {
		input = append(input, "null}"...)
	} else {
		input = append(input, '"')
		input = base64.StdEncoding.AppendEncode(input, secret)
		input = append(input, '"', '}')
	}
	return input
}

func keychainResult(operation string, raw []byte) ([]byte, error) {
	switch {
	case bytes.Equal(raw, []byte(`{"status":"missing","secret":null}`)):
		return nil, ErrUnconfigured
	case bytes.Equal(raw, []byte(`{"status":"conflict","secret":null}`)):
		return nil, ErrConflict
	case bytes.Equal(raw, []byte(`{"status":"unsupported","secret":null}`)):
		return nil, ErrUnsupported
	}
	encoded, ok := bytes.CutPrefix(raw, []byte(`{"status":"ok","secret":`))
	if !ok {
		return nil, ErrUnavailable
	}
	if operation != "read" {
		if bytes.Equal(encoded, []byte("null}")) {
			return nil, nil
		}
		return nil, ErrUnavailable
	}
	encoded, ok = bytes.CutPrefix(encoded, []byte{'"'})
	if !ok {
		return nil, ErrUnavailable
	}
	encoded, ok = bytes.CutSuffix(encoded, []byte(`"}`))
	if !ok || len(encoded) == 0 || len(encoded) > base64.StdEncoding.EncodedLen(MaxVaultBytes) || bytes.ContainsAny(encoded, "\r\n") {
		return nil, ErrUnavailable
	}
	secret := make([]byte, base64.StdEncoding.DecodedLen(len(encoded)))
	n, err := base64.StdEncoding.Strict().Decode(secret, encoded)
	if err != nil || n == 0 || n > MaxVaultBytes {
		clear(secret)
		return nil, ErrUnavailable
	}
	return secret[:n], nil
}

// The embedded helper never launches children. Kill bounds the single process;
// WaitDelay additionally prevents unbounded pipe waiting on an unexpected exit.
func (k *Keychain) invoke(parent context.Context, input []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, k.python, "-I", "-X", "utf8", "-c", keychainSource)
	cmd.Env = []string{"LANG=C.UTF-8"}
	cmd.Stdin = bytes.NewReader(input)
	out := &vaultOutput{cancel: cancel}
	diagnostic := &vaultOutput{cancel: cancel, discard: true}
	cmd.Stdout = out
	cmd.Stderr = diagnostic
	cmd.WaitDelay = time.Second
	err := cmd.Run()
	if err != nil || out.failed || diagnostic.failed {
		clear(out.data)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrUnavailable
	}
	return out.data, nil
}

type vaultOutput struct {
	data            []byte
	size            int
	failed, discard bool
	cancel          context.CancelFunc
}

func (w *vaultOutput) Write(p []byte) (int, error) {
	if len(p) > 2*MaxVaultBytes-w.size {
		w.failed = true
		w.cancel()
		return 0, io.ErrShortWrite
	}
	w.size += len(p)
	if !w.discard {
		if w.data == nil {
			// Allocate once so append never abandons an unwiped old secret buffer.
			w.data = make([]byte, 0, 2*MaxVaultBytes)
		}
		w.data = append(w.data, p...)
	}
	return len(p), nil
}
