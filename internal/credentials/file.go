package credentials

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/vankhaivn/compute-relay/internal/safefs"
	"github.com/vankhaivn/compute-relay/internal/statefs"
)

const fileVaultName = "secrets.env"
const maxFileVaultBytes = 8 << 20

// File stores binary-safe, unencrypted entries in a private installation directory.
// The caller must hold the installation's exclusive statefs lock for its lifetime and
// compose one File instance. The file is data, never shell input or process environment.
type File struct {
	mu           sync.Mutex
	directory    string
	installation string
	// The publication seam also permits exercising a failed commit without damaging a file.
	publish func(string, string) error
}

var _ Vault = (*File)(nil)

// NewFile creates or checks the private directory without reading any credential.
func NewFile(installation, directory string) (*File, error) {
	if !vaultKey.MatchString(installation) || !filepath.IsAbs(directory) {
		return nil, ErrInvalid
	}
	directory, err := statefs.PrivateDir(directory, false)
	if err != nil {
		return nil, ErrUnavailable
	}
	return &File{directory: directory, installation: installation, publish: os.Rename}, nil
}

func (f *File) Create(ctx context.Context, key string, secret []byte) error {
	if len(secret) == 0 || len(secret) > MaxVaultBytes {
		return ErrInvalid
	}
	return f.change(ctx, key, func(entries map[string][]byte) (bool, error) {
		if prior, exists := entries[key]; exists {
			if bytes.Equal(prior, secret) {
				return false, nil
			}
			return false, ErrConflict
		}
		entries[key] = bytes.Clone(secret)
		return true, nil
	})
}

func (f *File) Delete(ctx context.Context, key string) error {
	return f.change(ctx, key, func(entries map[string][]byte) (bool, error) {
		prior, exists := entries[key]
		clear(prior)
		delete(entries, key)
		return exists, nil
	})
}

func (f *File) WithSecret(ctx context.Context, key string, use func([]byte) error) error {
	if use == nil || f == nil || !vaultKey.MatchString(key) {
		return ErrInvalid
	}
	f.mu.Lock()
	entries, err := f.read(ctx)
	f.mu.Unlock()
	defer clearEntries(entries)
	if err != nil {
		return err
	}
	secret, exists := entries[key]
	if !exists {
		return ErrUnconfigured
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := use(secret); err != nil {
		return err
	}
	return ctx.Err()
}

func (f *File) change(ctx context.Context, key string, update func(map[string][]byte) (bool, error)) error {
	if f == nil || !vaultKey.MatchString(key) {
		return ErrInvalid
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	entries, err := f.read(ctx)
	defer clearEntries(entries)
	if err != nil {
		return err
	}
	changed, err := update(entries)
	if err != nil {
		return err
	}
	if !changed {
		return ctx.Err()
	}
	return f.write(ctx, entries)
}

func clearEntries(entries map[string][]byte) {
	for _, secret := range entries {
		clear(secret)
	}
}

func (f *File) read(ctx context.Context) (map[string][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if statefs.CheckDir(f.directory) != nil {
		return nil, ErrUnavailable
	}
	path := filepath.Join(f.directory, fileVaultName)
	if err := statefs.CheckFile(path); errors.Is(err, os.ErrNotExist) {
		return make(map[string][]byte), nil
	} else if err != nil {
		return nil, ErrUnavailable
	}
	root, err := safefs.OpenRoot(f.directory)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer root.Close()
	file, err := root.Open(fileVaultName)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil || before.Size() > maxFileVaultBytes {
		return nil, ErrUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxFileVaultBytes+1))
	defer clear(raw)
	after, statErr := file.Stat()
	if err != nil || statErr != nil || !safefs.Unchanged(before, after) || int64(len(raw)) != before.Size() || len(raw) > maxFileVaultBytes {
		return nil, ErrUnavailable
	}
	return f.decode(raw)
}

func (f *File) header() []byte {
	return []byte("COMPUTE_RELAY_VAULT=1\nINSTALLATION_ID=" + f.installation + "\n")
}

func (f *File) decode(raw []byte) (map[string][]byte, error) {
	if !bytes.HasPrefix(raw, f.header()) || len(raw) == 0 || raw[len(raw)-1] != '\n' {
		return nil, ErrUnavailable
	}
	entries := make(map[string][]byte)
	valid := false
	defer func() {
		if !valid {
			clearEntries(entries)
		}
	}()
	remaining := raw[len(f.header()):]
	for len(remaining) != 0 {
		line, rest, found := bytes.Cut(remaining, []byte{'\n'})
		if !found || len(entries) >= 4096 {
			return nil, ErrUnavailable
		}
		remaining = rest
		name, value, found := bytes.Cut(line, []byte{'='})
		if !found || !bytes.HasPrefix(name, []byte("SECRET_")) || len(name) > 7+256 || len(value) > base64.StdEncoding.EncodedLen(MaxVaultBytes) {
			return nil, ErrUnavailable
		}
		keyBytes := make([]byte, hex.DecodedLen(len(name)-7))
		n, err := hex.Decode(keyBytes, name[7:])
		key := string(keyBytes[:n])
		if err != nil || !vaultKey.MatchString(key) || entries[key] != nil || !bytes.Equal(name[7:], []byte(hex.EncodeToString(keyBytes))) {
			return nil, ErrUnavailable
		}
		secret := make([]byte, base64.StdEncoding.DecodedLen(len(value)))
		n, err = base64.StdEncoding.Strict().Decode(secret, value)
		if err != nil || n == 0 || n > MaxVaultBytes || bytes.ContainsAny(value, "\r\n") {
			clear(secret)
			return nil, ErrUnavailable
		}
		entries[key] = secret[:n]
	}
	valid = true
	return entries, nil
}

func (f *File) write(ctx context.Context, entries map[string][]byte) error {
	if len(entries) > 4096 {
		return ErrUnavailable
	}
	keys := make([]string, 0, len(entries))
	size := len(f.header())
	for key, secret := range entries {
		keys = append(keys, key)
		size += 7 + hex.EncodedLen(len(key)) + 1 + base64.StdEncoding.EncodedLen(len(secret)) + 1
	}
	if size > maxFileVaultBytes {
		return ErrUnavailable
	}
	slices.Sort(keys)
	raw := make([]byte, 0, size)
	defer func() { clear(raw) }()
	raw = append(raw, f.header()...)
	for _, key := range keys {
		raw = append(raw, "SECRET_"...)
		raw = hex.AppendEncode(raw, []byte(key))
		raw = append(raw, '=')
		raw = base64.StdEncoding.AppendEncode(raw, entries[key])
		raw = append(raw, '\n')
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	temp, err := os.CreateTemp(f.directory, ".secrets-*")
	if err != nil {
		return ErrUnavailable
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	if _, err := temp.Write(raw); err != nil || temp.Sync() != nil || temp.Close() != nil || statefs.CheckFile(temp.Name()) != nil {
		return ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.publish(temp.Name(), filepath.Join(f.directory, fileVaultName)) != nil || statefs.SyncDir(f.directory) != nil {
		return ErrUnavailable
	}
	return nil
}
