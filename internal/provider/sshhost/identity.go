package sshhost

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"math"
	"path"
	"regexp"
	"strconv"
	"strings"
)

// Field names of the write-only connection credential. Every field is stored only in the
// Relay vault; the endpoint is not secret but travels with the login so one immutable
// entry describes one verified host identity.
const (
	FieldHost        = "host"
	FieldPort        = "port"
	FieldUsername    = "username"
	FieldHostKey     = "host_key_fingerprint"
	FieldPrivateKey  = "private_key"
	FieldPassphrase  = "private_key_passphrase"
	FieldPassword    = "password"
	FieldDockerPath  = "docker_path"
	FieldCPUs        = "cpus"
	FieldMemoryMiB   = "memory_mib"
	maxPrivateKey    = 16 << 10
	maxShortField    = 1024
	accountPrefix    = "ssh-"
	accountKeyBytes  = sha256.Size
	accountHashChars = 32
)

var cpuLimit = regexp.MustCompile(`^[0-9]{1,3}(\.[0-9]{1,2})?$`)

// Endpoint is one decoded vault entry. Limits are optional operator settings.
type Endpoint struct {
	Target      Target
	Credential  Credential
	DockerPath  string
	CPUs        string
	MemoryBytes int64
}

type storedEndpoint struct {
	Version    int    `json:"version"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Username   string `json:"username"`
	HostKey    string `json:"host_key_fingerprint,omitempty"`
	PrivateKey string `json:"private_key,omitempty"`
	Passphrase string `json:"private_key_passphrase,omitempty"`
	Password   string `json:"password,omitempty"`
	DockerPath string `json:"docker_path,omitempty"`
	CPUs       string `json:"cpus,omitempty"`
	MemoryMiB  int64  `json:"memory_mib,omitempty"`
}

// EncodeFields validates connection request fields and returns the vault entry.
func EncodeFields(fields map[string]string) ([]byte, error) {
	known := map[string]bool{FieldHost: true, FieldPort: true, FieldUsername: true, FieldHostKey: true, FieldPrivateKey: true,
		FieldPassphrase: true, FieldPassword: true, FieldDockerPath: true, FieldCPUs: true, FieldMemoryMiB: true}
	for name, value := range fields {
		limit := maxShortField
		if name == FieldPrivateKey {
			limit = maxPrivateKey
		}
		if !known[name] || len(value) == 0 || len(value) > limit {
			return nil, ErrRequest
		}
	}
	stored := storedEndpoint{Version: 1, Host: fields[FieldHost], Port: 22, Username: fields[FieldUsername], HostKey: fields[FieldHostKey],
		PrivateKey: fields[FieldPrivateKey], Passphrase: fields[FieldPassphrase], Password: fields[FieldPassword],
		DockerPath: fields[FieldDockerPath], CPUs: fields[FieldCPUs]}
	if raw, ok := fields[FieldPort]; ok {
		port, err := strconv.Atoi(raw)
		if err != nil || strconv.Itoa(port) != raw {
			return nil, ErrRequest
		}
		stored.Port = port
	}
	if raw, ok := fields[FieldMemoryMiB]; ok {
		memory, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || strconv.FormatInt(memory, 10) != raw {
			return nil, ErrRequest
		}
		stored.MemoryMiB = memory
	}
	encoded, err := json.Marshal(stored)
	if err != nil {
		return nil, ErrRequest
	}
	if _, err = ParseEndpoint(encoded); err != nil {
		return nil, err
	}
	return encoded, nil
}

// ParseEndpoint strictly decodes a vault entry written by EncodeFields.
func ParseEndpoint(raw []byte) (Endpoint, error) {
	var stored storedEndpoint
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&stored) != nil || decoder.More() || stored.Version != 1 {
		return Endpoint{}, ErrRequest
	}
	endpoint := Endpoint{
		Target:     Target{Host: stored.Host, Port: stored.Port, User: stored.Username, HostKey: stored.HostKey},
		Credential: Credential{PrivateKey: []byte(stored.PrivateKey), Passphrase: []byte(stored.Passphrase), Password: []byte(stored.Password)},
		DockerPath: stored.DockerPath, CPUs: stored.CPUs, MemoryBytes: stored.MemoryMiB << 20,
	}
	if endpoint.Target.Validate(false) != nil || strings.HasPrefix(stored.Host, "-") {
		return Endpoint{}, ErrRequest
	}
	if _, err := endpoint.Credential.methods(); err != nil {
		return Endpoint{}, ErrRequest
	}
	if stored.DockerPath != "" && (!path.IsAbs(stored.DockerPath) || path.Clean(stored.DockerPath) != stored.DockerPath || !safeArg.MatchString(stored.DockerPath)) {
		return Endpoint{}, ErrRequest
	}
	if stored.CPUs != "" {
		cpus, err := strconv.ParseFloat(stored.CPUs, 64)
		if !cpuLimit.MatchString(stored.CPUs) || err != nil || cpus <= 0 || math.IsInf(cpus, 0) {
			return Endpoint{}, ErrRequest
		}
	}
	if stored.MemoryMiB != 0 && (stored.MemoryMiB < 64 || stored.MemoryMiB > 1<<20) {
		return Endpoint{}, ErrRequest
	}
	return endpoint, nil
}

// Account is the canonical host identity: the pinned host-key fingerprint plus a digest
// of the login endpoint. It fits the store's account identifier alphabet.
func Account(target Target) (string, error) {
	if err := target.Validate(true); err != nil {
		return "", err
	}
	key, err := base64.RawStdEncoding.Strict().DecodeString(strings.TrimPrefix(target.HostKey, "SHA256:"))
	if err != nil || len(key) != accountKeyBytes {
		return "", ErrRequest
	}
	endpoint := sha256.Sum256([]byte(target.User + "\x00" + target.Host + "\x00" + strconv.Itoa(target.Port)))
	return accountPrefix + hex.EncodeToString(key) + "-" + hex.EncodeToString(endpoint[:])[:accountHashChars], nil
}

// HostKeyOf recovers the pinned fingerprint from a canonical account.
func HostKeyOf(account string) (string, error) {
	rest, ok := strings.CutPrefix(account, accountPrefix)
	key, digest, split := strings.Cut(rest, "-")
	raw, err := hex.DecodeString(key)
	if !ok || !split || err != nil || len(raw) != accountKeyBytes || len(digest) != accountHashChars || hex.EncodeToString(raw) != key {
		return "", ErrRequest
	}
	return "SHA256:" + base64.RawStdEncoding.EncodeToString(raw), nil
}

// Pin binds a decoded endpoint to a canonical account: the account's host key replaces
// any configured fingerprint, which must agree, and the endpoint digest must match.
func (e Endpoint) Pin(account string) (Endpoint, error) {
	key, err := HostKeyOf(account)
	if err != nil || e.Target.HostKey != "" && e.Target.HostKey != key {
		return Endpoint{}, ErrHostKeyMismatch
	}
	e.Target.HostKey = key
	if derived, err := Account(e.Target); err != nil || derived != account {
		return Endpoint{}, ErrHostKeyMismatch
	}
	return e, nil
}
