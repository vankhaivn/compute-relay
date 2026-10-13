package sshhost

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path"
	"regexp"
	"strings"
)

var ErrResourceIdentity = errors.New("SSH scheduling resource identity unavailable or changed")

var (
	linuxMachineID = regexp.MustCompile(`^[0-9a-f]{32}$`)
	platformUUID   = regexp.MustCompile(`(?m)^\s*"IOPlatformUUID"\s*=\s*"([0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12})"\s*$`)
)

// resourceDigest namespaces native identifiers before they enter durable state.
// Raw machine UUIDs, endpoint names and socket paths are never public evidence.
func resourceDigest(kind, native string) string {
	hash := hmac.New(sha256.New, []byte("compute-relay/scheduling-resource/v1"))
	_, _ = hash.Write([]byte(kind + "\x00" + native))
	return hex.EncodeToString(hash.Sum(nil))
}

func readIdentity(ctx context.Context, exec executor, limit int64, args ...string) ([]byte, error) {
	var out bytes.Buffer
	result, err := exec.Run(ctx, nil, args, nil, &limitedWriter{w: &out, limit: limit})
	if err != nil || result.ExitCode != 0 {
		return nil, ErrResourceIdentity
	}
	return out.Bytes(), nil
}

// machineIdentity reads stable OS identity through the authenticated SSH peer.
// Credentials, login names, addresses and host keys do not contribute to equality.
func machineIdentity(ctx context.Context, exec executor) (string, error) {
	raw, err := readIdentity(ctx, exec, 32, "/usr/bin/uname", "-s")
	if err != nil {
		return "", err
	}
	switch strings.TrimSpace(string(raw)) {
	case "Linux":
		raw, err = readIdentity(ctx, exec, 64, "/bin/cat", "/etc/machine-id")
		id := strings.TrimSpace(string(raw))
		if err != nil || !linuxMachineID.MatchString(id) || id == strings.Repeat("0", 32) {
			return "", ErrResourceIdentity
		}
		return resourceDigest("linux-machine-id", id), nil
	case "Darwin":
		raw, err = readIdentity(ctx, exec, 64<<10, "/usr/sbin/ioreg", "-rd1", "-c", "IOPlatformExpertDevice")
		matches := platformUUID.FindAllSubmatch(raw, -1)
		if err != nil || len(matches) != 1 {
			return "", ErrResourceIdentity
		}
		id := strings.ToLower(string(matches[0][1]))
		if strings.ReplaceAll(id, "-", "") == strings.Repeat("0", 32) {
			return "", ErrResourceIdentity
		}
		return resourceDigest("darwin-platform-uuid", id), nil
	default:
		return "", ErrResourceIdentity
	}
}

// localResource refuses Docker contexts on another host. Reading native machine
// identity at an SSH gateway cannot establish the capacity of a remote daemon.
func (d docker) localResource(ctx context.Context) (docker, error) {
	if d.host != "" {
		return d, nil
	}
	raw, err := readIdentity(ctx, d.exec, 4096, "/bin/sh", "-c", `printf '%s\n%s\n' "${DOCKER_CONTEXT-}" "${DOCKER_HOST-}"`)
	if err != nil {
		return docker{}, err
	}
	values := strings.Split(string(raw), "\n")
	if len(values) != 3 || values[2] != "" {
		return docker{}, ErrResourceIdentity
	}
	host := values[1]
	if values[0] != "" || host == "" {
		var out bytes.Buffer
		args := []string{"context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"}
		if values[0] != "" {
			args = append(args, "--", values[0])
		}
		result, err := d.run(ctx, nil, &limitedWriter{w: &out, limit: 4096}, args...)
		if err != nil || result.ExitCode != 0 || json.Unmarshal(out.Bytes(), &host) != nil {
			return docker{}, ErrResourceIdentity
		}
	}
	socket, ok := strings.CutPrefix(host, "unix://")
	if !ok || !path.IsAbs(socket) || path.Clean(socket) != socket || !printable.MatchString(socket) || len(socket) > 2048 {
		return docker{}, ErrResourceIdentity
	}
	d.host = host
	return d, nil
}

func (d docker) engineIdentity(ctx context.Context) (string, error) {
	var out bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &out, limit: 1024}, "info", "--format", "{{json .ID}}")
	var native string
	if err != nil || result.ExitCode != 0 || json.Unmarshal(out.Bytes(), &native) != nil || len(native) < 8 || len(native) > 256 || !safeArg.MatchString(native) {
		return "", ErrResourceIdentity
	}
	return resourceDigest("docker-engine-id", native), nil
}

func (d docker) verifyResource(ctx context.Context, machine, engine string) error {
	actual, err := machineIdentity(ctx, d.exec)
	if err != nil || actual != machine {
		return ErrResourceIdentity
	}
	actual, err = d.engineIdentity(ctx)
	if err != nil || engine != "" && actual != engine {
		return ErrResourceIdentity
	}
	return nil
}
