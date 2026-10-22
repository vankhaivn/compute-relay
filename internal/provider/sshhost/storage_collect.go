package sshhost

import (
	"context"

	"github.com/vankhaivn/compute-relay/internal/hoststorage"
)

// CollectStorage uses a verified account and frozen machine/engine identity.
// The caller supplies only a vault-resolved credential for this bounded call.
func CollectStorage(ctx context.Context, raw []byte, account string, scope hoststorage.Scope) (hoststorage.Scan, error) {
	if !scope.Valid() {
		return hoststorage.Scan{}, failure(hoststorage.ErrInvalid, "storage_scope_invalid")
	}
	endpoint, err := ParseEndpoint(raw)
	if err != nil {
		return hoststorage.Scan{}, failure(ErrRequest, "storage_endpoint_invalid")
	}
	defer clear(endpoint.Credential.PrivateKey)
	defer clear(endpoint.Credential.Passphrase)
	defer clear(endpoint.Credential.Password)
	endpoint, err = endpoint.Pin(account)
	if err != nil {
		return hoststorage.Scan{}, failure(ErrHostKeyMismatch, "host_key_mismatch")
	}
	ctx, cancel := context.WithTimeout(ctx, hoststorage.ScanTimeout)
	defer cancel()
	client, err := Dial(ctx, endpoint.Target, endpoint.Credential)
	if err != nil {
		return hoststorage.Scan{}, failure(err, failureReasonForTransport(err))
	}
	defer client.Close()
	binary, err := findDocker(ctx, client, endpoint.DockerPath)
	if err != nil {
		return hoststorage.Scan{}, err
	}
	d, err := (docker{exec: client, binary: binary}).localResource(ctx)
	if err != nil {
		return hoststorage.Scan{}, err
	}
	if err = d.verifyResource(ctx, string(scope.MachineID), string(scope.EngineID)); err != nil {
		return hoststorage.Scan{}, err
	}
	return scanDockerStorage(ctx, d, scope)
}
