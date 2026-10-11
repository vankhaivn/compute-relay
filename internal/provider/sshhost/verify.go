package sshhost

import "context"

// VerifyHost logs in with one vault entry and proves a Linux Docker engine answers. A
// configured fingerprint must match; otherwise the presented host key is pinned into the
// returned canonical account. It never pulls images or creates containers.
func VerifyHost(ctx context.Context, raw []byte) (string, error) {
	endpoint, err := ParseEndpoint(raw)
	if err != nil {
		return "", err
	}
	client, err := Dial(ctx, endpoint.Target, endpoint.Credential)
	if err != nil {
		return "", err
	}
	defer client.Close()
	return verifyDocker(ctx, client, endpoint, client.HostKey)
}

func verifyDocker(ctx context.Context, exec executor, endpoint Endpoint, presented string) (string, error) {
	binary, err := findDocker(ctx, exec, endpoint.DockerPath)
	if err != nil {
		return "", err
	}
	if os, err := (docker{exec: exec, binary: binary}).serverOS(ctx); err != nil || os != "linux" {
		return "", ErrRemoteDocker
	}
	target := endpoint.Target
	target.HostKey = presented
	return Account(target)
}
