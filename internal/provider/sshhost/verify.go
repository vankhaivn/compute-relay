package sshhost

import (
	"context"
	"errors"
)

// HostVerification contains non-secret evidence from the SSH peer and Docker engine.
type HostVerification struct {
	Account      string
	HostKey      string
	OS           string
	ImageID      string
	Architecture string
	MachineID    string
	EngineID     string
}

// VerifyHost retains the canonical identity interface for existing callers.
func VerifyHost(ctx context.Context, raw []byte) (string, error) {
	verified, err := VerifyHostDetails(ctx, raw)
	return verified.Account, err
}

// VerifyHostDetails authenticates and inspects the engine without remote mutations.
func VerifyHostDetails(ctx context.Context, raw []byte) (HostVerification, error) {
	endpoint, err := ParseEndpoint(raw)
	if err != nil {
		return HostVerification{}, failure(err, "ssh_endpoint_invalid")
	}
	client, err := Dial(ctx, endpoint.Target, endpoint.Credential)
	if err != nil {
		return HostVerification{}, failure(err, failureReasonForTransport(err))
	}
	defer client.Close()
	return verifyDocker(ctx, client, endpoint, client.HostKey)
}

func verifyDocker(ctx context.Context, exec executor, endpoint Endpoint, presented string) (HostVerification, error) {
	binary, err := findDocker(ctx, exec, endpoint.DockerPath)
	if err != nil {
		return HostVerification{}, err
	}
	os, arch, err := (docker{exec: exec, binary: binary}).platform(ctx)
	if err != nil || os != "linux" || !SupportedArchitecture(arch) {
		return HostVerification{}, failure(errors.Join(ErrRemoteDocker, err), "docker_platform_unverified")
	}
	target := endpoint.Target
	target.HostKey = presented
	account, err := Account(target)
	return HostVerification{Account: account, HostKey: presented, OS: os, Architecture: arch}, err
}

// SupportedArchitecture limits execution to the standard image's qualified platforms.
func SupportedArchitecture(arch string) bool { return arch == "amd64" || arch == "arm64" }
