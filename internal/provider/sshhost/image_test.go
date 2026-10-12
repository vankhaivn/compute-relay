package sshhost

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func imageHost(t *testing.T) (*fakeHost, []byte) {
	t.Helper()
	fake := newFakeHost(t)
	server := newTestServer(t)
	server.Password = "synthetic-image-password"
	server.Handle = func(command string, stdin io.Reader, stdout, stderr io.Writer) int {
		result, err := fake.Run(context.Background(), nil, commandArgs(t, command), stdin, stdout)
		if err != nil {
			return 255
		}
		_, _ = stderr.Write(result.Stderr)
		return result.ExitCode
	}
	target := server.target()
	raw, err := EncodeFields(map[string]string{FieldHost: target.Host, FieldPort: strconv.Itoa(target.Port), FieldUsername: target.User, FieldPassword: server.Password})
	if err != nil {
		t.Fatal(err)
	}
	return fake, raw
}

func TestImageInstallationBuildsOnlyThePinnedRepositoryRecipe(t *testing.T) {
	fake, raw := imageHost(t)
	ctx := context.Background()
	before, err := VerifyManagedHost(ctx, raw)
	if err != nil || before.ImageID != "" || fake.builds != 0 {
		t.Fatal("verification installed an image", before, err)
	}
	op := domain.OperationID("op_image_fixture")
	installed, err := InstallImage(ctx, raw, before.Account, before.Architecture, op, false)
	if err != nil || installed.ImageID != fakeImageID || fake.builds != 1 {
		t.Fatal("installation failed", installed, err)
	}
	checked, err := VerifyManagedHost(ctx, raw)
	if err != nil || checked.ImageID != installed.ImageID || fake.builds != 1 {
		t.Fatal("check rebuilt or changed the image", checked, err)
	}
	recovered, err := InstallImage(ctx, raw, before.Account, before.Architecture, op, true)
	if err != nil || recovered != installed || fake.builds != 1 {
		t.Fatal("recovery rebuilt the image", recovered, err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	var build []string
	for _, call := range fake.calls {
		if len(call) > 1 && call[1] == "build" {
			build = call
		}
	}
	command := strings.Join(build, " ")
	for _, expected := range []string{"--pull=false", "--network=none", "--platform linux/amd64", imageRecipeLabel + "=" + ImageRecipeDigest(), "--tag " + operationImageTag(op)} {
		if !strings.Contains(command, expected) {
			t.Fatal("unbounded or unfrozen build", command)
		}
	}
	archive := tar.NewReader(bytes.NewReader(fake.buildInput))
	header, err := archive.Next()
	if err != nil || header.Name != "Dockerfile" {
		t.Fatal("wrong build context", header, err)
	}
	recipe, _ := io.ReadAll(archive)
	if !bytes.Equal(recipe, standardDockerfile) || !bytes.HasPrefix(recipe, []byte("FROM "+StandardImage+"\n")) || bytes.Contains(recipe, []byte("RUN ")) || bytes.Contains(recipe, []byte("ADD ")) || bytes.Contains(recipe, []byte("COPY ")) {
		t.Fatal("recipe has an unpinned base or mutable build steps")
	}
	if _, err = archive.Next(); err != io.EOF {
		t.Fatal("build context contains another file", err)
	}
	if strings.Contains(command+string(recipe), "synthetic-image-password") {
		t.Fatal("login leaked into image installation")
	}
}

func TestImageInstallationUnknownRecoversWithoutAnotherBuild(t *testing.T) {
	fake, raw := imageHost(t)
	ctx := context.Background()
	before, err := VerifyManagedHost(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	fake.fail = func(args []string) (Result, bool) {
		if fake.builds == 1 && args[0] == "image" {
			return Result{ExitCode: 255}, true
		}
		return Result{}, false
	}
	op := domain.OperationID("op_lost_image_response")
	if _, err = InstallImage(ctx, raw, before.Account, before.Architecture, op, false); !errors.Is(err, ErrImageInstallationUnknown) {
		t.Fatal("lost inspection became success", err)
	}
	fake.mu.Lock()
	fake.fail = nil
	fake.mu.Unlock()
	installed, err := InstallImage(ctx, raw, before.Account, before.Architecture, op, true)
	if err != nil || installed.ImageID != fakeImageID || fake.builds != 1 {
		t.Fatal("recovery repeated installation", installed, err)
	}
	if _, err = InstallImage(ctx, raw, before.Account, before.Architecture, "op_not_started", true); !errors.Is(err, ErrImageInstallationUnknown) || fake.builds != 1 {
		t.Fatal("absence authorized a new build", err)
	}
	fake.mu.Lock()
	image := fake.images[operationImageTag(op)]
	image.Config.Labels[imageOperationLabel] = "op_someone_else"
	fake.images[operationImageTag(op)] = image
	fake.mu.Unlock()
	if _, err = InstallImage(ctx, raw, before.Account, before.Architecture, op, true); !errors.Is(err, ErrIdentity) || fake.builds != 1 {
		t.Fatal("another operation image was adopted", err)
	}
}

func TestImageInstallationRejectsChangedPlatformBeforeBuilding(t *testing.T) {
	fake, raw := imageHost(t)
	ctx := context.Background()
	before, err := VerifyManagedHost(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.platform = "linux/arm64"
	fake.mu.Unlock()
	if _, err = InstallImage(ctx, raw, before.Account, before.Architecture, "op_changed_platform", false); !errors.Is(err, ErrIdentity) || fake.builds != 0 {
		t.Fatal("changed architecture built an image", err)
	}
}

func TestInstalledImagePreparationFreezesLimitsAndNeverPulls(t *testing.T) {
	h := newHarness(t)
	h.adapter.config.Image, h.adapter.config.ImageRecipeSHA256, h.adapter.config.Architecture = fakeImageID, ImageRecipeDigest(), "amd64"
	h.adapter.config.CPUs, h.adapter.config.MemoryBytes, h.adapter.config.MaxProcesses = "2", 128<<20, 128
	plan, err := h.adapter.Validate(context.Background(), h.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.adapter.Prepare(context.Background(), plan, testOperation); !errors.Is(err, ErrRemoteDocker) || h.host.created != 0 {
		t.Fatal("missing installation prepared a container", err)
	}
	info := imageInfo{ID: fakeImageID, OS: "linux", Architecture: "amd64"}
	info.Config.Labels = map[string]string{imageRecipeLabel: ImageRecipeDigest()}
	h.host.images[fakeImageID] = info
	if _, err := h.adapter.Prepare(context.Background(), plan, testOperation); err != nil {
		t.Fatal(err)
	}
	h.host.mu.Lock()
	defer h.host.mu.Unlock()
	for _, call := range h.host.calls {
		if len(call) > 1 && (call[1] == "pull" || call[1] == "build") {
			t.Fatal("prepare installed an image")
		}
	}
	var args string
	for _, c := range h.host.containers {
		args = strings.Join(c.args, " ")
	}
	for _, want := range []string{"--cpus 2", "--memory 134217728", "--pids-limit 128", "--platform linux/amd64", fakeImageID} {
		if !strings.Contains(args, want) {
			t.Fatal("frozen policy was not applied", args)
		}
	}
}

func TestImageInstallationNonzeroExitUsesImageEvidence(t *testing.T) {
	fake, raw := imageHost(t)
	ctx := context.Background()
	before, err := VerifyManagedHost(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	fake.buildExit = 137
	fake.mu.Unlock()
	installed, err := InstallImage(ctx, raw, before.Account, before.Architecture, "op_nonzero_installed", false)
	if err != nil || installed.ImageID != fakeImageID || fake.builds != 1 {
		t.Fatal("CLI exit hid an installed image", installed, err)
	}
	fake.mu.Lock()
	fake.fail = func(args []string) (Result, bool) {
		if args[0] == "build" {
			return Result{ExitCode: 137}, true
		}
		return Result{}, false
	}
	fake.mu.Unlock()
	if _, err := InstallImage(ctx, raw, before.Account, before.Architecture, "op_nonzero_absent", false); !errors.Is(err, ErrImageInstallationUnknown) {
		t.Fatal("nonzero CLI proved an absent outcome", err)
	}
}

func TestManagedHostRecheckPinsPreviouslyVerifiedKey(t *testing.T) {
	fake, raw := imageHost(t)
	ctx := context.Background()
	verified, err := VerifyManagedHost(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := ParseEndpoint(raw)
	if err != nil {
		t.Fatal(err)
	}
	endpoint.Target.HostKey = "SHA256:" + strings.Repeat("A", 43)
	previousAccount, err := Account(endpoint.Target)
	if err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	calls := len(fake.calls)
	fake.mu.Unlock()
	if _, err := VerifyManagedHostBound(ctx, raw, previousAccount); !errors.Is(err, ErrHostKeyMismatch) {
		t.Fatal("recheck trusted a different key", err)
	}
	fake.mu.Lock()
	after := len(fake.calls)
	fake.mu.Unlock()
	if after != calls {
		t.Fatal("changed key reached Docker")
	}
	if again, err := VerifyManagedHostBound(ctx, raw, verified.Account); err != nil || again.Account != verified.Account {
		t.Fatal("verified key could not be rechecked", err)
	}
}
