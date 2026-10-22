package sshhost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/hostimages"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

// observeApplicationImage never inspects an ID outside the installation's
// filtered image list. A Docker load response is not identity evidence.
func (d docker) observeApplicationImage(ctx context.Context, g hostimages.Generation, architecture string) (bool, error) {
	if !g.Valid() || g.ExpectedImageID == "" || !SupportedArchitecture(architecture) {
		return false, failure(ErrRequest, "observe_application_image_request_invalid")
	}
	var listed bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &listed, limit: 1 << 20}, "image", "ls", "--all", "--no-trunc",
		"--filter", "label="+hostimages.LabelInstallation+"="+string(g.Key.InstallationID), "--format", "{{.ID}}")
	if err != nil || result.ExitCode != 0 {
		return false, failure(errors.Join(ErrRemoteDocker, err), "application_image_observation_failed")
	}
	found := false
	for _, line := range strings.Split(strings.TrimSpace(listed.String()), "\n") {
		if line == "" {
			continue
		}
		if !ValidImageID(line) {
			return false, failure(errors.Join(ErrRemoteDocker, err), "application_image_observation_failed")
		}
		found = found || line == g.ExpectedImageID
	}
	if !found {
		return false, nil
	}
	var output bytes.Buffer
	result, err = d.run(ctx, nil, &limitedWriter{w: &output, limit: 1 << 20}, "image", "inspect", g.ExpectedImageID)
	if err != nil {
		return false, failure(errors.Join(ErrRemoteDocker, err), "application_image_observation_failed")
	}
	if missing(result) {
		return false, nil
	}
	if result.ExitCode != 0 {
		return false, failure(errors.Join(ErrRemoteDocker, err), "application_image_observation_failed")
	}
	var images []struct {
		ID           string   `json:"Id"`
		OS           string   `json:"Os"`
		Architecture string   `json:"Architecture"`
		RepoTags     []string `json:"RepoTags"`
		RepoDigests  []string `json:"RepoDigests"`
		Config       struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
		RootFS struct {
			Layers []string `json:"Layers"`
		} `json:"RootFS"`
	}
	if json.Unmarshal(output.Bytes(), &images) != nil || len(images) != 1 {
		return false, failure(errors.Join(ErrRemoteDocker, err), "application_image_observation_failed")
	}
	image := images[0]
	if image.ID != g.ExpectedImageID || image.OS != "linux" || image.Architecture != architecture ||
		len(image.RepoTags) != 0 || len(image.RepoDigests) != 0 || !slices.Equal(image.RootFS.Layers, g.RootFSDiffIDs) {
		return false, failure(ErrIdentity, "observe_application_image_identity_unverified")
	}
	want := map[string]string{
		hostimages.LabelInstallation: string(g.Key.InstallationID),
		hostimages.LabelKind:         "application",
		hostimages.LabelArchive:      string(g.Key.ArchiveSHA256),
		hostimages.LabelSchema:       hostimages.ApplicationSchema,
		hostimages.LabelMachine:      string(g.Key.MachineID),
		hostimages.LabelEngine:       string(g.Key.EngineID),
	}
	for name, value := range want {
		if image.Config.Labels[name] != value {
			return false, failure(ErrIdentity, "observe_application_image_identity_unverified")
		}
	}
	for name := range image.Config.Labels {
		if strings.HasPrefix(strings.ToLower(name), "io.compute-relay.") {
			if _, ok := want[name]; !ok {
				return false, failure(ErrIdentity, "observe_application_image_identity_unverified")
			}
		}
	}
	return true, nil
}

func (d docker) loadApplicationArchive(ctx context.Context, archive io.Reader) error {
	bounded, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	result, err := d.run(bounded, archive, &limitedWriter{w: io.Discard, limit: 8 << 10}, "image", "load", "--quiet")
	if err != nil || result.ExitCode != 0 {
		return failure(errors.Join(ErrRemoteDocker, err), "application_image_load_failed")
	}
	return nil
}

const imageStoreFormat = `{"driver":{{json .Driver}},"status":{{json .DriverStatus}}}`

// The classic store identifies an image by its config; containerd identifies the
// canonical manifest. Select that exact digest before committing the load intent.
func (d docker) applicationImageID(ctx context.Context, archive *hostimages.Prepared) (string, error) {
	if archive == nil || !ValidImageID(archive.ImageID) || !ValidImageID(archive.ManifestID) {
		return "", failure(ErrRequest, "application_image_request_invalid")
	}
	var output bytes.Buffer
	result, err := d.run(ctx, nil, &limitedWriter{w: &output, limit: 8 << 10}, "info", "--format", imageStoreFormat)
	if err != nil || result.ExitCode != 0 {
		return "", failure(errors.Join(ErrRemoteDocker, err), "application_image_store_unverified")
	}
	var info struct {
		Driver string     `json:"driver"`
		Status [][]string `json:"status"`
	}
	decoder := json.NewDecoder(&output)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&info) != nil || decoder.Decode(new(any)) != io.EOF || info.Driver == "" || len(info.Driver) > 64 || len(info.Status) > 64 {
		return "", failure(errors.Join(ErrRemoteDocker, err), "application_image_store_unverified")
	}
	seen := map[string]bool{}
	driverType := ""
	for _, row := range info.Status {
		if len(row) != 2 || row[0] == "" || len(row[0]) > 256 || len(row[1]) > 256 || seen[row[0]] {
			return "", failure(errors.Join(ErrRemoteDocker, err), "application_image_store_unverified")
		}
		seen[row[0]] = true
		if row[0] == "driver-type" {
			driverType = row[1]
		}
	}
	if seen["driver-type"] {
		if driverType == "io.containerd.snapshotter.v1" {
			return archive.ManifestID, nil
		}
		return "", failure(errors.Join(ErrRemoteDocker, err), "application_image_store_unverified")
	}
	switch info.Driver {
	case "overlay", "overlay2", "aufs", "btrfs", "devicemapper", "fuse-overlayfs", "vfs", "zfs":
		return archive.ImageID, nil
	default:
		return "", failure(errors.Join(ErrRemoteDocker, err), "application_image_store_unverified")
	}
}

type applicationImageContext struct {
	config            Config
	blobs             StagingBlobs
	applicationImages hostimages.Repository
}

// frozenApplicationImage resolves an admitted attempt through its immutable pin.
// It never loads an image, and it verifies the current Docker object before
// ReconcilePrepare or Start can trust the stored derived ID.
func (a applicationImageContext) frozenApplicationImage(ctx context.Context, d docker, id provider.Identity) (string, bool, error) {
	if a.applicationImages == nil {
		return "", false, nil
	}
	pin, err := a.applicationImages.PinnedImage(ctx, id.WorkspaceID, id.JobID, id.AttemptID)
	if err != nil || pin == nil {
		return "", false, err
	}
	if pin.Released || pin.Key.InstallationID != id.InstallationID ||
		pin.Key.MachineID != domain.SHA256Digest(a.config.MachineID) ||
		pin.Key.EngineID != domain.SHA256Digest(a.config.EngineID) ||
		!SupportedArchitecture(a.config.Architecture) {
		return "", true, failure(ErrIdentity, "frozen_application_image_identity_unverified")
	}
	g, err := a.applicationImages.ActiveImage(ctx, pin.Key)
	if err != nil {
		return "", true, err
	}
	if g == nil || g.State != hostimages.StateReady {
		return "", true, failure(hostimages.ErrImageState, "frozen_application_image_image_state_unknown")
	}
	present, err := d.observeApplicationImage(ctx, *g, a.config.Architecture)
	if err != nil {
		return "", true, err
	}
	if !present {
		return "", true, failure(hostimages.ErrImageState, "frozen_application_image_image_state_unknown")
	}
	return g.ExpectedImageID, true, nil
}

func validApplicationContainer(info containerInfo, imageID string) bool {
	return info.Image == imageID && info.Config.Labels[labelPrefix+"application-image"] == imageID &&
		info.Config.Healthcheck != nil && slices.Equal(info.Config.Healthcheck.Test, []string{"NONE"})
}

// selectApplicationImage is called only by Prepare. The repository commits a
// unique load intent before this adapter can stream even one byte to Docker.
func (a applicationImageContext) selectApplicationImage(ctx context.Context, d docker, plan provider.Plan) (string, error) {
	if a.applicationImages == nil || plan.Job.Inputs == nil || plan.Job.Inputs.ApplicationImage == nil ||
		!domain.SHA256Digest(a.config.MachineID).Valid() || !domain.SHA256Digest(a.config.EngineID).Valid() ||
		!SupportedArchitecture(a.config.Architecture) {
		return "", failure(ErrRequest, "select_application_image_request_invalid")
	}
	meta := *plan.Job.Inputs.ApplicationImage
	key := hostimages.Key{InstallationID: plan.Job.Identity.InstallationID,
		MachineID: domain.SHA256Digest(a.config.MachineID), EngineID: domain.SHA256Digest(a.config.EngineID),
		ArchiveSHA256: meta.SHA256, ArchiveBytes: meta.Bytes}
	owner := hostimages.Owner{WorkspaceID: plan.Job.Identity.WorkspaceID, JobID: plan.Job.Identity.JobID,
		AttemptID: plan.Job.Identity.AttemptID, ObjectID: meta.ID}
	if !key.Valid() || !owner.Valid() {
		return "", failure(ErrRequest, "select_application_image_request_invalid")
	}
	osName, arch, err := d.platform(ctx)
	if err != nil || osName != "linux" || arch != a.config.Architecture {
		return "", failure(errors.Join(ErrIdentity, err), "select_application_image_identity_unverified")
	}
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		g, elected, err := a.applicationImages.ClaimImage(ctx, owner, key)
		if err != nil {
			return "", err
		}
		if elected {
			blob, err := a.blobs.Open(ctx, meta.WorkspaceID, meta.ID)
			if err != nil {
				_ = a.applicationImages.RetirePreloadImage(ctx, g)
				return "", err
			}
			prepared, normErr := hostimages.Normalize(ctx, blob, hostimages.Options{ArchiveSHA256: meta.SHA256,
				ArchiveBytes: meta.Bytes, InstallationID: key.InstallationID, MachineID: key.MachineID,
				EngineID: key.EngineID, Architecture: a.config.Architecture})
			closeErr := blob.Close()
			if normErr != nil || closeErr != nil {
				_ = a.applicationImages.RetirePreloadImage(ctx, g)
				if normErr != nil {
					return "", failure(normErr, "application_image_archive_invalid")
				}
				return "", failure(closeErr, "application_image_archive_close_failed")
			}
			id, err := d.applicationImageID(ctx, prepared)
			if err != nil {
				_ = prepared.Close()
				_ = a.applicationImages.RetirePreloadImage(ctx, g)
				return "", err
			}
			diffs := append([]string(nil), prepared.RootFSDiffIDs...)
			begun, beginErr := a.applicationImages.BeginImageLoad(ctx, g, id, diffs)
			if beginErr != nil || !begun {
				_ = prepared.Close()
				if beginErr != nil {
					return "", beginErr
				}
				continue
			}
			g.State, g.ExpectedImageID, g.RootFSDiffIDs = hostimages.StateLoading, id, diffs
			present, observeErr := d.observeApplicationImage(ctx, g, a.config.Architecture)
			if observeErr != nil {
				_ = prepared.Close()
				return "", observeErr
			}
			var loadErr error
			if !present {
				loadErr = d.loadApplicationArchive(ctx, prepared)
			}
			_ = prepared.Close()
			present, observeErr = d.observeApplicationImage(ctx, g, a.config.Architecture)
			if observeErr != nil {
				return "", observeErr
			}
			if !present {
				_ = a.applicationImages.MarkImageUnknown(ctx, g)
				return "", failure(errors.Join(hostimages.ErrImageState, loadErr), "application_image_load_unresolved")
			}
			if err := a.applicationImages.MarkImageReady(ctx, g); err != nil {
				return "", err
			}
			return id, nil
		}
		switch g.State {
		case hostimages.StateReady:
			present, err := d.observeApplicationImage(ctx, g, a.config.Architecture)
			if err != nil {
				return "", err
			}
			if present {
				if err := a.applicationImages.TouchImage(ctx, g); err != nil {
					return "", err
				}
				return g.ExpectedImageID, nil
			}
			if err := a.applicationImages.RetireMissingImage(ctx, g); err != nil {
				return "", err
			}
		case hostimages.StateLoading, hostimages.StateUnknown:
			present, err := d.observeApplicationImage(ctx, g, a.config.Architecture)
			if err != nil {
				return "", err
			}
			if present {
				if err := a.applicationImages.MarkImageReady(ctx, g); err != nil {
					return "", err
				}
				return g.ExpectedImageID, nil
			}
			if g.State == hostimages.StateUnknown || !g.LoadingAt.IsZero() && time.Since(g.LoadingAt) > 10*time.Minute {
				_ = a.applicationImages.MarkImageUnknown(ctx, g)
				return "", failure(hostimages.ErrImageState, "select_application_image_image_state_unknown")
			}
		case hostimages.StatePreparing:
			// Another writer is still doing purely local normalization.
		default:
			return "", failure(hostimages.ErrImageState, "select_application_image_image_state_unknown")
		}
		wait := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			wait.Stop()
			return "", ctx.Err()
		case <-wait.C:
		}
	}
}
