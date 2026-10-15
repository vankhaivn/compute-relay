package sshhost

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"io"
	"slices"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

//go:embed host_verifier.py
var hostVerifierSource string

// The producer's status goes to the original stdout through fd 3. The verifier
// consumes the local pipe to EOF before emitting its bounded inventory. POSIX
// pipeline status alone would only report the verifier's exit status.
const hostVerifierShell = `exec 3>&1; { /usr/bin/env -u DOCKER_CONTEXT -u DOCKER_HOST "$1" --host "$2" container cp "$3:/relay" -; cp_status=$?; printf 'cp:%s\n' "$cp_status" >&3; } | /usr/bin/env -u DOCKER_CONTEXT -u DOCKER_HOST "$1" --host "$2" container run --rm -i --pull=never --user 65534:65534 --network none --cap-drop ALL --security-opt no-new-privileges --read-only --pids-limit 32 --memory 134217728 --memory-swap 134217728 --cpus 1 --label io.compute-relay.kind=verification --label "io.compute-relay.installation=$8" --label "io.compute-relay.attempt=$9" --label "io.compute-relay.operation=${10}" --entrypoint python3 "$4" -B -E -s -c "$5" "$6" "$7"`

const hostVerifierBootstrap = "import base64,sys;exec(base64.b64decode(sys.argv[1]))"

type verifierAllowed struct {
	Name  string `json:"name"`
	Bytes int64  `json:"bytes"`
}

type verifierDigest struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type verifierInventory struct {
	Files  []verifierDigest `json:"files"`
	Marker *verifierDigest  `json:"marker"`
}

type verifierOutput struct {
	bytes.Buffer
	overLimit bool
}

func (o *verifierOutput) Write(p []byte) (int, error) {
	if o.Len()+len(p) > 128<<10 {
		o.overLimit = true
		return 0, errResponseLimit
	}
	return o.Buffer.Write(p)
}

// standardVerifierImage resolves the installed standard image independently of
// the target image. Application images may later back the target container.
func (a *Adapter) standardVerifierImage(ctx context.Context, d docker) (string, error) {
	image, found, err := d.inspectImage(ctx, a.config.Image)
	if err != nil {
		return "", err
	}
	if !found || !ValidImageID(image.ID) || image.OS != "linux" || !SupportedArchitecture(image.Architecture) ||
		a.config.ImageRecipeSHA256 == "" && a.config.Image != StandardImage ||
		a.config.ImageRecipeSHA256 != "" && (image.Config.Labels[imageRecipeLabel] != a.config.ImageRecipeSHA256 || image.Config.Labels["io.compute-relay.image-schema"] != "1" || image.Architecture != a.config.Architecture) {
		return "", ErrIdentity
	}
	return image.ID, nil
}

// verifyStagedOnHost hashes a stopped container's Docker archive on the same
// machine. Only a small inventory crosses SSH. The target is never started.
func (a *Adapter) verifyStagedOnHost(ctx context.Context, d docker, info containerInfo, identity provider.Identity, expected map[string]stagedFile) error {
	labels := info.Config.Labels
	operation := domain.OperationID(labels[labelPrefix+"preparation"])
	if identity.Validate() != nil || !containerID.MatchString(info.ID) || info.Started() || info.State.Running || !owned(info, identity) || !operation.Valid() ||
		labels[labelPrefix+"workspace"] != string(identity.WorkspaceID) || labels[labelPrefix+"job"] != string(identity.JobID) || labels[labelPrefix+"nonce"] != identity.Nonce {
		return ErrIdentity
	}
	imageID, err := a.standardVerifierImage(ctx, d)
	if err != nil {
		return err
	}
	d, err = d.localResource(ctx)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(expected))
	for name := range expected {
		names = append(names, name)
	}
	slices.Sort(names)
	allowed := make([]verifierAllowed, 0, len(names))
	for _, name := range names {
		allowed = append(allowed, verifierAllowed{Name: "relay/" + name, Bytes: expected[name].Bytes})
	}
	encoded, err := json.Marshal(allowed)
	if err != nil {
		return ErrIdentity
	}
	var output verifierOutput
	argv := []string{"/bin/sh", "-c", hostVerifierShell, "relay-verifier", d.binary, d.host, info.ID, imageID,
		hostVerifierBootstrap, base64.StdEncoding.EncodeToString([]byte(hostVerifierSource)), base64.StdEncoding.EncodeToString(encoded),
		string(identity.InstallationID), string(identity.AttemptID), string(operation)}
	result, err := d.exec.Run(ctx, d.env(), argv, nil, &output)
	if err != nil {
		return err
	}
	if output.overLimit {
		return ErrIdentity
	}
	status, raw, ok := bytes.Cut(output.Bytes(), []byte("\n"))
	if result.ExitCode != 0 || !ok || !bytes.Equal(status, []byte("cp:0")) {
		return ErrIdentity
	}
	var inventory verifierInventory
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&inventory) != nil || decoder.Decode(new(any)) != io.EOF || len(inventory.Files) != len(names) {
		return ErrIdentity
	}
	for i, name := range names {
		got, want := inventory.Files[i], expected[name]
		if got.Bytes != want.Bytes || got.SHA256 != string(want.SHA256) {
			return ErrIdentity
		}
	}
	if inventory.Marker != nil && (inventory.Marker.Bytes < 0 || inventory.Marker.Bytes > 4096 || !domain.SHA256Digest(inventory.Marker.SHA256).Valid()) {
		return ErrIdentity
	}
	return nil
}
