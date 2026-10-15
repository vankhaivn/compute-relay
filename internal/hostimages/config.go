package hostimages

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func rejectCaseAliases(m map[string]json.RawMessage, names ...string) error {
	for key := range m {
		for _, name := range names {
			if key != name && strings.EqualFold(key, name) {
				return ErrInvalidArchive
			}
		}
	}
	return nil
}

func rewriteConfig(raw []byte, opts Options, limits Limits) (parsedImage, error) {
	top, err := strictObject(raw, limits.MaxMetadataBytes)
	if err != nil || rejectCaseAliases(top, "os", "architecture", "rootfs", "config") != nil {
		return parsedImage{}, ErrInvalidArchive
	}
	osName, err := stringField(top, "os")
	if err != nil || osName != "linux" {
		return parsedImage{}, ErrInvalidArchive
	}
	arch, err := stringField(top, "architecture")
	if err != nil || arch != opts.Architecture {
		return parsedImage{}, ErrInvalidArchive
	}
	root, err := strictObject(top["rootfs"], limits.MaxMetadataBytes)
	if err != nil || rejectCaseAliases(root, "type", "diff_ids") != nil {
		return parsedImage{}, ErrInvalidArchive
	}
	rtype, err := stringField(root, "type")
	if err != nil || rtype != "layers" {
		return parsedImage{}, ErrInvalidArchive
	}
	var diffIDs []string
	if json.Unmarshal(root["diff_ids"], &diffIDs) != nil || len(diffIDs) < 1 || len(diffIDs) > limits.MaxLayers {
		return parsedImage{}, ErrInvalidArchive
	}
	for _, digest := range diffIDs {
		if !strings.HasPrefix(digest, "sha256:") || !domain.SHA256Digest(strings.TrimPrefix(digest, "sha256:")).Valid() {
			return parsedImage{}, ErrInvalidArchive
		}
	}
	var config map[string]json.RawMessage
	if len(top["config"]) == 0 || strings.TrimSpace(string(top["config"])) == "null" {
		config = map[string]json.RawMessage{}
	} else {
		config, err = strictObject(top["config"], limits.MaxMetadataBytes)
		if err != nil {
			return parsedImage{}, err
		}
	}
	if rejectCaseAliases(config, "Labels", "Entrypoint", "Volumes", "Healthcheck") != nil {
		return parsedImage{}, ErrInvalidArchive
	}
	if entry := config["Entrypoint"]; len(entry) != 0 && strings.TrimSpace(string(entry)) != "null" {
		var parts []string
		if json.Unmarshal(entry, &parts) != nil || len(parts) != 0 {
			return parsedImage{}, ErrInvalidArchive
		}
	}
	if volumes := config["Volumes"]; len(volumes) != 0 && strings.TrimSpace(string(volumes)) != "null" {
		v, err := strictObject(volumes, limits.MaxMetadataBytes)
		if err != nil || len(v) != 0 {
			return parsedImage{}, ErrInvalidArchive
		}
	}
	labels := map[string]string{}
	if current := config["Labels"]; len(current) != 0 && strings.TrimSpace(string(current)) != "null" {
		if _, err := strictObject(current, limits.MaxMetadataBytes); err != nil || json.Unmarshal(current, &labels) != nil {
			return parsedImage{}, ErrInvalidArchive
		}
	}
	for name := range labels {
		if strings.HasPrefix(strings.ToLower(name), "io.compute-relay.") {
			return parsedImage{}, ErrInvalidArchive
		}
	}
	if len(labels) > 26 {
		return parsedImage{}, ErrLimitExceeded
	}
	labels[LabelInstallation] = string(opts.InstallationID)
	labels[LabelKind] = "application"
	labels[LabelArchive] = string(opts.ArchiveSHA256)
	labels[LabelSchema] = ApplicationSchema
	labels[LabelMachine] = string(opts.MachineID)
	labels[LabelEngine] = string(opts.EngineID)
	labelJSON, _ := json.Marshal(labels)
	if len(labelJSON) > 4096 {
		return parsedImage{}, ErrLimitExceeded
	}
	config["Labels"] = labelJSON
	configJSON, err := json.Marshal(config)
	if err != nil {
		return parsedImage{}, ErrInvalidArchive
	}
	top["config"] = configJSON
	result, err := json.Marshal(top)
	if err != nil || int64(len(result)) > limits.MaxMetadataBytes {
		return parsedImage{}, ErrLimitExceeded
	}
	id := sha256.Sum256(result)
	return parsedImage{config: result, id: "sha256:" + hex.EncodeToString(id[:]), architecture: arch, diffIDs: diffIDs}, nil
}
