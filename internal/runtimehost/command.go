package runtimehost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	"github.com/vankhaivn/compute-relay/internal/admission"
	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operatorcli"
	"github.com/vankhaivn/compute-relay/internal/provider/kaggle"
)

// Command is the main executable's local composition boundary. Non-serve commands
// are finite. Provider credentials are resolved only when serve is explicitly
// configured with the complete provider authorization flag set.
func Command(parent context.Context, r operatorcli.Request, out io.Writer) (err error) {
	ctx := parent
	if r.Command != "serve" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(parent, 30*time.Second)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.Command == "profile" {
		return profileCommand(ctx, r, out)
	}
	if r.Command == "validate" {
		return validateFile(ctx, r.File, out)
	}
	if r.Command == "init" {
		result, err := Initialize(ctx, r.Root)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(result)
	}
	switch r.Command {
	case "state":
	case "serve":
		if !ValidListen(r.Listen) {
			return ErrRequest
		}
	case "workspace":
		if !domain.WorkspaceID(r.ID).Valid() || (r.Action != "create" && r.Action != "show" && r.Action != "enable" && r.Action != "disable") {
			return ErrRequest
		}
	case "token":
		if r.Action != "issue" && r.Action != "revoke" {
			return ErrRequest
		}
	default:
		return ErrRequest
	}
	h, err := Open(ctx, r.Root)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, h.Close()) }()
	encoder := json.NewEncoder(out)
	switch r.Command {
	case "state":
		result, err := h.Status(ctx)
		if err != nil {
			return err
		}
		return encoder.Encode(result)
	case "serve":
		serve := ServeConfig{Address: r.Listen}
		if r.ProviderConfig != "" {
			raw, err := readPrivate(r.ProviderConfig, 8192)
			if err != nil {
				return ErrRequest
			}
			config, err := kaggle.ParseConfig(raw)
			clear(raw)
			if err != nil {
				return ErrRequest
			}
			serve.Kaggle = &KaggleServeConfig{
				Config: config, Profile: r.ProviderProfile,
				MachineShape: r.ProviderMachineShape, MaxAttempts: r.MaxProviderAttempts,
			}
		}
		return h.ServeConfigured(ctx, serve, func(result Listening) error { return encoder.Encode(result) })
	case "workspace":
		var result WorkspaceView
		switch r.Action {
		case "create":
			result, err = h.CreateWorkspace(ctx, domain.WorkspaceID(r.ID))
		case "show":
			result, err = h.Workspace(ctx, domain.WorkspaceID(r.ID))
		default:
			result, err = h.EnableWorkspace(ctx, domain.WorkspaceID(r.ID), r.Action == "enable")
		}
		if err != nil {
			return err
		}
		return encoder.Encode(result)
	case "token":
		if r.Action == "revoke" {
			if err := h.RevokeToken(ctx, r.ID); err != nil {
				return err
			}
			return encoder.Encode(map[string]any{"id": r.ID, "revoked": true})
		}
		scopes := make([]auth.Scope, len(r.Scopes))
		for i, s := range r.Scopes {
			scopes[i] = auth.Scope(s)
		}
		result, err := h.IssueToken(ctx, domain.WorkspaceID(r.Workspace), scopes, r.TTL, r.Output)
		// Retain a non-secret token ID even if delivery/revocation was uncertain.
		if result.ID != "" {
			err = errors.Join(err, encoder.Encode(result))
		}
		return err
	}
	return ErrRequest
}

func validateFile(ctx context.Context, path string, out io.Writer) error {
	if path == "" {
		return ErrRequest
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() > 1<<20 {
		return ErrRequest
	}
	file, err := os.Open(path)
	if err != nil {
		return ErrRequest
	}
	opened, statErr := file.Stat()
	if statErr != nil || !os.SameFile(before, opened) {
		_ = file.Close()
		return ErrRequest
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	after, statErr := file.Stat()
	closeErr := file.Close()
	defer clear(raw)
	if readErr != nil || statErr != nil || closeErr != nil || int64(len(raw)) != opened.Size() || after.Size() != opened.Size() || !after.ModTime().Equal(opened.ModTime()) {
		return ErrRequest
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	request, err := admission.Parse(raw)
	if err != nil {
		return ErrRequest
	}
	sum := sha256.Sum256(request.Canonical())
	return json.NewEncoder(out).Encode(map[string]any{"status": "schema-valid-local", "specification_sha256": hex.EncodeToString(sum[:]), "admitted": false, "provider_checked": false})
}
