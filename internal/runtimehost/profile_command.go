package runtimehost

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operatorcli"
)

func profileCommand(ctx context.Context, r operatorcli.Request, out io.Writer) (err error) {
	var document ProfileDocument
	switch r.Action {
	case "apply":
		raw, readErr := readPrivate(r.File, 8192)
		if readErr != nil {
			return ErrRequest
		}
		defer clear(raw)
		document, err = ParseProfile(raw)
		if err != nil {
			return err
		}
	case "show", "grant", "revoke":
		if !profileName.MatchString(r.ID) {
			return ErrRequest
		}
		if r.Action != "show" && !domain.WorkspaceID(r.Workspace).Valid() {
			return ErrRequest
		}
	default:
		return ErrRequest
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	h, err := Open(ctx, r.Root)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, h.Close()) }()
	var result any
	switch r.Action {
	case "apply":
		result, err = h.ApplyProfile(ctx, document)
	case "show":
		result, err = h.Profile(ctx, r.ID)
	default:
		result, err = h.GrantProfile(ctx, domain.WorkspaceID(r.Workspace), r.ID, r.Action == "grant")
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(result)
}
