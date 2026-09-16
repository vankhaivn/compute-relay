package staging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"time"
)

var (
	ErrIdentity    = errors.New("staging resource identity mismatch")
	ErrPrivacy     = errors.New("private staging is not established")
	ErrCatalog     = errors.New("staging file catalog is incomplete or inconsistent")
	ErrTransfer    = errors.New("staging byte verification failed")
	ErrChanged     = errors.New("staging changed while being verified")
	ErrObservation = errors.New("staging observation unavailable")
	ErrProcessing  = errors.New("staging processing failed; preserve creation evidence")
)

// Phase is a normalized observation, not a guessed mapping of Kaggle raw status.
// The future pinned SDK transport must justify each mapping with fixture/live evidence.
type Phase string

const (
	Unknown    Phase = "unknown"
	Absent     Phase = "absent"
	Processing Phase = "processing"
	Ready      Phase = "ready"
	Failed     Phase = "failed"
)

type Visibility string

const (
	Private Visibility = "private"
	Public  Visibility = "public"
)

// Snapshot must come from the authenticated read-only transport. Account means
// the server-verified effective account, not an echoed configuration field.
// This library checks consistency; it does not implement authentication itself.
type Snapshot struct {
	Account    string
	Reference  string
	DatasetID  string
	Version    int64
	Visibility Visibility
	Phase      Phase
}

// Pin identifies the exact dataset/version for all list and byte reads. The
// first-version-only rule avoids silently following an operator-created update.
type Pin struct {
	Reference string
	DatasetID string
	Version   int64
}
type RemoteFile struct {
	Name  string
	Bytes int64
	Kind  string
}
type Page struct {
	Files      []RemoteFile
	NextCursor string
}

// Source has deliberately no create, version, upload, submit, delete or retry-
// mutation method. Listing and downloads must target the requested pinned version.
// Callbacks/readers must cooperate with context cancellation. A transport that
// cannot supply explicit visibility, version and regular-file evidence cannot
// claim Ready. Provider exceptions must not be logged by this interface wrapper.
type Source interface {
	Inspect(context.Context, string) (Snapshot, error)
	List(context.Context, Pin, string, int) (Page, error)
	Open(context.Context, Pin, string) (io.ReadCloser, error)
}

// Assessment's fields cannot be forged by a caller through an exported bool.
// A zero value never means ready. This is a trusted-process type boundary,
// not cryptographic attestation or a durable authorization to mutate a provider.
type Assessment struct {
	phase      Phase
	pin        Pin
	planSHA256 string
}

func (a Assessment) Phase() Phase {
	if a.phase == "" {
		return Unknown
	}
	return a.phase
}
func (a Assessment) Ready() bool        { return a.phase == Ready && a.planSHA256 != "" }
func (a Assessment) Pin() Pin           { return a.pin }
func (a Assessment) PlanSHA256() string { return a.planSHA256 }

// Assess verifies all pages and every expected byte, including the identity
// manifest, before re-inspecting the same dataset. prior is nil only when the
// durable creation acknowledgement did not establish a dataset ID; a matching
// manifest is still mandatory. A known ID is never silently replaced.
// Absent/Unknown/Processing return non-ready observations and NEVER a new-create
// permit. The caller must preserve the prewritten M3 one-shot preparation gate.
func (p Plan) Assess(ctx context.Context, source Source, prior *Pin) (Assessment, error) {
	if !p.valid() || source == nil {
		return Assessment{}, ErrPlan
	}
	var expected Pin
	if prior != nil {
		expected = *prior
		if expected.Reference != p.Reference() || !idRE.MatchString(expected.DatasetID) || expected.Version != 1 {
			return Assessment{}, ErrIdentity
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return Assessment{}, err
	}
	before, err := source.Inspect(ctx, p.Reference())
	if err != nil {
		return Assessment{}, observationError(ctx)
	}
	if err = ctx.Err(); err != nil {
		return Assessment{}, err
	}
	// A miss is explicitly inconclusive about a previous creation's acceptance.
	if before.Phase == Absent || before.Phase == Unknown {
		return Assessment{phase: before.Phase}, nil
	}
	pin := Pin{before.Reference, before.DatasetID, before.Version}
	if before.Account != p.account || pin.Reference != p.Reference() || !idRE.MatchString(pin.DatasetID) ||
		pin.Version != 1 || (prior != nil && pin != expected) {
		return Assessment{}, ErrIdentity
	}
	if before.Visibility != Private {
		return Assessment{}, ErrPrivacy
	}
	switch before.Phase {
	case Processing:
		return Assessment{phase: Processing}, nil
	case Failed:
		return Assessment{}, ErrProcessing
	case Ready:
	default:
		return Assessment{phase: Unknown}, nil
	}
	if err = p.checkCatalog(ctx, source, pin); err != nil {
		return Assessment{}, err
	}
	// Verify the small identity manifest before transferring potentially GiB inputs.
	manifest := p.files[len(p.files)-1]
	if err = verifyFile(ctx, source, pin, manifest); err != nil {
		return Assessment{}, err
	}
	for _, f := range p.files[:len(p.files)-1] {
		if err = verifyFile(ctx, source, pin, f); err != nil {
			return Assessment{}, err
		}
	}
	if err = ctx.Err(); err != nil {
		return Assessment{}, err
	}
	after, err := source.Inspect(ctx, p.Reference())
	if err != nil {
		return Assessment{}, observationError(ctx)
	}
	if err = ctx.Err(); err != nil {
		return Assessment{}, err
	}
	if after != before {
		return Assessment{}, ErrChanged
	}
	return Assessment{phase: Ready, pin: pin, planSHA256: p.digest}, nil
}

func observationError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ErrObservation
}
func (p Plan) checkCatalog(ctx context.Context, s Source, pin Pin) error {
	expected := make(map[string]File, len(p.files))
	for _, f := range p.files {
		expected[f.Name] = f
	}
	seen := make(map[string]bool, len(p.files))
	cursors := map[string]bool{"": true}
	cursor := ""
	// Even one file per page must finish; bound empty pages/cursor loops as well.
	for pageNo := 0; pageNo <= len(p.files); pageNo++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, err := s.List(ctx, pin, cursor, 100)
		if err != nil {
			return observationError(ctx)
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if len(page.Files) > 100 || len(page.Files) > len(p.files)-len(seen) {
			return ErrCatalog
		}
		for _, f := range page.Files {
			want, ok := expected[f.Name]
			if !ok || seen[f.Name] || f.Bytes != want.Bytes || f.Kind != "file" {
				return ErrCatalog
			}
			seen[f.Name] = true
		}
		if page.NextCursor == "" {
			if len(seen) != len(expected) {
				return ErrCatalog
			}
			return nil
		}
		if len(page.Files) == 0 || !safeCursor(page.NextCursor) || cursors[page.NextCursor] {
			return ErrCatalog
		}
		cursor = page.NextCursor
		cursors[cursor] = true
	}
	return ErrCatalog
}
func safeCursor(cursor string) bool {
	if len(cursor) > 512 {
		return false
	}
	for _, c := range cursor {
		if c < 33 || c > 126 {
			return false
		}
	}
	return true
}

func verifyFile(ctx context.Context, s Source, pin Pin, f File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, err := s.Open(ctx, pin, f.Name)
	if err != nil || r == nil {
		if r != nil {
			_ = r.Close()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrTransfer
	}
	// LimitReader's extra byte requires an actual EOF after the declared bytes;
	// it detects overrun and also surfaces a late error after the final byte.
	h := sha256.New()
	buffer := make([]byte, 32<<10)
	n, readErr := io.CopyBuffer(h, io.LimitReader(&contextReader{ctx: ctx, r: r}, f.Bytes+1), buffer)
	closeErr := r.Close()
	clear(buffer)
	if err = ctx.Err(); err != nil {
		return err
	}
	if readErr != nil || closeErr != nil || n != f.Bytes || hex.EncodeToString(h.Sum(nil)) != f.SHA256 {
		return ErrTransfer
	}
	return nil
}

type contextReader struct {
	ctx   context.Context
	r     io.Reader
	empty int
}

func (r *contextReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.r.Read(b)
	if n == 0 && err == nil {
		r.empty++
		if r.empty >= 100 {
			return 0, io.ErrNoProgress
		}
	} else {
		r.empty = 0
	}
	return n, err
}
