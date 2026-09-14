package objects

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/httpsinput"
)

type inputFetcher func(context.Context, httpsinput.Request, func(context.Context, int64, io.Reader) error) error

func (f inputFetcher) Fetch(ctx context.Context, r httpsinput.Request, consume func(context.Context, int64, io.Reader) error) error {
	return f(ctx, r, consume)
}

// Explicitly nondurable fixtures. They test the consumer contract, not SQLite durability.
type ingestFixture struct {
	token                auth.TokenRecord
	blobs                map[domain.ObjectID][]byte
	rows                 map[domain.ObjectID]domain.ObjectMetadata
	beforeCommit         func()
	commitErr            error
	committedBeforeError bool
}

func (m *ingestFixture) CreateToken(_ context.Context, r auth.TokenRecord) error {
	m.token = r
	return nil
}
func (m *ingestFixture) LookupToken(_ context.Context, digest [32]byte) (auth.TokenRecord, error) {
	if m.token.Digest != digest {
		return auth.TokenRecord{}, auth.ErrNotFound
	}
	return m.token, nil
}
func (m *ingestFixture) RevokeToken(context.Context, string) error {
	m.token.Revoked = true
	return nil
}
func (m *ingestFixture) LookupWorkspace(context.Context, domain.WorkspaceID) (auth.Workspace, error) {
	return auth.Workspace{ID: "a", Enabled: true}, nil
}
func (m *ingestFixture) Put(ctx context.Context, meta domain.ObjectMetadata, body io.Reader) (domain.ObjectMetadata, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return domain.ObjectMetadata{}, err
	}
	if err := ctx.Err(); err != nil {
		return domain.ObjectMetadata{}, err
	}
	digest := sha256.Sum256(data)
	if meta.Bytes >= 0 && meta.Bytes != int64(len(data)) {
		return domain.ObjectMetadata{}, ErrInvalid
	}
	if meta.SHA256 != "" && string(meta.SHA256) != hex.EncodeToString(digest[:]) {
		return domain.ObjectMetadata{}, httpsinput.ErrDigest
	}
	meta.Bytes = int64(len(data))
	meta.SHA256 = domain.SHA256Digest(hex.EncodeToString(digest[:]))
	m.blobs[meta.ID] = data
	if m.beforeCommit != nil {
		m.beforeCommit()
	}
	return meta, nil
}
func (m *ingestFixture) CommitObject(_ context.Context, meta domain.ObjectMetadata) error {
	if m.commitErr == nil || m.committedBeforeError {
		m.rows[meta.ID] = meta
	}
	return m.commitErr
}
func (m *ingestFixture) GetObject(_ context.Context, w domain.WorkspaceID, id domain.ObjectID) (domain.ObjectMetadata, error) {
	meta, ok := m.rows[id]
	if !ok || meta.WorkspaceID != w {
		return domain.ObjectMetadata{}, ErrNotFound
	}
	return meta, nil
}
func ingestSetup(t *testing.T) (*Service, *auth.Service, auth.Principal, *ingestFixture) {
	t.Helper()
	f := &ingestFixture{blobs: map[domain.ObjectID][]byte{}, rows: map[domain.ObjectID]domain.ObjectMetadata{}}
	a, err := auth.New(f, f, nil)
	if err != nil {
		t.Fatal(err)
	}
	secret, _, err := a.Issue(context.Background(), "a", []auth.Scope{auth.Read, auth.Write}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := a.Authenticate(context.Background(), secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(a, f, f)
	if err != nil {
		t.Fatal(err)
	}
	return s, a, p, f
}

func TestHTTPSIngestAuthorizationBeforeNetwork(t *testing.T) {
	for _, mode := range []string{"anonymous", "workspace", "scope", "revoked", "invalid-url", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			s, _, p, f := ingestSetup(t)
			calls := 0
			i, _ := NewIngestor(s, inputFetcher(func(context.Context, httpsinput.Request, func(context.Context, int64, io.Reader) error) error {
				calls++
				return nil
			}))
			r := httpsinput.Request{URL: "https://example.com/input"}
			w := domain.WorkspaceID("a")
			ctx := context.Background()
			switch mode {
			case "anonymous":
				p = auth.Principal{}
			case "workspace":
				w = "b"
			case "scope":
				f.token.Scopes = []auth.Scope{auth.Read}
			case "revoked":
				f.token.Revoked = true
			case "invalid-url":
				r.URL = "http://example.com/input"
			case "cancelled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			}
			if _, err := i.Ingest(ctx, p, w, r); err == nil || calls != 0 || len(f.rows) != 0 {
				t.Fatalf("network before authorization: %v calls=%d", err, calls)
			}
		})
	}
}

func TestHTTPSIngestNewSnapshotDoesNotMutateOldBytes(t *testing.T) {
	s, _, p, f := ingestSetup(t)
	payload := "original"
	calls := 0
	i, _ := NewIngestor(s, inputFetcher(func(ctx context.Context, r httpsinput.Request, consume func(context.Context, int64, io.Reader) error) error {
		calls++
		return consume(ctx, -1, strings.NewReader(payload))
	}))
	r := httpsinput.Request{URL: "https://example.com/mutable?q=private-canary"}
	first, err := i.Ingest(context.Background(), p, "a", r)
	if err != nil {
		t.Fatal(err)
	}
	payload = "changed"
	second, err := i.Ingest(context.Background(), p, "a", r)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || first.SHA256 == second.SHA256 || string(f.blobs[first.ID]) != "original" || len(f.rows) != 2 || calls != 2 {
		t.Fatal("snapshot identity rewritten")
	}
	if got, err := s.Get(context.Background(), p, "a", first.ID); err != nil || got != first || calls != 2 {
		t.Fatal("read fetched mutable URL again")
	}
}

type failedInput struct{ failure error }

func (f failedInput) Read([]byte) (int, error) { return 0, f.failure }

func TestHTTPSIngestFailedTransfersNeverCommit(t *testing.T) {
	for _, failure := range []error{httpsinput.ErrFetch, httpsinput.ErrDigest, httpsinput.ErrLimit, httpsinput.ErrTimeout, context.Canceled} {
		t.Run(failure.Error(), func(t *testing.T) {
			s, _, p, f := ingestSetup(t)
			i, _ := NewIngestor(s, inputFetcher(func(ctx context.Context, _ httpsinput.Request, consume func(context.Context, int64, io.Reader) error) error {
				return consume(ctx, -1, io.MultiReader(strings.NewReader("partial"), failedInput{failure}))
			}))
			meta, err := i.Ingest(context.Background(), p, "a", httpsinput.Request{URL: "https://example.com/data"})
			if !errors.Is(err, failure) || meta.Valid() || len(f.rows) != 0 || len(f.blobs) != 0 {
				t.Fatal("failed transfer published", err)
			}
		})
	}
}

func TestHTTPSIngestRevalidatesAndPreservesUncertainCommit(t *testing.T) {
	for _, mode := range []string{"revoked", "commit-before-failure", "commit-failure"} {
		t.Run(mode, func(t *testing.T) {
			s, _, p, f := ingestSetup(t)
			if mode == "revoked" {
				f.beforeCommit = func() { f.token.Revoked = true }
			} else {
				f.commitErr = errors.New("private-backend-canary")
				f.committedBeforeError = mode == "commit-before-failure"
			}
			i, _ := NewIngestor(s, inputFetcher(func(ctx context.Context, _ httpsinput.Request, consume func(context.Context, int64, io.Reader) error) error {
				return consume(ctx, 7, strings.NewReader("payload"))
			}))
			meta, err := i.Ingest(context.Background(), p, "a", httpsinput.Request{URL: "https://example.com/data"})
			if err == nil || meta.Valid() || len(f.blobs) != 1 || strings.Contains(err.Error(), "canary") {
				t.Fatal("unsafe commit outcome", err)
			}
			want := 0
			if mode == "commit-before-failure" {
				want = 1
			}
			if len(f.rows) != want {
				t.Fatal("incorrect metadata publication")
			}
		})
	}
}

func TestHTTPSIngestRejectsMissingAndRepeatedConsumer(t *testing.T) {
	s, _, p, f := ingestSetup(t)
	if _, err := NewIngestor(nil, nil); err == nil {
		t.Fatal("nil dependencies")
	}
	if _, err := NewIngestor(s, nil); err == nil {
		t.Fatal("nil fetcher")
	}
	i, _ := NewIngestor(s, inputFetcher(func(context.Context, httpsinput.Request, func(context.Context, int64, io.Reader) error) error {
		return nil
	}))
	r := httpsinput.Request{URL: "https://example.com/data"}
	if _, err := i.Ingest(context.Background(), p, "a", r); !errors.Is(err, ErrUnavailable) {
		t.Fatal("missing consumer accepted", err)
	}
	i, _ = NewIngestor(s, inputFetcher(func(ctx context.Context, _ httpsinput.Request, consume func(context.Context, int64, io.Reader) error) error {
		if err := consume(ctx, 1, strings.NewReader("a")); err != nil {
			return err
		}
		return consume(ctx, 1, strings.NewReader("b"))
	}))
	if _, err := i.Ingest(context.Background(), p, "a", r); !errors.Is(err, ErrUnavailable) || len(f.rows) != 1 {
		t.Fatal("repeated consumer admitted", err)
	}
}
