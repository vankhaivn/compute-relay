package auth_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/testsupport"
)

var ctx = context.Background()

func fixture(t *testing.T) (*auth.Service, *testsupport.Memory) {
	t.Helper()
	m := testsupport.NewMemory(
		auth.Workspace{ID: "a", Enabled: true, AllowedProfiles: []string{"default-gpu"}},
		auth.Workspace{ID: "b", Enabled: true},
	)
	s, err := auth.New(m, m, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s, m
}

func issue(t *testing.T, s *auth.Service) (auth.Secret, auth.TokenRecord, auth.Principal) {
	t.Helper()
	secret, record, err := s.Issue(ctx, "a", []auth.Scope{auth.Read, auth.Write}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.Authenticate(ctx, secret.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	return secret, record, p
}

func TestIssueStoresOnlyDigestAndRedactsSecret(t *testing.T) {
	s, m := fixture(t)
	secret, record, _ := issue(t, s)
	if len(secret.Reveal()) != 47 || record.Digest != sha256.Sum256([]byte(secret.Reveal())) {
		t.Fatal("invalid token encoding/digest")
	}
	encoded, err := json.Marshal(secret)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded)+fmt.Sprintf("%v %+v %#v %s", secret, secret, secret, secret), secret.Reveal()) {
		t.Fatal("secret leaked through ordinary formatting")
	}
	stored, err := m.LookupToken(ctx, record.Digest)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ = json.Marshal(stored)
	if strings.Contains(string(encoded), secret.Reveal()) {
		t.Fatal("plaintext in repository")
	}
	record.Scopes[0] = auth.Operate
	stored, _ = m.LookupToken(ctx, record.Digest)
	if stored.Scopes[0] != auth.Read {
		t.Fatal("repository scopes aliased to caller")
	}
	second, _, _ := issue(t, s)
	if second.Reveal() == secret.Reveal() {
		t.Fatal("repeated secret")
	}
}

func TestInvalidExpiredRevokedAndDisabledTokens(t *testing.T) {
	s, m := fixture(t)
	secret, record, p := issue(t, s)
	for _, raw := range []string{"", "secret", "Bearer " + secret.Reveal(), secret.Reveal() + " ", strings.Repeat("x", 1000), "cr1_" + strings.Repeat("!", 43), "cr1_" + strings.Repeat("a", 43)} {
		if _, err := s.Authenticate(ctx, raw); !errors.Is(err, auth.ErrUnauthenticated) {
			t.Fatalf("malformed/unknown token: %v", err)
		}
	}
	if err := s.Revoke(ctx, record.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Authenticate(ctx, secret.Reveal()); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("revoked token accepted")
	}
	if _, err := s.Revalidate(ctx, p); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("cached principal bypassed revocation")
	}
	secret, _, _ = issue(t, s)
	m.SetWorkspace(auth.Workspace{ID: "a", Enabled: false})
	if _, err := s.Authenticate(ctx, secret.Reveal()); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("disabled workspace accepted")
	}
	m.SetWorkspace(auth.Workspace{ID: "a", Enabled: true})
	now := time.Now()
	s, _ = auth.New(m, m, func() time.Time { return now })
	secret, _, err := s.Issue(ctx, "a", []auth.Scope{auth.Read}, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if _, err := s.Authenticate(ctx, secret.Reveal()); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("expired token accepted at exact deadline")
	}
	for _, scopes := range [][]auth.Scope{nil, {"admin"}, {auth.Read, auth.Read}} {
		if _, _, err := s.Issue(ctx, "a", scopes, time.Time{}); err == nil {
			t.Fatal("invalid scopes accepted")
		}
	}
	if _, _, err := s.Issue(ctx, "a", []auth.Scope{auth.Read}, now); err == nil {
		t.Fatal("expired issuance accepted")
	}
}

func TestCrossWorkspaceResourceAndProfileMatrix(t *testing.T) {
	s, m := fixture(t)
	_, _, p := issue(t, s)
	for _, kind := range []auth.ResourceKind{auth.Job, auth.Attempt, auth.Object, auth.Artifact, auth.Event, auth.Log, auth.Operation} {
		t.Run(string(kind), func(t *testing.T) {
			m.SetOwner(kind, "own", "a")
			m.SetOwner(kind, "foreign", "b")
			for _, tc := range []struct {
				w    domain.WorkspaceID
				id   string
				want error
			}{{"a", "own", nil}, {"a", "foreign", auth.ErrNotFound}, {"a", "absent", auth.ErrNotFound}, {"b", "foreign", auth.ErrForbidden}, {"a", "../bad", auth.ErrNotFound}} {
				err := auth.RequireResource(ctx, p, tc.w, auth.Read, kind, tc.id, m)
				if !errors.Is(err, tc.want) {
					t.Fatalf("%s/%s: %v, want %v", tc.w, tc.id, err, tc.want)
				}
			}
		})
	}
	if err := auth.Require(p, "a", auth.Operate); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("scope escalation")
	}
	if err := auth.Require(auth.Principal{}, "a", auth.Read); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("zero principal accepted")
	}
	if err := s.RequireProfile(ctx, p, "a", "default-gpu"); err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{"", "unlisted", "../other"} {
		if err := s.RequireProfile(ctx, p, "a", profile); !errors.Is(err, auth.ErrForbidden) {
			t.Fatal("unlisted profile accepted")
		}
	}
	if err := s.RequireProfile(ctx, p, "b", "default-gpu"); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("cross-workspace profile accepted")
	}
}

type badTokens struct{ *testsupport.Memory }

func (b badTokens) LookupToken(context.Context, [32]byte) (auth.TokenRecord, error) {
	return auth.TokenRecord{}, errors.New("PRIVATE secret-canary")
}

func TestRepositoryFailureFailsClosed(t *testing.T) {
	s, m := fixture(t)
	secret, _, _ := issue(t, s)
	s, _ = auth.New(badTokens{m}, m, nil)
	if _, err := s.Authenticate(ctx, secret.Reveal()); !errors.Is(err, auth.ErrUnavailable) || strings.Contains(err.Error(), "canary") {
		t.Fatalf("unsafe repository error: %v", err)
	}
}

func TestConcurrentAuthenticationAndRevocation(t *testing.T) {
	s, _ := fixture(t)
	secret, record, _ := issue(t, s)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Authenticate(ctx, secret.Reveal())
			if err != nil && !errors.Is(err, auth.ErrUnauthenticated) {
				t.Error(err)
			}
		}()
	}
	if err := s.Revoke(ctx, record.ID); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}
