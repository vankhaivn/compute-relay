package httpsinput

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
)

type noProgress struct{}

func (noProgress) Read([]byte) (int, error) { return 0, nil }

type badReader struct{}

func (badReader) Read([]byte) (int, error) { return 0, errors.New("private-url-query-canary") }

func TestVerifiedEOFAndStickyFailure(t *testing.T) {
	for _, tc := range []struct {
		source        io.Reader
		max, declared int64
		expected      string
		want          error
	}{
		{strings.NewReader("12345"), 4, -1, "", ErrLimit},
		{strings.NewReader("payload"), 32, 7, strings.Repeat("a", 64), ErrDigest},
		{strings.NewReader("short"), 32, 9, "", ErrFetch},
		{noProgress{}, 32, -1, "", ErrFetch}, {badReader{}, 32, -1, "", ErrFetch},
	} {
		r := &verifiedReader{ctx: context.Background(), source: tc.source, max: tc.max, declared: tc.declared, expected: tc.expected, hash: sha256.New()}
		_, err := io.Copy(io.Discard, r)
		if !errors.Is(err, tc.want) || r.done {
			t.Fatal("failure became EOF", err)
		}
		for i := 0; i < 3; i++ {
			if _, err := r.Read(make([]byte, 1)); !errors.Is(err, tc.want) {
				t.Fatal("error not sticky", err)
			}
		}
	}
	r := &verifiedReader{ctx: context.Background(), source: strings.NewReader(""), max: 1, declared: 0, hash: sha256.New()}
	if _, err := io.Copy(io.Discard, r); err != nil || !r.done {
		t.Fatal("empty valid input", err)
	}
}

func TestSensitiveResolverErrorsAreNotReturned(t *testing.T) {
	c, _ := New(DefaultConfig())
	c.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return nil, errors.New("private-url-query-canary")
	}
	err := c.Fetch(context.Background(), Request{URL: "https://example.com/file?q=query-canary"}, drain)
	if err != ErrFetch {
		t.Fatal("raw error escaped", err)
	}
}
