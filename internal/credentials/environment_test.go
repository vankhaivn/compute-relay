package credentials

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/ports"
)

func TestEnvironmentExplicitReferenceAndRotation(t *testing.T) {
	value := "synthetic-first"
	reads := 0
	refs := []ports.CredentialRef{"env:PROBE_TOKEN"}
	r, err := NewEnvironment(refs, func(name string) (string, bool) {
		reads++
		if name != "PROBE_TOKEN" {
			t.Fatal("unconfigured lookup")
		}
		return value, true
	})
	if err != nil {
		t.Fatal(err)
	}
	refs[0] = "env:OTHER"
	for _, want := range []string{"synthetic-first", "synthetic-rotated"} {
		value = want
		var retained []byte
		err = r.WithCredential(context.Background(), "env:PROBE_TOKEN", func(b []byte) error {
			if string(b) != want {
				t.Fatal("cached credential")
			}
			retained = b
			return nil
		})
		if err != nil || !bytes.Equal(retained, make([]byte, len(want))) {
			t.Fatal("owned buffer not erased", err)
		}
	}
	if err := r.WithCredential(context.Background(), "env:OTHER", func([]byte) error { t.Fatal("callback"); return nil }); !errors.Is(err, ErrUnconfigured) || reads != 2 {
		t.Fatal(err)
	}
}

func TestEnvironmentErasesOnCallbackErrorAndPanic(t *testing.T) {
	for _, panicNow := range []bool{false, true} {
		var retained []byte
		r, _ := NewEnvironment([]ports.CredentialRef{"env:T"}, func(string) (string, bool) { return "synthetic-canary", true })
		sentinel := errors.New("callback failure")
		func() {
			defer func() {
				if p := recover(); (p != nil) != panicNow {
					t.Fatal("unexpected panic", p)
				}
			}()
			err := r.WithCredential(context.Background(), "env:T", func(b []byte) error {
				retained = b
				if panicNow {
					panic(sentinel)
				}
				return sentinel
			})
			if !errors.Is(err, sentinel) {
				t.Fatal(err)
			}
		}()
		if len(retained) == 0 || !bytes.Equal(retained, make([]byte, len(retained))) {
			t.Fatal("buffer survived callback")
		}
	}
}

func TestEnvironmentRejectsInvalidMissingAndCancelled(t *testing.T) {
	lookup := func(string) (string, bool) { return "synthetic", true }
	for _, refs := range [][]ports.CredentialRef{nil, {"env:"}, {"file:/private/token"}, {"env:T", "env:T"}, {"env:T\n"}, {"env:A=B"}} {
		if _, err := NewEnvironment(refs, lookup); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid refs accepted")
		}
	}
	for _, value := range []string{"", strings.Repeat("x", MaxBytes+1)} {
		r, _ := NewEnvironment([]ports.CredentialRef{"env:T"}, func(string) (string, bool) { return value, true })
		if err := r.WithCredential(context.Background(), "env:T", func([]byte) error { t.Fatal("callback"); return nil }); !errors.Is(err, ErrUnavailable) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, _ := NewEnvironment([]ports.CredentialRef{"env:T"}, func(string) (string, bool) { t.Fatal("cancelled lookup"); return "", false })
	if err := r.WithCredential(ctx, "env:T", func([]byte) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestEnvironmentConcurrentBuffersAreIndependent(t *testing.T) {
	r, _ := NewEnvironment([]ports.CredentialRef{"env:T"}, func(string) (string, bool) { return "synthetic", true })
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r.WithCredential(context.Background(), "env:T", func(b []byte) error { b[0] = 'X'; return nil })
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}
