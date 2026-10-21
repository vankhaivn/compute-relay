package sshhost

import (
	"context"
	"errors"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"io"
	"testing"
)

func TestPrepareRetriesCopyIntoSameStoppedContainer(t *testing.T) {
	h := newHarness(t)
	failures := 0
	h.host.fail = func(args []string) (Result, bool) {
		if len(args) > 1 && args[0] == "container" && args[1] == "cp" && failures < 1 {
			failures++
			return Result{ExitCode: 1}, true
		}
		return Result{}, false
	}
	_, prepared := h.prepare(t)
	if !prepared.Ready || h.host.created != 1 || h.host.starts != 0 || failures != 1 {
		t.Fatal("copy retry repeated compute mutation", h.host.created, h.host.starts, failures)
	}
}
func TestObserveDistinguishesAbsentAndChangedEngine(t *testing.T) {
	for _, changed := range []bool{false, true} {
		h := newHarness(t)
		_, prepared := h.prepare(t)
		out := h.adapter.Start(context.Background(), prepared, nil, nil)
		if out.Remote == nil {
			t.Fatal("fixture did not start")
		}
		if changed {
			h.host.engineID = "different-engine"
		} else {
			h.host.containers = map[string]*fakeContainer{}
		}
		_, err := h.adapter.Observe(context.Background(), *out.Remote)
		var p domain.Problem
		if !errors.As(err, &p) {
			t.Fatal("provider failure lost identity", err)
		}
		var absence provider.RemoteAbsence
		if errors.As(err, &absence) == changed {
			t.Fatal("absence proof does not distinguish engine identity", err)
		}
		if !changed && (p.Code != domain.CodeRemoteExecutionUnresolved || p.PublicDetails()[domain.ReasonDetail] != "remote_not_found") {
			t.Fatal("exact absence masked", p)
		}
		if changed && p.Code != domain.CodeRemoteIdentityMismatch {
			t.Fatal("changed engine treated as absence", p)
		}
	}
}

type brokenStagingSession struct {
	session
	broken bool
}

func (s *brokenStagingSession) Run(ctx context.Context, env map[string]string, args []string, stdin io.Reader, stdout io.Writer) (Result, error) {
	if s.broken {
		return Result{}, ErrUnavailable
	}
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "container" && args[i+1] == "cp" && stdin != nil {
			s.broken = true
			return Result{}, ErrUnavailable
		}
	}
	return s.session.Run(ctx, env, args, stdin, stdout)
}
func TestPrepareReconnectsBrokenSessionWithoutRecreatingContainer(t *testing.T) {
	h := newHarness(t)
	dial := h.adapter.dial
	calls := 0
	h.adapter.dial = func(ctx context.Context, target Target, c Credential) (session, error) {
		calls++
		conn, err := dial(ctx, target, c)
		if calls == 1 && err == nil {
			return &brokenStagingSession{session: conn}, nil
		}
		return conn, err
	}
	_, prepared := h.prepare(t)
	if !prepared.Ready || calls != 2 || h.host.created != 1 || h.host.starts != 0 {
		t.Fatal("broken session did not recover the same stopped target", calls, h.host.created, h.host.starts)
	}
}
