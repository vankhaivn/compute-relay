package operations

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

func TestExplicitTargetAndCanonicalControls(t *testing.T) {
	for _, kind := range []domain.OperationKind{domain.OperationCancel, domain.OperationRetryCompute, domain.OperationReconcile, domain.OperationCollect} {
		valid := []byte(`{"attempt_id":"attempt_1","reason":"operator request"}`)
		r, err := Parse(kind, valid)
		if err != nil {
			t.Fatal(kind, err)
		}
		hash, err := r.Digest("job_1", kind)
		if err != nil {
			t.Fatal(err)
		}
		reordered, err := Parse(kind, []byte(` {"reason":"operator request", "attempt_id":"attempt_1"} `))
		if err != nil {
			t.Fatal(err)
		}
		same, _ := reordered.Digest("job_1", kind)
		other, _ := reordered.Digest("job_2", kind)
		if same != hash || other == hash {
			t.Fatal("unsafe canonical identity")
		}
		for _, raw := range []string{``, `null`, `{}`, `[]`, `{"reason":"operator request"}`, `{"attempt_id":null}`, `{"attempt_id":123}`, `{"attempt_id":"a","attempt_id":"a"}`, `{"attempt_id":"a","reason":null}`, `{"attempt_id":"a","unknown":true}`, `{"attempt_id":"a"} {}`, `{"attempt_id":"a","reason":"secret\nvalue"}`, `{"attempt_id":"a","reason":"` + strings.Repeat("x", 513) + `"}`} {
			if _, err = Parse(kind, []byte(raw)); !errors.Is(err, ErrRequest) {
				t.Fatalf("%s accepted %q: %v", kind, raw, err)
			}
		}
	}
	for _, kind := range []domain.OperationKind{domain.OperationCancel, domain.OperationReconcile, domain.OperationCollect} {
		a, err := Parse(kind, []byte(`{"attempt_id":"a"}`))
		if err != nil {
			t.Fatal(err)
		}
		b, _ := Parse(kind, []byte(`{"attempt_id":"a","reason":""}`))
		ha, _ := a.Digest("j", kind)
		hb, _ := b.Digest("j", kind)
		if ha != hb {
			t.Fatal("omitted reason drift")
		}
	}
	if _, err := Parse(domain.OperationRetryCompute, []byte(`{"attempt_id":"a","reason":"  "}`)); err == nil {
		t.Fatal("retry reason required")
	}
	if _, err := Parse(domain.OperationCleanup, []byte(`{"attempt_id":"a"}`)); err == nil {
		t.Fatal("cleanup entered job controls")
	}
}

func TestRetryRefusesUnresolvedAndSuccessfulCompute(t *testing.T) {
	s := domain.InitialAttemptState()
	if !errors.Is(RetryAllowed(s), ErrUnresolved) {
		t.Fatal("queued attempt retried")
	}
	s.Orchestration = domain.OrchestrationCancelled
	s.Cancellation = domain.CancellationPrevented
	s.ReleaseEvidence = domain.ReleaseEvidenceNotApplicable
	if err := RetryAllowed(s); err != nil {
		t.Fatal(err)
	}
	s = domain.InitialAttemptState()
	s.Orchestration = domain.OrchestrationCollecting
	s.Execution = domain.ExecutionSucceeded
	s.RemoteActivity = domain.RemoteActivityInactive
	s.ReleaseEvidence = domain.ReleaseEvidenceNotObservable
	if !errors.Is(RetryAllowed(s), ErrCollect) {
		t.Fatal("successful compute used for recovery")
	}
	s = domain.InitialAttemptState()
	s.Orchestration = domain.OrchestrationReconciling
	s.Execution = domain.ExecutionUnknown
	s.RemoteActivity = domain.RemoteActivityPossible
	if !errors.Is(RetryAllowed(s), ErrUnresolved) {
		t.Fatal("ambiguous execution retried")
	}
}

type readFixture struct {
	data      []byte
	err       error
	nilReader bool
}

func (b readFixture) Open(context.Context, domain.WorkspaceID, domain.ObjectID) (io.ReadCloser, error) {
	if b.nilReader {
		return nil, b.err
	}
	return io.NopCloser(bytes.NewReader(b.data)), b.err
}
func TestVerifyOriginalBytesRejectsEveryUnavailableOutcome(t *testing.T) {
	m := domain.ObjectMetadata{WorkspaceID: "w", ID: "o", Bytes: 3, SHA256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}
	for _, fixture := range []readFixture{{data: []byte("ab")}, {data: []byte("abcd")}, {data: []byte("xyz")}, {err: errors.New("private-path-canary")}, {nilReader: true}} {
		if err := verifyBlob(context.Background(), fixture, m); !errors.Is(err, ErrInputs) {
			t.Fatal("unsafe blob accepted", err)
		}
	}
	if err := verifyBlob(context.Background(), readFixture{data: []byte("abc")}, m); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(verifyBlob(ctx, readFixture{}, m), context.Canceled) {
		t.Fatal("lost cancellation")
	}
}

func TestOperationEffectsCannotInventTerminationOrRetryIdentity(t *testing.T) {
	op, err := domain.NewOperation("op", "w", "j", "a", domain.OperationCancel, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	r := Record{Operation: op, Effect: Pending}
	if r.Validate() != nil {
		t.Fatal("valid acceptance")
	}
	r.TerminationConfirmed = true
	if r.Validate() == nil {
		t.Fatal("acceptance confirmed termination")
	}
	r.TerminationConfirmed = false
	r.Effect = NewAttemptCreated
	r.NewAttemptID = "other"
	if r.Validate() == nil {
		t.Fatal("cancel created a compute attempt")
	}
}
