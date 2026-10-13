package cleanup

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

func TestParseIsStrictAndCanonical(t *testing.T) {
	for _, raw := range []string{
		`{"attempt_id":"att_1"}`,
		`{"attempt_id":"att_1","discard_uncollected":false}`,
		`{"attempt_id":"att_1","accept_name_based_delete":false}`,
		`{"reason":"","discard_uncollected":false,"attempt_id":"att_1"}`,
	} {
		r, err := Parse([]byte(raw))
		if err != nil || r != (Request{AttemptID: "att_1"}) {
			t.Fatal("equivalent request parsed differently", raw, r, err)
		}
	}
	r, err := Parse([]byte(`{"attempt_id":"att_1","discard_uncollected":true,"reason":"operator confirmed"}`))
	if err != nil || !r.DiscardUncollected || r.Reason != "operator confirmed" {
		t.Fatal("explicit discard lost", r, err)
	}
	for _, raw := range []string{
		``, `null`, `[]`, `{}`, `{"attempt_id":null}`, `{"attempt_id":1}`, `{"attempt_id":""}`,
		`{"attempt_id":"att_1","attempt_id":"att_2"}`, `{"attempt_id":"att_1","discard_uncollected":"true"}`,
		`{"attempt_id":"att_1","discard_uncollected":null}`, `{"attempt_id":"att_1","discard_uncollected":1}`,
		`{"attempt_id":"att_1","accept_name_based_delete":null}`, `{"attempt_id":"att_1","accept_name_based_delete":"true"}`, `{"attempt_id":"att_1","accept_name_based_delete":true,"accept_name_based_delete":false}`,
		`{"attempt_id":"att_1","reason":null}`, `{"attempt_id":"att_1","reason":"line\nbreak"}`,
		`{"attempt_id":"att_1","unknown":true}`, `{"attempt_id":"att_1"}{}`, `{"attempt_id":"att_1"} x`,
		`{"attempt_id":"att_1","reason":"` + strings.Repeat("r", 513) + `"}`, "{\"attempt_id\":\"att_1\",\"reason\":\"\xff\"}",
	} {
		if _, err := Parse([]byte(raw)); !errors.Is(err, operations.ErrRequest) {
			t.Fatal("invalid cleanup request accepted", raw, err)
		}
	}
}

func TestDigestBindsJobAndEveryField(t *testing.T) {
	base := Request{AttemptID: "att_1"}
	seen := map[string]bool{}
	for _, c := range []struct {
		job domain.JobID
		r   Request
	}{
		{"job_1", base}, {"job_2", base}, {"job_1", Request{AttemptID: "att_2"}},
		{"job_1", Request{AttemptID: "att_1", DiscardUncollected: true}}, {"job_1", Request{AttemptID: "att_1", Reason: "why"}}, {"job_1", Request{AttemptID: "att_1", AcceptNameBasedDelete: true}},
	} {
		digest, err := c.r.Digest(c.job)
		if err != nil || len(digest) != 64 || seen[digest] {
			t.Fatal("digest is not distinct", c, err)
		}
		seen[digest] = true
	}
	if _, err := base.Digest(""); !errors.Is(err, operations.ErrRequest) {
		t.Fatal("digest accepted an invalid job", err)
	}
}

func TestRecordStatusEffectMatrix(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	op, err := domain.NewOperation("op_1", "a", "job_1", "att_1", domain.OperationCleanup, now)
	if err != nil {
		t.Fatal(err)
	}
	failure, err := domain.NewProblem(domain.CodeIllegalStateTransition, "not eligible", domain.FailureStageOperation)
	if err != nil {
		t.Fatal(err)
	}
	at := func(status domain.OperationStatus) domain.Operation {
		var p *domain.Problem
		if status == domain.OperationFailed || status == domain.OperationManualRequired {
			p = &failure
		}
		next, err := op.Transition(status, p, now)
		if err != nil {
			t.Fatal(err)
		}
		return next
	}
	valid := map[domain.OperationStatus][]Effect{
		domain.OperationAccepted:  {Pending},
		domain.OperationRunning:   {Pending},
		domain.OperationSucceeded: {RemoteDeleted, AlreadyAbsent},
		domain.OperationFailed:    {Failed},
	}
	for _, status := range []domain.OperationStatus{domain.OperationAccepted, domain.OperationRunning, domain.OperationSucceeded, domain.OperationFailed, domain.OperationManualRequired} {
		current := op
		if status != domain.OperationAccepted {
			current = at(status)
		}
		for _, effect := range []Effect{Pending, RemoteDeleted, AlreadyAbsent, Failed, "deleted"} {
			want := false
			for _, ok := range valid[status] {
				want = want || ok == effect
			}
			if got := (Record{Operation: current, Effect: effect}).Validate() == nil; got != want {
				t.Fatal("status/effect matrix mismatch", status, effect, got)
			}
		}
	}
	collect := op
	collect.Kind = domain.OperationCollect
	if (Record{Operation: collect, Effect: Pending}).Validate() == nil {
		t.Fatal("non-cleanup operation accepted")
	}
	orphan := op
	orphan.AttemptID = ""
	if (Record{Operation: orphan, Effect: Pending}).Validate() == nil {
		t.Fatal("cleanup without an attempt accepted")
	}
}

func TestNameBasedAcceptancePreservesLegacyDigest(t *testing.T) {
	r, err := Parse([]byte(`{"attempt_id":"att_1","accept_name_based_delete":true}`))
	if err != nil || !r.AcceptNameBasedDelete {
		t.Fatal(r, err)
	}
	old := []byte(`{"version":"cleanup/v1","job_id":"job_1","kind":"cleanup","request":{"attempt_id":"att_1","discard_uncollected":false}}`)
	digest, err := (Request{AttemptID: "att_1"}).Digest("job_1")
	if err != nil || digest != string(provider.Digest(old)) {
		t.Fatal("old persisted hash changed", digest, err)
	}
}
