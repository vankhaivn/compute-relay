package kaggleacceptance

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

func TestAcceptanceMarkerFailureNeverRearmsCommittedSubmission(t *testing.T) {
	f := newAcceptanceFixture(t, fake.Accept)
	f.must("prepare", processA, "prepared-local")
	path := filepath.Join(f.options.Root, "submission-process.json")
	// Force the create-only acknowledgement to fail AFTER the SQL intent commit.
	// This is a local record-I/O fault, not a simulated live provider response.
	if err := os.WriteFile(path, []byte(`{"partial":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.run("submit", processA); err == nil {
		t.Fatal("failed process marker permitted submission")
	}
	if stats := f.backend.Stats(); stats.SubmitCalls != 0 || stats.Executions != 0 || stats.PrepareCalls != 1 {
		t.Fatal("marker failure caused a remote execution", stats)
	}
	o := f.options
	o.Mode = "status"
	s, err := openSession(context.Background(), o, true)
	if err != nil {
		t.Fatal(err)
	}
	j, readErr := s.journal(context.Background())
	closeErr := s.close()
	if readErr != nil || closeErr != nil || !j.SubmitStarted || j.Plan == nil {
		t.Fatal("committed intent was lost after marker failure", readErr, closeErr)
	}
	providers := f.constructed
	f.must("submit", processB, "resume-required")
	if _, err := f.run("resume", processB); err == nil || f.constructed != providers {
		t.Fatal("broken marker triggered a replacement provider path", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != `{"partial":true}` || f.backend.Stats().SubmitCalls != 0 {
		t.Fatal("recovery overwrote evidence or resubmitted", err)
	}
}

func TestAcceptancePublishedResultsDoNotReplaceMissingRestartEvidence(t *testing.T) {
	for _, fault := range []string{"missing", "partial", "same-process", "foreign-plan", "before-submit"} {
		t.Run(fault, func(t *testing.T) {
			f := newAcceptanceFixture(t, fake.Accept)
			f.must("prepare", processA, "prepared-local")
			f.must("submit", processA, "resume-required")
			original := f.must("resume", processB, "passed-offline")
			path := filepath.Join(f.options.Root, "resume-process.json")
			var mark, start submissionMark
			if readRecord(path, &mark) != nil || readRecord(filepath.Join(f.options.Root, "submission-process.json"), &start) != nil {
				t.Fatal("fixture lacks original restart records")
			}
			switch fault {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "partial":
				if err := os.WriteFile(path, []byte(`{"protocol":1}`), 0600); err != nil {
					t.Fatal(err)
				}
			default:
				switch fault {
				case "same-process":
					mark.ProcessNonce = start.ProcessNonce
				case "foreign-plan":
					mark.PlanSHA256 = provider.Digest([]byte("another plan"))
				case "before-submit":
					mark.RecordedAt = start.RecordedAt.Add(-1)
				}
				if err := os.WriteFile(path, mustJSON(t, mark), 0600); err != nil {
					t.Fatal(err)
				}
			}
			providers := f.constructed
			report, _ := f.run("status", processB)
			if report.Evidence == "passed-live" || report.Evidence == "passed-offline" || report.RestartVerified || !report.GPUVerified || report.JobID != original.JobID || report.AttemptID != original.AttemptID || f.constructed != providers {
				t.Fatal("local result verification invented restart evidence", report)
			}
			if stats := f.backend.Stats(); stats.SubmitCalls != 1 || stats.Executions != 1 || stats.CleanupCalls != 0 {
				t.Fatal("evidence rejection caused compute or cleanup", stats)
			}
		})
	}
}
