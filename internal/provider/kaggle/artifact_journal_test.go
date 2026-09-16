package kaggle

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/auth"
	"github.com/vankhaivn/compute-relay/internal/blobfs"
	"github.com/vankhaivn/compute-relay/internal/collection"
	"github.com/vankhaivn/compute-relay/internal/domain"
	"github.com/vankhaivn/compute-relay/internal/operations"
	"github.com/vankhaivn/compute-relay/internal/provider"
	"github.com/vankhaivn/compute-relay/internal/provider/fake"
)

// Only tests combine the real artifact port with the fixture capability surface.
// The real collection engine/SQLite/blobs are used; SDK HTTP fixtures are separate.
type artifactJournalAdapter struct {
	*fake.Bound
	reader *ArtifactReader
	pages  int
}

func (p *artifactJournalAdapter) ListArtifacts(ctx context.Context, ref provider.RemoteReference, page provider.PageRequest) (provider.ArtifactPage, error) {
	p.pages++
	page.Limit = 2 // Exercise the real snapshot cursor through the collector.
	return p.reader.ListArtifacts(ctx, ref, page)
}
func (p *artifactJournalAdapter) FetchArtifact(ctx context.Context, ref provider.RemoteReference, file provider.Artifact, dst io.Writer, limit int64) (provider.TransferResult, error) {
	return p.reader.FetchArtifact(ctx, ref, file, dst, limit)
}

type artifactJournalRepository struct {
	collection.Repository
	lose string
}

var errArtifactAck = errors.New("synthetic collection acknowledgement loss")

func (r *artifactJournalRepository) PinCollection(ctx context.Context, w collection.Work, s collection.Snapshot, now time.Time) error {
	err := r.Repository.PinCollection(ctx, w, s, now)
	if err == nil && r.lose == "pin" {
		r.lose = ""
		return errArtifactAck
	}
	return err
}
func (r *artifactJournalRepository) CompleteCollection(ctx context.Context, w collection.Work, v collection.Verified, now time.Time) error {
	err := r.Repository.CompleteCollection(ctx, w, v, now)
	if err == nil && r.lose == "publication" {
		r.lose = ""
		return errArtifactAck
	}
	return err
}

type artifactJournalFixture struct {
	x       *executionJournalFixture
	results *blobfs.Store
	repo    *artifactJournalRepository
	adapter *artifactJournalAdapter
	engine  *collection.Engine
	files   map[string][]byte
	fault   string
	lists   int
	fetches map[string]int
}

func newArtifactJournalFixture(t *testing.T, payload []byte) *artifactJournalFixture {
	t.Helper()
	x := newExecutionJournalFixture(t)
	x.step(t, false)
	x.raw = "COMPLETE"
	x.step(t, false)
	ref := *x.f.journal(t).Remote
	id := ref.Identity
	manifest := map[string]any{
		"manifest_version": "1", "runner_version": "offline-fixture",
		"job_id": id.JobID, "attempt_id": id.AttemptID, "attempt_nonce": id.Nonce,
		"bundle_sha256": id.BundleSHA256, "input_manifest_sha256": id.InputManifestSHA256,
		"started_at":  x.f.clock.Now().Add(-time.Second).Format(time.RFC3339),
		"finished_at": x.f.clock.Now().Format(time.RFC3339),
		"phase":       "completed", "exit_code": 0, "timed_out": false, "error": nil,
		"resource_check": map[string]bool{"gpu_required": false, "gpu_verified": false},
		"artifacts":      []any{map[string]any{"path": "answer.json", "bytes": len(payload), "sha256": provider.Digest(payload)}},
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	results, err := blobfs.New(filepath.Join(t.TempDir(), "results"), blobfs.Limits{MaxObjectBytes: 32 << 20, MaxTotalBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = results.Close() })
	f := &artifactJournalFixture{x: x, results: results, fetches: map[string]int{}, files: map[string][]byte{
		artifactManifest: raw, "outputs/answer.json": payload,
		"control/stdout.log": []byte("synthetic stdout\n"), "control/stderr.log": {},
		"control/environment.json": []byte(`{"fixture":true}`),
	}}
	f.attach(t)
	return f
}
func (f *artifactJournalFixture) attach(t *testing.T) {
	t.Helper()
	reader, err := NewArtifactReader(f.x.executor, DefaultArtifactPolicy())
	if err != nil {
		t.Fatal(err)
	}
	reader.run = func(ctx context.Context, c Config, mode string, token []byte, request artifactRequest, dst io.Writer) ([]byte, error) {
		if c != f.x.f.config || string(token) != "SYNTHETIC_TOKEN" || request.Execution.Source != "" || request.Identity.AttemptID != f.x.f.receipt.AttemptID {
			return nil, errors.New("artifact helper lost the frozen binding")
		}
		// Independently visible committed ownership, not a return-value assertion.
		f.x.f.inspect(t, func(db *sql.DB) {
			var n int
			if err := db.QueryRowContext(ctx, "SELECT count(*) FROM collection_leases WHERE held=1").Scan(&n); err != nil || n != 1 {
				t.Fatal("helper entered without committed collection lease", n, err)
			}
			if request.Target != nil && strings.HasPrefix(request.Target.Path, "outputs/") {
				if err := db.QueryRowContext(ctx, "SELECT count(*) FROM collection_snapshots").Scan(&n); err != nil || n != 1 {
					t.Fatal("payload preceded the immutable result pin", n, err)
				}
			}
		})
		if mode == "catalog" {
			f.lists++
			return artifactCatalog(f.files), nil
		}
		if request.Target == nil {
			return nil, errors.New("missing pinned transfer target")
		}
		path := request.Target.Path
		f.fetches[path]++
		data := f.files[path]
		if strings.HasPrefix(path, "outputs/") {
			if f.fault == "partial" {
				_, _ = dst.Write(data[:len(data)/2])
				return nil, errors.New("SYNTHETIC_TOKEN partial transfer")
			}
			if f.fault == "wrong" {
				data = bytes.Repeat([]byte{'x'}, len(data))
			}
		}
		if _, err := io.Copy(dst, bytes.NewReader(data)); err != nil {
			return nil, err
		}
		if f.fault == "late" && strings.HasPrefix(path, "outputs/") {
			return nil, errors.New("SYNTHETIC_TOKEN after final byte")
		}
		return nil, nil
	}
	binding := provider.BindingSnapshot{Binding: f.x.f.profile.Binding, AccountScope: f.x.f.profile.AccountScope, CredentialRef: f.x.f.profile.CredentialRef}
	backend, err := fake.NewBackend(fake.DefaultScenario())
	if err != nil {
		t.Fatal(err)
	}
	bound, err := fake.NewBound(backend, f.x.f.clock, binding)
	if err != nil {
		t.Fatal(err)
	}
	f.adapter = &artifactJournalAdapter{Bound: bound, reader: reader}
	registry := provider.NewSnapshotRegistry()
	if err := registry.Register(binding, f.adapter); err != nil {
		t.Fatal(err)
	}
	f.repo = &artifactJournalRepository{Repository: f.x.f.store}
	f.engine, err = collection.New(f.repo, registry, f.results, f.x.f.clock, collection.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
}
func (f *artifactJournalFixture) restart(t *testing.T) {
	t.Helper()
	f.x.restart(t)
	f.x.f.clock.advance()
	f.x.f.clock.advance() // Past the old collection lease, not evidence of remote state.
	f.attach(t)
}
func (f *artifactJournalFixture) run(t *testing.T, failure bool) {
	t.Helper()
	worked, err := f.engine.RunOnce(context.Background())
	if !worked || (err != nil) != failure {
		t.Fatal("unexpected collection outcome", worked, err, failure)
	}
}
func (f *artifactJournalFixture) count(t *testing.T, table string) int {
	t.Helper()
	var n int
	f.x.f.inspect(t, func(db *sql.DB) {
		if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
	})
	return n
}
func (f *artifactJournalFixture) pin(t *testing.T) collection.Snapshot {
	t.Helper()
	var snapshot collection.Snapshot
	f.x.f.inspect(t, func(db *sql.DB) {
		var raw, digest string
		if err := db.QueryRow("SELECT snapshot,snapshot_sha256 FROM collection_snapshots").Scan(&raw, &digest); err != nil {
			t.Fatal(err)
		}
		if json.Unmarshal([]byte(raw), &snapshot) != nil || string(snapshot.Digest()) != digest {
			t.Fatal("persisted pin identity invalid")
		}
	})
	return snapshot
}
func (f *artifactJournalFixture) noNewCompute(t *testing.T) {
	t.Helper()
	if f.x.saves != 1 || f.x.f.creates != 1 || f.count(t, "attempts") != 1 || f.count(t, "submission_intents") != 1 {
		t.Fatal("artifact recovery created another compute attempt")
	}
}

func TestArtifactsM3PublicationUsesScopedVerifiedBytesAndOriginalReceipt(t *testing.T) {
	ctx := context.Background()
	f := newArtifactJournalFixture(t, []byte("verified answer\n"))
	service, actor := operationalService(t, f.x.f)
	raw, _ := json.Marshal(map[string]any{"attempt_id": f.x.f.receipt.AttemptID})
	original, err := service.Submit(ctx, actor, "workspace", f.x.f.receipt.JobID, domain.OperationCollect, "m405-original", raw)
	if err != nil {
		t.Fatal(err)
	}
	changed := f.x.f.profile
	changed.Binding.ConfigurationRevision, changed.AccountScope = "remapped", "foreign_account"
	if err := f.x.f.store.PutProfile(ctx, changed, false); err != nil {
		t.Fatal(err)
	}
	f.run(t, false)
	if f.adapter.pages != 3 || f.lists != 1 || f.count(t, "artifacts") != 5 || f.count(t, "collection_publications") != 1 {
		t.Fatal("incomplete pagination/publication")
	}
	current, err := service.Get(ctx, actor, "workspace", original.Operation.ID)
	if err != nil || current.Effect != operations.ResultsAvailable || current.Operation.Status != domain.OperationSucceeded {
		t.Fatal("collection operation not completed", err)
	}
	replay, err := service.Submit(ctx, actor, "workspace", f.x.f.receipt.JobID, domain.OperationCollect, "m405-original", raw)
	if err != nil || !replay.Replay || !reflect.DeepEqual(replay.Operation, original.Operation) || replay.Effect != original.Effect {
		t.Fatal("original receipt was rewritten", err)
	}
	access, _ := auth.New(f.x.f.store, f.x.f.store, nil)
	reader, err := collection.NewReader(access, f.x.f.store, f.results)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reader.Read(ctx, actor, "workspace", f.x.f.receipt.JobID, f.x.f.receipt.AttemptID)
	if err != nil || len(result.Files) != 5 || result.Phase != "completed" {
		t.Fatal("missing verified publication", err)
	}
	for _, file := range result.Files {
		meta, stream, err := reader.Open(ctx, actor, "workspace", f.x.f.receipt.JobID, f.x.f.receipt.AttemptID, file.ID)
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(stream)
		closeErr := stream.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(data, f.files[file.Path]) || provider.Digest(data) != meta.Object.SHA256 {
			t.Fatal("published bytes differ from verified snapshot")
		}
	}
	if _, err := reader.Read(ctx, actor, "foreign", f.x.f.receipt.JobID, f.x.f.receipt.AttemptID); !errors.Is(err, auth.ErrForbidden) {
		t.Fatal("foreign workspace exposed artifacts", err)
	}
	if _, _, err := reader.Open(ctx, actor, "workspace", f.x.f.receipt.JobID, "foreign", result.Files[0].ID); err == nil {
		t.Fatal("foreign attempt exposed artifacts")
	}
	if err := f.x.f.store.RevokeToken(ctx, actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(ctx, actor, "workspace", f.x.f.receipt.JobID, f.x.f.receipt.AttemptID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("revoked credential exposed artifacts", err)
	}
	attempt, err := f.x.f.store.LoadAttempt(ctx, "workspace", f.x.f.receipt.JobID, f.x.f.receipt.AttemptID)
	if err != nil || attempt.State.Result != domain.ResultAvailable || attempt.State.Orchestration != domain.OrchestrationSucceeded || attempt.State.ReleaseEvidence != domain.ReleaseEvidenceNotObservable {
		t.Fatal("publication changed execution/release evidence", err)
	}
	f.noNewCompute(t)
}

func TestArtifactsM3PartialAndLateTransferRecoverOnlyTheOriginalPin(t *testing.T) {
	for _, fault := range []string{"partial", "late", "wrong"} {
		t.Run(fault, func(t *testing.T) {
			f := newArtifactJournalFixture(t, bytes.Repeat([]byte("original"), 2<<20)) // 16 MiB
			f.fault = fault
			f.run(t, true)
			pin := f.pin(t)
			if f.count(t, "artifacts") != 0 || f.count(t, "collection_publications") != 0 {
				t.Fatal("failed bytes became visible")
			}
			for _, file := range pin.Files {
				if strings.HasPrefix(file.Path, "outputs/") {
					stream, err := f.results.Open(context.Background(), "workspace", file.Object.ID)
					if err == nil {
						stream.Close()
						t.Fatal("failed payload received successful blob EOF")
					}
				}
			}
			f.restart(t)
			service, actor := operationalService(t, f.x.f)
			raw, _ := json.Marshal(map[string]any{"attempt_id": f.x.f.receipt.AttemptID})
			if _, err := service.Submit(context.Background(), actor, "workspace", f.x.f.receipt.JobID, domain.OperationCollect, "m405-explicit-recovery", raw); err != nil {
				t.Fatal(err)
			}
			f.fault = ""
			f.run(t, false)
			if f.pin(t).Digest() != pin.Digest() || f.lists != 1 || f.fetches["outputs/answer.json"] != 2 || f.count(t, "collection_publications") != 1 {
				t.Fatal("recovery replaced the pin or rediscovered newer results")
			}
			for _, file := range pin.Files {
				stream, err := f.results.Open(context.Background(), "workspace", file.Object.ID)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(stream)
				closeErr := stream.Close()
				if err != nil || closeErr != nil || !bytes.Equal(data, f.files[file.Path]) {
					t.Fatal("recovered bytes differ from original pin")
				}
			}
			f.noNewCompute(t)
		})
	}
}

func TestArtifactsM3LostPinAndPublicationAcknowledgementsAreRecoverable(t *testing.T) {
	for _, phase := range []string{"pin", "publication"} {
		t.Run(phase, func(t *testing.T) {
			f := newArtifactJournalFixture(t, []byte("original answer"))
			f.repo.lose = phase
			f.run(t, true)
			pin := f.pin(t)
			events := f.count(t, "events")
			reads := f.fetches["outputs/answer.json"]
			f.restart(t)
			if phase == "pin" {
				f.run(t, false)
			} else {
				worked, err := f.engine.RunOnce(context.Background())
				if err != nil || worked || f.count(t, "events") != events || f.fetches["outputs/answer.json"] != reads {
					t.Fatal("lost publication acknowledgement duplicated work/events", err)
				}
			}
			if f.pin(t).Digest() != pin.Digest() || f.lists != 1 || f.count(t, "collection_publications") != 1 || f.count(t, "artifacts") != 5 {
				t.Fatal("acknowledgement recovery changed publication identity")
			}
			f.noNewCompute(t)
		})
	}
}

func TestArtifactsM3RejectsFalseManifestClaimsBeforePayloadTransfer(t *testing.T) {
	for _, fault := range []string{"nonce", "required", "false-gpu", "false-success"} {
		t.Run(fault, func(t *testing.T) {
			f := newArtifactJournalFixture(t, []byte("answer"))
			var m map[string]any
			if err := json.Unmarshal(f.files[artifactManifest], &m); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "nonce":
				m["attempt_nonce"] = "foreign"
			case "required":
				m["artifacts"] = []any{}
				delete(f.files, "outputs/answer.json")
			case "false-gpu":
				m["resource_check"] = map[string]bool{"gpu_required": true, "gpu_verified": true}
			case "false-success":
				m["exit_code"] = 1
			}
			f.files[artifactManifest], _ = json.Marshal(m)
			f.run(t, true)
			if f.count(t, "collection_snapshots") != 0 || f.count(t, "artifacts") != 0 || f.fetches["outputs/answer.json"] != 0 {
				t.Fatal("invalid manifest authorized payload or publication")
			}
			f.noNewCompute(t)
		})
	}
}
