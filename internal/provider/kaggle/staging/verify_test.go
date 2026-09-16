package staging

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

type readProbe struct {
	plan                              Plan
	snapshot                          Snapshot
	content                           map[string][]byte
	inspections, lists, opens, closed int
	pageSize                          int
	inspectHook                       func(*readProbe)
	listHook                          func(*Page)
	openHook                          func(string, io.ReadCloser) (io.ReadCloser, error)
	requested                         []string
}

func probe(t testing.TB) *readProbe {
	t.Helper()
	p := planFixture(t)
	return &readProbe{plan: p, snapshot: Snapshot{"fixture_user", p.Reference(), "12345", 1, Private, Ready}, pageSize: 1,
		content: map[string][]byte{"bundle.bin": []byte("code"), "input-000.bin": []byte("hello"), ManifestName: p.Manifest()}}
}
func (s *readProbe) Inspect(ctx context.Context, ref string) (Snapshot, error) {
	if ref != s.plan.Reference() {
		return Snapshot{}, errors.New("wrong reference")
	}
	s.inspections++
	if s.inspectHook != nil {
		s.inspectHook(s)
	}
	return s.snapshot, nil
}
func (s *readProbe) List(ctx context.Context, pin Pin, cursor string, limit int) (Page, error) {
	s.lists++
	if pin != (Pin{s.plan.Reference(), "12345", 1}) || limit != 100 {
		return Page{}, errors.New("wrong pinned list request")
	}
	start := 0
	if cursor != "" {
		var err error
		start, err = strconv.Atoi(cursor)
		if err != nil {
			return Page{}, err
		}
	}
	files := s.plan.Files()
	end := min(len(files), start+s.pageSize)
	var p Page
	for _, f := range files[start:end] {
		p.Files = append(p.Files, RemoteFile{f.Name, f.Bytes, "file"})
	}
	if end < len(files) {
		p.NextCursor = strconv.Itoa(end)
	}
	if s.listHook != nil {
		s.listHook(&p)
	}
	return p, nil
}
func (s *readProbe) Open(ctx context.Context, pin Pin, name string) (io.ReadCloser, error) {
	s.opens++
	s.requested = append(s.requested, name)
	if pin != (Pin{s.plan.Reference(), "12345", 1}) {
		return nil, errors.New("wrong pinned byte request")
	}
	r := &trackedReader{Reader: bytes.NewReader(s.content[name]), close: func() { s.closed++ }}
	if s.openHook != nil {
		return s.openHook(name, r)
	}
	return r, nil
}

type trackedReader struct {
	io.Reader
	close func()
}

func (r *trackedReader) Close() error { r.close(); return nil }
func TestAssessmentRequiresManifestAllPagesAllBytesAndFinalObservation(t *testing.T) {
	s := probe(t)
	p := s.plan
	for i := 0; i < 2; i++ {
		prior := Pin{p.Reference(), "12345", 1}
		a, err := p.Assess(context.Background(), s, &prior)
		if err != nil || !a.Ready() || a.Pin() != prior || a.PlanSHA256() != p.Digest() {
			t.Fatal("valid snapshot failed", a, err)
		}
	}
	if s.inspections != 4 || s.lists != 6 || s.opens != 6 || s.closed != 6 {
		t.Fatal("missing verification steps", s)
	}
	if !reflect.DeepEqual(s.requested, []string{ManifestName, "bundle.bin", "input-000.bin", ManifestName, "bundle.bin", "input-000.bin"}) {
		t.Fatal("manifest must be first")
	}
	if (Assessment{}).Ready() {
		t.Fatal("zero assessment ready")
	}
}
func TestAssessmentDoesNotPromoteUploadOrMissingEvidence(t *testing.T) {
	for _, phase := range []Phase{Processing, Absent, Unknown, "new-upstream-state"} {
		t.Run(string(phase), func(t *testing.T) {
			s := probe(t)
			s.snapshot.Phase = phase
			a, err := s.plan.Assess(context.Background(), s, nil)
			if err != nil || a.Ready() || a.Pin() != (Pin{}) || s.opens != 0 || s.lists != 0 {
				t.Fatal("premature readiness", a, err)
			}
		})
	}
}
func TestAssessmentRejectsIdentityPrivacyVersionAndKnownIDChanges(t *testing.T) {
	for _, mode := range []string{"account", "reference", "id-missing", "version", "private-unknown", "public", "failed", "known-id", "prior-ref", "prior-version"} {
		t.Run(mode, func(t *testing.T) {
			s := probe(t)
			var prior *Pin
			switch mode {
			case "account":
				s.snapshot.Account = "other_user"
			case "reference":
				s.snapshot.Reference += "-other"
			case "id-missing":
				s.snapshot.DatasetID = ""
			case "version":
				s.snapshot.Version = 2
			case "private-unknown":
				s.snapshot.Visibility = ""
			case "public":
				s.snapshot.Visibility = Public
			case "failed":
				s.snapshot.Phase = Failed
			case "known-id":
				prior = &Pin{s.plan.Reference(), "other", 1}
			case "prior-ref":
				prior = &Pin{"wrong", "12345", 1}
			case "prior-version":
				prior = &Pin{s.plan.Reference(), "12345", 2}
			}
			a, err := s.plan.Assess(context.Background(), s, prior)
			if err == nil || a.Ready() || s.opens != 0 || s.lists != 0 {
				t.Fatal("unsafe staging accepted", a, err)
			}
		})
	}
}
func TestAssessmentRejectsIncompleteDuplicateAndUnsafeCatalogs(t *testing.T) {
	for _, mode := range []string{"missing", "duplicate", "wrong-size", "symlink", "extra", "traversal", "cursor-cycle", "cursor-large", "cursor-control", "empty-continuation"} {
		t.Run(mode, func(t *testing.T) {
			s := probe(t)
			s.listHook = func(p *Page) {
				switch mode {
				case "missing":
					p.NextCursor = ""
				case "duplicate":
					p.Files[0] = RemoteFile{"bundle.bin", 4, "file"}
				case "wrong-size":
					p.Files[0].Bytes++
				case "symlink":
					p.Files[0].Kind = "symlink"
				case "extra":
					p.Files = append(p.Files, RemoteFile{"untracked", 0, "file"})
				case "traversal":
					p.Files[0].Name = "../runtime.db"
				case "cursor-cycle":
					p.NextCursor = "1"
				case "cursor-large":
					p.NextCursor = strings.Repeat("x", 513)
				case "cursor-control":
					p.NextCursor = "bad\n"
				case "empty-continuation":
					p.Files = nil
					p.NextCursor = "1"
				}
			}
			a, err := s.plan.Assess(context.Background(), s, nil)
			if err != ErrCatalog || a.Ready() || s.opens != 0 {
				t.Fatal("bad catalog accepted", a, err)
			}
		})
	}
}

// A reader may deliver exactly all bytes and then fail; EOF/Close still matter.
type lateReader struct{ io.Reader }

func (r lateReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		return n, errors.New("SYNTHETIC_PRIVATE_DIAGNOSTIC")
	}
	return n, err
}

type closingReader struct{ io.ReadCloser }

func (r closingReader) Close() error {
	_ = r.ReadCloser.Close()
	return errors.New("SYNTHETIC_PRIVATE_DIAGNOSTIC")
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, nil }
func TestAssessmentRejectsTransferBoundaryFailures(t *testing.T) {
	for _, mode := range []string{"short", "long", "digest", "manifest-other-attempt", "late-read", "close", "empty-loop", "nil-open", "open-error"} {
		t.Run(mode, func(t *testing.T) {
			s := probe(t)
			switch mode {
			case "short":
				s.content["bundle.bin"] = []byte("cod")
			case "long":
				s.content["bundle.bin"] = []byte("codeX")
			case "digest":
				s.content["bundle.bin"] = []byte("evil")
			case "manifest-other-attempt":
				s.content[ManifestName] = bytes.ReplaceAll(s.content[ManifestName], []byte(`"attempt"`), []byte(`"another"`))
			default:
				s.openHook = func(name string, r io.ReadCloser) (io.ReadCloser, error) {
					if name != "bundle.bin" {
						return r, nil
					}
					switch mode {
					case "late-read":
						return &trackedReader{Reader: lateReader{r}, close: func() { _ = r.Close() }}, nil
					case "close":
						return closingReader{r}, nil
					case "empty-loop":
						return &trackedReader{Reader: emptyReader{}, close: func() { _ = r.Close() }}, nil
					case "nil-open":
						_ = r.Close()
						return nil, nil
					default:
						return r, errors.New("SYNTHETIC_PRIVATE_DIAGNOSTIC")
					}
				}
			}
			a, err := s.plan.Assess(context.Background(), s, nil)
			if err != ErrTransfer || a.Ready() || s.opens != s.closed {
				t.Fatal("bad transfer accepted or handle leaked", a, err, s.opens, s.closed)
			}
			if mode == "manifest-other-attempt" && s.opens != 1 {
				t.Fatal("payload read before validating manifest")
			}
			if strings.Contains(err.Error(), "SYNTHETIC_PRIVATE") {
				t.Fatal("raw provider diagnostic leaked")
			}
		})
	}
}
func TestAssessmentRechecksIdentityPrivacyAndReadinessAfterBytes(t *testing.T) {
	for _, mode := range []string{"id", "version", "privacy", "state", "account"} {
		t.Run(mode, func(t *testing.T) {
			s := probe(t)
			s.inspectHook = func(s *readProbe) {
				if s.inspections != 2 {
					return
				}
				switch mode {
				case "id":
					s.snapshot.DatasetID = "replacement"
				case "version":
					s.snapshot.Version = 2
				case "privacy":
					s.snapshot.Visibility = Public
				case "state":
					s.snapshot.Phase = Processing
				case "account":
					s.snapshot.Account = "another_user"
				}
			}
			a, err := s.plan.Assess(context.Background(), s, nil)
			if err != ErrChanged || a.Ready() || a.Pin() != (Pin{}) {
				t.Fatal("race accepted", a, err)
			}
		})
	}
}
func TestAssessmentCancellationClosesStreamAndNeverReturnsReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := probe(t)
	s.openHook = func(name string, r io.ReadCloser) (io.ReadCloser, error) { cancel(); return r, nil }
	a, err := s.plan.Assess(ctx, s, nil)
	if !errors.Is(err, context.Canceled) || a.Ready() || s.closed != s.opens || s.opens != 1 {
		t.Fatal("cancellation lost", a, err)
	}
	s = probe(t)
	if _, err := s.plan.Assess(ctx, s, nil); !errors.Is(err, context.Canceled) || s.inspections != 0 {
		t.Fatal("cancelled context performed I/O", err)
	}
}

type errorProbe struct {
	*readProbe
	mode string
}

func (s *errorProbe) Inspect(ctx context.Context, ref string) (Snapshot, error) {
	if s.mode == "inspect" || (s.mode == "final-inspect" && s.inspections == 1) {
		return Snapshot{}, errors.New("SYNTHETIC_PRIVATE_DIAGNOSTIC")
	}
	return s.readProbe.Inspect(ctx, ref)
}
func (s *errorProbe) List(ctx context.Context, pin Pin, cursor string, limit int) (Page, error) {
	if s.mode == "list" {
		return Page{}, errors.New("SYNTHETIC_PRIVATE_DIAGNOSTIC")
	}
	return s.readProbe.List(ctx, pin, cursor, limit)
}
func TestAssessmentSanitizesObservationFailures(t *testing.T) {
	for _, mode := range []string{"inspect", "list", "final-inspect"} {
		t.Run(mode, func(t *testing.T) {
			s := &errorProbe{probe(t), mode}
			a, err := s.plan.Assess(context.Background(), s, nil)
			if err != ErrObservation || a.Ready() || a.Pin() != (Pin{}) {
				t.Fatal("raw or successful observation failure", a, err)
			}
		})
	}
	if _, err := (Plan{}).Assess(context.Background(), probe(t), nil); err != ErrPlan {
		t.Fatal("zero plan usable", err)
	}
	if _, err := planFixture(t).Assess(context.Background(), nil, nil); err != ErrPlan {
		t.Fatal("nil source usable", err)
	}
}
