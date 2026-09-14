package blobfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vankhaivn/compute-relay/internal/domain"
)

var background = context.Background()

func newStore(t *testing.T) (*Store, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "blobs")
	s, err := New(root, Limits{MaxObjectBytes: 8 << 20, MaxTotalBytes: 32 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, root
}

func declaration(id string) domain.ObjectMetadata {
	return domain.ObjectMetadata{ID: domain.ObjectID(id), WorkspaceID: "a", Bytes: -1}
}

func readObject(t *testing.T, s *Store, w domain.WorkspaceID, id domain.ObjectID) []byte {
	t.Helper()
	f, err := s.Open(background, w, id)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestRoundTripNoOverwriteIsolationAndRestart(t *testing.T) {
	s, root := newStore(t)
	payload := []byte("immutable synthetic payload")
	meta, err := s.Put(background, declaration("obj_1"), bytes.NewReader(payload))
	if err != nil || !meta.Valid() || meta.Bytes != int64(len(payload)) {
		t.Fatalf("put: %+v %v", meta, err)
	}
	if got := readObject(t, s, "a", meta.ID); !bytes.Equal(got, payload) {
		t.Fatal("bytes changed")
	}
	if _, err := s.Put(background, declaration("obj_1"), strings.NewReader("replacement")); !errors.Is(err, ErrExists) {
		t.Fatal("overwrite allowed")
	}
	if _, err := s.Open(background, "b", meta.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross-workspace access")
	}
	if _, err := New(root, s.limits); !errors.Is(err, ErrLocked) {
		t.Fatalf("second store not blocked: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, s.limits)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := readObject(t, reopened, "a", meta.ID); !bytes.Equal(got, payload) {
		t.Fatal("restart lost bytes")
	}
	if reopened.used != meta.Bytes {
		t.Fatalf("usage: %d != %d", reopened.used, meta.Bytes)
	}
}

type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, nil }

func TestIncompleteInvalidAndFaultedWritesAreNeverVisible(t *testing.T) {
	for _, name := range []string{"length", "digest", "size", "read", "no-progress", "write", "sync", "rename", "free-space", "cancel"} {
		t.Run(name, func(t *testing.T) {
			s, _ := newStore(t)
			meta := declaration("obj_failed")
			var source io.Reader = strings.NewReader("payload")
			ctx := background
			want := ErrUnavailable
			switch name {
			case "length":
				meta.Bytes = 99
				want = ErrLengthMismatch
			case "digest":
				meta.SHA256 = domain.SHA256Digest(strings.Repeat("a", 64))
				want = ErrDigestMismatch
			case "size":
				s.limits.MaxObjectBytes = 3
				want = ErrTooLarge
			case "read":
				source = failReader{}
				want = io.ErrUnexpectedEOF
			case "no-progress":
				source = emptyReader{}
				want = io.ErrNoProgress
			case "write":
				s.write = func(f *os.File, b []byte) (int, error) {
					n, _ := f.Write(b[:2])
					return n, errors.New("ENOSPC injected")
				}
			case "sync":
				s.syncFile = func(*os.File) error { return errors.New("sync fault") }
			case "rename":
				s.rename = func(string, string) error { return errors.New("rename fault") }
			case "free-space":
				s.freeBytes = func(string) (uint64, error) { return 0, nil }
				want = ErrStorageFull
			case "cancel":
				c, cancel := context.WithCancel(background)
				cancel()
				ctx = c
				want = context.Canceled
			}
			result, err := s.Put(ctx, meta, source)
			if !errors.Is(err, want) || result.Valid() {
				t.Fatalf("failed put: %+v %v; want %v", result, err, want)
			}
			if _, err := s.Open(background, "a", meta.ID); !errors.Is(err, ErrNotFound) {
				t.Fatalf("failed object visible: %v", err)
			}
			if s.used != 0 {
				t.Fatalf("leaked usage: %d", s.used)
			}
			matches, _ := filepath.Glob(filepath.Join(s.root, "workspaces", "*", "temporary", "*"))
			if len(matches) != 0 {
				t.Fatalf("unfinished upload not cleaned: %v", matches)
			}
		})
	}
}

func TestTemporaryBytesAreNotReadable(t *testing.T) {
	s, _ := newStore(t)
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() { _, err := s.Put(background, declaration("obj_slow"), reader); done <- err }()
	if _, err := writer.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(background, "a", "obj_slow"); !errors.Is(err, ErrNotFound) {
		t.Fatal("partial bytes visible")
	}
	_ = writer.CloseWithError(io.ErrUnexpectedEOF)
	if err := <-done; !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}

func TestConcurrentWritesRespectGlobalByteBudget(t *testing.T) {
	s, _ := newStore(t)
	s.limits.MaxObjectBytes, s.limits.MaxTotalBytes = 100, 200
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.Put(background, declaration(fmt.Sprintf("obj_%d", i)), strings.NewReader(strings.Repeat("x", 100)))
			if err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			} else if !errors.Is(err, ErrStorageFull) {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if successes != 2 || s.used != 200 {
		t.Fatalf("successes=%d usage=%d", successes, s.used)
	}
}

type boundedReader struct {
	left    int
	maxRead int
}

func (r *boundedReader) Read(p []byte) (int, error) {
	if len(p) > bufferBytes {
		return 0, errors.New("unbounded copy buffer")
	}
	r.maxRead = max(r.maxRead, len(p))
	if r.left == 0 {
		return 0, io.EOF
	}
	n := min(r.left, len(p))
	clear(p[:n])
	r.left -= n
	return n, nil
}

func TestStreamingBufferAndPortableOpaqueIDs(t *testing.T) {
	s, _ := newStore(t)
	reader := &boundedReader{left: 4 << 20}
	meta := declaration("CON:" + strings.Repeat("a", 124))
	got, err := s.Put(background, meta, reader)
	if err != nil || got.Bytes != 4<<20 {
		t.Fatalf("stream: %+v %v", got, err)
	}
	if reader.maxRead != bufferBytes {
		t.Fatalf("buffer %d", reader.maxRead)
	}
	if _, err := s.Open(background, "a", "../../escape"); !errors.Is(err, ErrNotFound) {
		t.Fatal("unsafe ID accepted")
	}
	empty := declaration("obj_empty")
	empty.Bytes = 0
	got, err = s.Put(background, empty, strings.NewReader(""))
	if err != nil || !got.Valid() || got.Bytes != 0 {
		t.Fatalf("empty object: %+v %v", got, err)
	}
}

func TestSymlinkAndCorruptionRejected(t *testing.T) {
	s, _ := newStore(t)
	meta, err := s.Put(background, declaration("obj_1"), strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.root, "workspaces", encodedID("a"), "objects", encodedID(string(meta.ID)), "data")
	if err := os.WriteFile(path, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(background, "a", meta.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corruption accepted: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "private-file")
	if err := os.WriteFile(outside, []byte("secret-canary"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if _, err := s.Open(background, "a", meta.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatal("symlink followed")
	}
}

func TestCrashRecoveryPreservesCompleteObjectsAndRemovesOnlyStaging(t *testing.T) {
	root := filepath.Join(t.TempDir(), "blobs")
	marker := filepath.Join(t.TempDir(), "ready")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
	cmd.Env = append(os.Environ(), "CR_BLOB_CRASH_ROOT="+root, "CR_BLOB_CRASH_MARKER="+marker)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("crash helper did not reach staging")
		}
		time.Sleep(10 * time.Millisecond)
	}
	limits := Limits{MaxObjectBytes: 1024, MaxTotalBytes: 4096}
	if unexpected, err := New(root, limits); !errors.Is(err, ErrLocked) {
		if unexpected != nil {
			_ = unexpected.Close()
		}
		t.Fatalf("active writer not protected: %v", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	s, err := New(root, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if string(readObject(t, s, "a", "complete")) != "complete-bytes" {
		t.Fatal("complete bytes lost")
	}
	if _, err := s.Open(background, "a", "interrupted"); !errors.Is(err, ErrNotFound) {
		t.Fatal("interrupted upload exposed")
	}
	matches, _ := filepath.Glob(filepath.Join(root, "workspaces", "*", "temporary", "*"))
	if len(matches) != 0 {
		t.Fatal("staging survived recovery")
	}
}

func TestCrashHelper(t *testing.T) {
	root := os.Getenv("CR_BLOB_CRASH_ROOT")
	if root == "" {
		return
	}
	s, err := New(root, Limits{MaxObjectBytes: 1024, MaxTotalBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(background, declaration("complete"), strings.NewReader("complete-bytes")); err != nil {
		t.Fatal(err)
	}
	reader, writer := io.Pipe()
	go func() {
		if _, err := writer.Write([]byte("unfinished")); err != nil {
			return
		}
		_ = os.WriteFile(os.Getenv("CR_BLOB_CRASH_MARKER"), []byte("ready"), 0o600)
		time.Sleep(time.Minute)
	}()
	_, _ = s.Put(background, declaration("interrupted"), reader)
}

func TestEmptyObjectsAreBoundedAndCountSurvivesRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "blobs")
	limits := Limits{MaxObjectBytes: 64, MaxTotalBytes: 128, MaxObjects: 1}
	s, err := New(root, limits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Put(background, declaration("empty1"), strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Put(background, declaration("empty2"), strings.NewReader("")); !errors.Is(err, ErrStorageFull) {
		t.Fatalf("empty quota: %v", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(root, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Put(background, declaration("empty3"), strings.NewReader("")); !errors.Is(err, ErrStorageFull) {
		t.Fatalf("restart quota: %v", err)
	}
}
