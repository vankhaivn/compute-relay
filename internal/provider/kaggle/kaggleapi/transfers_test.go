package kaggleapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSignedStorageAllowlist(t *testing.T) {
	for _, location := range []string{"https://storage.googleapis.com/b/o?signature=fixture", "https://storage.googleapis.com:443/b/o"} {
		if !signedURL(location, true, false) || !signedURL(location, false, false) {
			t.Fatal(location)
		}
	}
	if !signedURL("https://www.googleapis.com/upload/storage/v1/b/fixture/o", true, false) ||
		!signedURL("https://www.kaggleusercontent.com/fixture", false, true) {
		t.Fatal("supported storage destination rejected")
	}
	for _, location := range []string{"http://storage.googleapis.com/b/o", "https://evil.invalid/o", "https://storage.googleapis.com.evil.invalid/o",
		"https://fixture@storage.googleapis.com/o", "https://storage.googleapis.com:444/o", "https://storage.googleapis.com/o#fragment",
		"https://storage.googleapis.com/o\n", "https://storage.googleapis.com/o\\path", "https://www.googleapis.com/identity/v1/o",
		"https://storage.googleapis.com./o", "https://STORAGE.GOOGLEAPIS.COM/o", "https://storage.googleapis.com"} {
		if signedURL(location, true, true) || signedURL(location, false, true) {
			t.Fatalf("unapproved storage URL: %s", location)
		}
	}
	if signedURL("https://www.googleapis.com/upload/storage/v1/b/fixture/o", false, false) || signedURL("https://www.kaggleusercontent.com/o", true, true) {
		t.Fatal("storage surfaces mixed")
	}
}

func TestDownloadsBoundSizeErrorsAndStorageRedirects(t *testing.T) {
	for _, mode := range []string{"overflow", "short", "encoding", "late-read", "writer", "storage-redirect", "untrusted-location", "duplicate-location"} {
		t.Run(mode, func(t *testing.T) {
			storageCalls := 0
			c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
				r := response(302, "text/html", "")
				r.Body = &unreadBody{t}
				r.Header.Set("Location", "https://storage.googleapis.com/fixture?signature=fixture")
				if mode == "untrusted-location" {
					r.Header.Set("Location", "https://evil.invalid/?signature=CANARY")
				}
				if mode == "duplicate-location" {
					r.Header.Add("Location", "https://storage.googleapis.com/second")
				}
				return r, nil
			}, func(request *http.Request) (*http.Response, error) {
				storageCalls++
				if request.Method != "GET" || request.Header.Get("Authorization") != "" {
					t.Fatal("wrong signed request")
				}
				r := response(200, "application/octet-stream", "abc")
				switch mode {
				case "overflow":
					r.Body = io.NopCloser(strings.NewReader("abcd"))
				case "short":
					r.Header.Set("Content-Length", "3")
					r.Body = io.NopCloser(strings.NewReader("ab"))
				case "encoding":
					r.Header.Set("Content-Encoding", "gzip")
					r.Body = &unreadBody{t}
				case "late-read":
					r.Body = &lateFailureBody{data: []byte("abc")}
				case "storage-redirect":
					r.StatusCode = 302
					r.Header.Set("Location", "https://evil.invalid")
					r.Body = &unreadBody{t}
				}
				return r, nil
			})
			var destination io.Writer = &bytes.Buffer{}
			if mode == "writer" {
				destination = failedWriter{}
			}
			receipt, err := c.DownloadKernelOutput(context.Background(), fixtureKernel, "relay-result/control/environment.json", 3, destination)
			if err == nil || receipt != (TransferReceipt{}) || strings.Contains(fmt.Sprint(err), fixtureToken) || strings.Contains(fmt.Sprint(err), "signature") {
				t.Fatal("invalid download succeeded or leaked")
			}
			want := 1
			if mode == "untrusted-location" || mode == "duplicate-location" {
				want = 0
			}
			if storageCalls != want {
				t.Fatalf("unexpected storage request count: %d", storageCalls)
			}
		})
	}
}

type lateFailureBody struct{ data []byte }

func (r *lateFailureBody) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, errors.New(fixtureToken)
}
func (*lateFailureBody) Close() error { return nil }

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, errors.New(fixtureToken) }

type uploadSourceFixture struct {
	io.Reader
	closeErr error
	closed   int
}

func (r *uploadSourceFixture) Close() error { r.closed++; return r.closeErr }

func TestUploadRequiresCompleteSourceAndOneAttempt(t *testing.T) {
	for _, mode := range []string{"ok", "short", "excess", "digest", "read", "close", "early-ack", "ack-overflow", "redirect", "transport"} {
		t.Run(mode, func(t *testing.T) {
			puts := 0
			c := newFixtureClient(t, func(*http.Request) (*http.Response, error) {
				return response(200, "application/json", `{"token":"fixture-blob","createUrl":"https://storage.googleapis.com/upload?signature=fixture"}`), nil
			}, func(request *http.Request) (*http.Response, error) {
				puts++
				if request.Header.Get("Authorization") != "" || request.GetBody != nil {
					t.Fatal("credential/replay leaked")
				}
				if mode == "transport" {
					return nil, errors.New(fixtureToken)
				}
				if mode != "early-ack" {
					_, err := io.Copy(io.Discard, request.Body)
					if err != nil {
						return nil, err
					}
				}
				r := response(200, "application/json", "{}")
				if mode == "ack-overflow" {
					r.Body = io.NopCloser(strings.NewReader(strings.Repeat("a", (64<<10)+1)))
				}
				if mode == "redirect" {
					r.StatusCode = 307
					r.Body = &unreadBody{t}
				}
				return r, nil
			})
			ticket, err := c.StartBlobUpload(context.Background(), "code.bin", 3)
			requireOK(t, err)
			data := "abc"
			if mode == "short" {
				data = "ab"
			}
			if mode == "excess" {
				data = "abcd"
			}
			source := &uploadSourceFixture{Reader: strings.NewReader(data)}
			if mode == "close" {
				source.closeErr = errors.New(fixtureToken)
			}
			if mode == "read" {
				source.Reader = &lateFailureBody{data: []byte("ab")}
			}
			digest := sha256.Sum256([]byte("abc"))
			if mode == "digest" {
				digest = sha256.Sum256([]byte("wrong"))
			}
			file, err := c.UploadBlob(context.Background(), ticket, source, digest)
			if mode == "ok" {
				requireOK(t, err)
				if file == nil || file.size != 3 {
					t.Fatal("missing verified upload")
				}
			} else if err == nil || file != nil || strings.Contains(fmt.Sprint(err), fixtureToken) || strings.Contains(fmt.Sprint(err), "signature") {
				t.Fatal("incomplete upload acknowledged or leaked")
			}
			if source.closed != 1 || puts != 1 {
				t.Fatalf("source/transport counts: close=%d PUT=%d", source.closed, puts)
			}
			_, err = c.UploadBlob(context.Background(), ticket, io.NopCloser(strings.NewReader("abc")), digest)
			if !errors.Is(err, ErrConfig) || puts != 1 {
				t.Fatal("ticket reused after outcome")
			}
		})
	}
}

type blockedUploadSource struct {
	once   sync.Once
	closed chan struct{}
}

func (b *blockedUploadSource) Read([]byte) (int, error) { <-b.closed; return 0, io.ErrClosedPipe }
func (b *blockedUploadSource) Close() error             { b.once.Do(func() { close(b.closed) }); return nil }

func TestUploadDeadlineClosesBlockedSource(t *testing.T) {
	c, err := New([]byte(fixtureToken), Options{TransferTimeout: 5 * time.Millisecond,
		APITransport: fixtureTransport(func(*http.Request) (*http.Response, error) {
			return response(200, "application/json", `{"token":"fixture-blob","createUrl":"https://storage.googleapis.com/upload?signature=fixture"}`), nil
		}), StorageTransport: fixtureTransport(func(request *http.Request) (*http.Response, error) {
			_, err := io.Copy(io.Discard, request.Body)
			return nil, err
		})})
	requireOK(t, err)
	defer c.Close()
	ticket, err := c.StartBlobUpload(context.Background(), "code.bin", 3)
	requireOK(t, err)
	source := &blockedUploadSource{closed: make(chan struct{})}
	file, err := c.UploadBlob(context.Background(), ticket, source, sha256.Sum256([]byte("abc")))
	if file != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-source.closed:
	default:
		t.Fatal("deadline left source open")
	}
}

func TestMutationRequestsRejectUnsafeLocalInputsBeforeSending(t *testing.T) {
	calls := 0
	c := newFixtureClient(t, func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("unexpected") }, nil)
	for _, request := range []SaveKernelRequest{
		{Resource: fixtureKernel, Dataset: fixtureDataset, Source: "fixture", WallSeconds: 0},
		{Resource: fixtureKernel, Dataset: fixtureDataset, Source: "fixture", WallSeconds: 60, MachineShape: "NvidiaTeslaT4"},
		{Resource: fixtureKernel, Dataset: fixtureDataset, Source: "fixture", WallSeconds: 60, GPU: true},
		{Resource: fixtureKernel, Dataset: fixtureDataset, Source: fixtureToken, WallSeconds: 60},
	} {
		if _, err := c.SaveKernel(context.Background(), request); !errors.Is(err, ErrConfig) {
			t.Fatal(err)
		}
	}
	if _, err := c.CreateDataset(context.Background(), CreateDatasetRequest{Resource: fixtureDataset, License: "other"}); !errors.Is(err, ErrConfig) {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("invalid local request consumed a mutation")
	}
}
