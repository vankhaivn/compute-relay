package httpsinput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const publicTestIP = "93.184.216.34"

// Test-only routing maps a policy-approved logical peer to a local TLS fixture.
// There is no production config flag that permits this mapping or skips TLS checks.
type fixtureConn struct {
	net.Conn
	peer *net.TCPAddr
}

func (c *fixtureConn) RemoteAddr() net.Addr { return c.peer }

func tlsFixture(t *testing.T, handler http.HandlerFunc, mutate func(*Config)) (*Client, *atomic.Int32) {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)
	cfg := DefaultConfig()
	cfg.MaxBytes = 8 << 20
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	c.tlsConfig.RootCAs = pool
	var calls atomic.Int32
	c.lookup = func(ctx context.Context, host string) ([]net.IPAddr, error) {
		if !strings.HasSuffix(host, ".") {
			t.Error("nonabsolute DNS query")
		}
		return []net.IPAddr{{IP: net.ParseIP(publicTestIP)}}, nil
	}
	c.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		calls.Add(1)
		if network != "tcp" || address != publicTestIP+":443" {
			return nil, errors.New("unexpected fixture dial")
		}
		connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
		if err != nil {
			return nil, err
		}
		return &fixtureConn{connection, &net.TCPAddr{IP: net.ParseIP(publicTestIP), Port: 443}}, nil
	}
	return c, &calls
}

func drain(ctx context.Context, length int64, source io.Reader) error {
	_, err := io.CopyBuffer(io.Discard, source, make([]byte, BufferBytes))
	return err
}

func TestLoopbackTLSSmoke(t *testing.T) {
	// A real local TLS exchange, redirect, chunked stream and private temporary file;
	// never a public network request or a claim of SQLite/provider integration.
	payload := bytes.Repeat([]byte("synthetic input\n"), 280000)
	digest := sha256.Sum256(payload)
	var hits atomic.Int32
	c, calls := tlsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		for _, key := range []string{"Authorization", "Proxy-Authorization", "Cookie", "Referer", "X-Workspace-ID", "X-Request-ID"} {
			if r.Header.Get(key) != "" {
				t.Error("inherited header", key)
			}
		}
		if r.Host != "example.com" || r.Method != "GET" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("request identity changed")
		}
		if r.URL.Path == "/start" {
			w.Header().Set("Location", "/data")
			w.Header().Set("Set-Cookie", "synthetic=canary")
			w.WriteHeader(302)
			return
		}
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		_, _ = w.Write(payload)
	}, nil)
	t.Setenv("HTTPS_PROXY", "http://synthetic:canary@127.0.0.1:1")
	t.Setenv("ALL_PROXY", "http://127.0.0.1:1")
	root := t.TempDir()
	temp := filepath.Join(root, "partial")
	final := filepath.Join(root, "complete")
	err := c.Fetch(context.Background(), Request{URL: "https://example.com/start?q=synthetic-canary", SHA256: hex.EncodeToString(digest[:])}, func(ctx context.Context, n int64, r io.Reader) error {
		if n != -1 {
			t.Error("expected unknown-length stream")
		}
		f, err := os.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, copyErr := io.CopyBuffer(f, r, make([]byte, BufferBytes))
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		return os.Rename(temp, final)
	})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := os.ReadFile(final)
	if err != nil || !bytes.Equal(stored, payload) {
		t.Fatal("snapshot mismatch", err)
	}
	if calls.Load() != 2 || hits.Load() != 2 {
		t.Fatal("unexpected replay/redirect", calls.Load(), hits.Load())
	}
	t.Logf("passed-offline: %d bytes, TLS, checked redirect, chunked stream, SHA-256, no ambient headers/proxy; no provider calls", len(stored))
}

func TestBlockedDNSAnswersNeverDial(t *testing.T) {
	for _, ips := range [][]string{{"127.0.0.1"}, {"169.254.169.254"}, {"100.100.100.200"}, {"168.63.129.16"}, {publicTestIP, "10.0.0.1"}, {"::1"}, {"64:ff9b::7f00:1"}, {publicTestIP, "fc00::1"}, {}} {
		c, _ := New(DefaultConfig())
		var dials int
		c.lookup = func(context.Context, string) ([]net.IPAddr, error) {
			var out []net.IPAddr
			for _, ip := range ips {
				out = append(out, net.IPAddr{IP: net.ParseIP(ip)})
			}
			return out, nil
		}
		c.dial = func(context.Context, string, string) (net.Conn, error) { dials++; return nil, ErrFetch }
		if err := c.Fetch(context.Background(), Request{URL: "https://example.com/data"}, drain); !errors.Is(err, ErrBlocked) || dials != 0 {
			t.Fatalf("unsafe DNS reached dial: %v %v %d", ips, err, dials)
		}
	}
}

func TestEveryRedirectRevalidatesDNSAndPolicy(t *testing.T) {
	for _, destination := range []string{"https://127.0.0.1/metadata", "https://169.254.169.254/", "http://example.com/plain", "https://user:synthetic@example.com/", "https://example.com:8443/", "https://example.com/private", "https://other.example.org/data"} {
		t.Run(destination, func(t *testing.T) {
			c, dials := tlsFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", destination)
				w.WriteHeader(302)
			}, nil)
			var lookups atomic.Int32
			c.lookup = func(context.Context, string) ([]net.IPAddr, error) {
				ip := publicTestIP
				if lookups.Add(1) > 1 {
					ip = "10.0.0.1"
				}
				return []net.IPAddr{{IP: net.ParseIP(ip)}}, nil
			}
			if err := c.Fetch(context.Background(), Request{URL: "https://example.com/start"}, drain); err == nil {
				t.Fatal("unsafe redirect accepted")
			}
			if dials.Load() != 1 {
				t.Fatal("redirect reached prohibited endpoint", dials.Load())
			}
		})
	}
}

func TestRedirectCountAndAllowlist(t *testing.T) {
	c, dials := tlsFixture(t, func(w http.ResponseWriter, r *http.Request) { w.Header().Set("Location", "/loop"); w.WriteHeader(308) }, func(c *Config) { c.MaxRedirects = 2 })
	if err := c.Fetch(context.Background(), Request{URL: "https://example.com/loop"}, drain); !errors.Is(err, ErrURL) || dials.Load() != 3 {
		t.Fatal("unbounded redirect chain", err, dials.Load())
	}
	c, dials = tlsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "https://other.example.org/data")
		w.WriteHeader(302)
	}, func(c *Config) { c.AllowedHosts = []string{"example.com"} })
	if err := c.Fetch(context.Background(), Request{URL: "https://example.com/start"}, drain); !errors.Is(err, ErrBlocked) || dials.Load() != 1 {
		t.Fatal("allowlist bypass", err)
	}
	if err := c.Fetch(context.Background(), Request{URL: "https://other.example.org/data"}, drain); !errors.Is(err, ErrBlocked) || dials.Load() != 1 {
		t.Fatal("initial allowlist bypass", err)
	}
}

func TestPeerMismatchClosedBeforeTLS(t *testing.T) {
	for _, peer := range []*net.TCPAddr{{IP: net.ParseIP("127.0.0.1"), Port: 443}, {IP: net.ParseIP("8.8.8.8"), Port: 443}, {IP: net.ParseIP(publicTestIP), Port: 80}, {IP: net.ParseIP(publicTestIP), Port: 443, Zone: "scope"}} {
		c, _ := New(DefaultConfig())
		left, right := net.Pipe()
		c.dial = func(context.Context, string, string) (net.Conn, error) { return &fixtureConn{left, peer}, nil }
		_, err := c.connect(context.Background(), publicTestIP)
		if !errors.Is(err, ErrBlocked) {
			t.Fatal("mismatched peer accepted", err)
		}
		_ = right.SetReadDeadline(time.Now().Add(time.Second))
		var b [1]byte
		if n, err := right.Read(b[:]); n != 0 || err != io.EOF {
			t.Fatal("TLS bytes sent to wrong peer", n, err)
		}
		right.Close()
	}
}

func TestTLSIdentityAndUnknownAuthority(t *testing.T) {
	var hits atomic.Int32
	c, _ := tlsFixture(t, func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }, nil)
	if err := c.Fetch(context.Background(), Request{URL: "https://wrong.example.org/"}, drain); !errors.Is(err, ErrFetch) {
		t.Fatal("hostname verification disabled", err)
	}
	c.tlsConfig.RootCAs = x509.NewCertPool()
	if err := c.Fetch(context.Background(), Request{URL: "https://example.com/"}, drain); !errors.Is(err, ErrFetch) {
		t.Fatal("certificate verification disabled", err)
	}
	if hits.Load() != 0 {
		t.Fatal("request sent before TLS verification")
	}
}

func TestResponseAndIntegrityFailuresNeverReachVerifiedEOF(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		headers  map[string]string
		payload  string
		expected string
		limit    int64
		want     error
	}{
		{"digest", 200, nil, "payload", strings.Repeat("a", 64), 32, ErrDigest},
		{"too large known", 200, map[string]string{"Content-Length": "9"}, "123456789", "", 8, ErrLimit},
		{"too large unknown", 200, nil, "123456789", "", 8, ErrLimit},
		{"short", 200, map[string]string{"Content-Length": "12"}, "short", "", 32, ErrFetch},
		{"partial", 206, nil, "partial", "", 32, ErrFetch},
		{"range", 200, map[string]string{"Content-Range": "bytes 0-3/8"}, "data", "", 32, ErrFetch},
		{"encoded", 200, map[string]string{"Content-Encoding": "gzip"}, "data", "", 32, ErrFetch},
		{"expired", 403, nil, "secret-body-canary", "", 32, ErrFetch},
		{"not found", 404, nil, "secret-body-canary", "", 32, ErrFetch},
		{"headers", 200, map[string]string{"X-Padding": strings.Repeat("a", 40<<10)}, "data", "", 32, ErrFetch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, dials := tlsFixture(t, func(w http.ResponseWriter, r *http.Request) {
				for k, v := range tc.headers {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tc.status)
				w.(http.Flusher).Flush()
				_, _ = io.WriteString(w, tc.payload)
			}, func(c *Config) { c.MaxBytes = tc.limit })
			committed := false
			err := c.Fetch(context.Background(), Request{URL: "https://example.com/file?q=secret-query-canary", SHA256: tc.expected}, func(ctx context.Context, n int64, r io.Reader) error {
				err := drain(ctx, n, r)
				committed = err == nil
				return err
			})
			if !errors.Is(err, tc.want) || committed || dials.Load() != 1 {
				t.Fatalf("failure not preserved: %v committed=%v dials=%d", err, committed, dials.Load())
			}
			if strings.Contains(fmt.Sprintf("%+v", err), "canary") {
				t.Fatal("remote data leaked")
			}
		})
	}
}

func shortBudgets(c *Config) {
	c.ConnectTimeout = 200 * time.Millisecond
	c.TLSHandshakeTimeout = 200 * time.Millisecond
	c.HeaderTimeout = 200 * time.Millisecond
	c.ReadIdleTimeout = 200 * time.Millisecond
	c.TotalTimeout = time.Second
}

func TestConnectAndTLSBudgets(t *testing.T) {
	for _, phase := range []string{"dns", "connect", "tls"} {
		t.Run(phase, func(t *testing.T) {
			cfg := DefaultConfig()
			shortBudgets(&cfg)
			cfg.ConnectTimeout = 40 * time.Millisecond
			cfg.TLSHandshakeTimeout = 40 * time.Millisecond
			c, _ := New(cfg)
			if phase == "dns" {
				c.lookup = func(ctx context.Context, _ string) ([]net.IPAddr, error) { <-ctx.Done(); return nil, ctx.Err() }
			} else {
				c.lookup = func(context.Context, string) ([]net.IPAddr, error) {
					return []net.IPAddr{{IP: net.ParseIP(publicTestIP)}}, nil
				}
				c.dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
					if phase == "connect" {
						<-ctx.Done()
						return nil, ctx.Err()
					}
					left, right := net.Pipe()
					t.Cleanup(func() { left.Close(); right.Close() })
					return &fixtureConn{left, &net.TCPAddr{IP: net.ParseIP(publicTestIP), Port: 443}}, nil
				}
			}
			if err := c.Fetch(context.Background(), Request{URL: "https://example.com/"}, drain); !errors.Is(err, ErrTimeout) {
				t.Fatalf("%s deadline: %v", phase, err)
			}
		})
	}
}

func TestHeaderIdleAndTotalBudgets(t *testing.T) {
	for _, phase := range []string{"headers", "idle", "total"} {
		t.Run(phase, func(t *testing.T) {
			c, _ := tlsFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if phase == "headers" {
					<-r.Context().Done()
					return
				}
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				if phase == "idle" {
					<-r.Context().Done()
					return
				}
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-r.Context().Done():
						return
					case <-ticker.C:
						if _, err := io.WriteString(w, "x"); err != nil {
							return
						}
						w.(http.Flusher).Flush()
					}
				}
			}, func(c *Config) {
				shortBudgets(c)
				if phase == "headers" {
					c.HeaderTimeout = 50 * time.Millisecond
				}
				if phase == "idle" {
					c.ReadIdleTimeout = 50 * time.Millisecond
				}
				if phase == "total" {
					c.TotalTimeout = 250 * time.Millisecond
				}
			})
			if err := c.Fetch(context.Background(), Request{URL: "https://example.com/"}, drain); !errors.Is(err, ErrTimeout) {
				t.Fatal(phase, err)
			}
		})
	}
}

func TestCancellationConcurrencyAndEarlySink(t *testing.T) {
	entered := make(chan struct{})
	c, _ := tlsFixture(t, func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }, func(c *Config) { c.MaxConcurrent = 1 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Fetch(ctx, Request{URL: "https://example.com/"}, drain) }()
	<-entered
	if err := c.Fetch(context.Background(), Request{URL: "https://example.com/"}, drain); !errors.Is(err, ErrBusy) {
		t.Fatal("concurrency unbounded", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
	if len(c.slots) != 0 {
		t.Fatal("slot leaked")
	}
	c, _ = tlsFixture(t, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "data") }, nil)
	if err := c.Fetch(context.Background(), Request{URL: "https://example.com/"}, func(context.Context, int64, io.Reader) error { return nil }); !errors.Is(err, ErrFetch) {
		t.Fatal("unread source accepted", err)
	}
}
