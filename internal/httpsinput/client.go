package httpsinput

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

const BufferBytes = 64 * 1024

type Config struct {
	MaxBytes                                                                          int64
	MaxRedirects                                                                      int
	MaxConcurrent                                                                     int
	ConnectTimeout, TLSHandshakeTimeout, HeaderTimeout, ReadIdleTimeout, TotalTimeout time.Duration
	// Optional exact host allowlist; redirects must satisfy it too. It cannot permit
	// private IPs, non-443 ports, invalid certificates or ambient proxy use.
	AllowedHosts []string
}

func DefaultConfig() Config {
	return Config{MaxBytes: 2 << 30, MaxRedirects: 3, MaxConcurrent: 4,
		ConnectTimeout: 10 * time.Second, TLSHandshakeTimeout: 10 * time.Second,
		HeaderTimeout: 15 * time.Second, ReadIdleTimeout: 15 * time.Second, TotalTimeout: 2 * time.Minute}
}

type Client struct {
	config Config
	hosts  map[string]bool
	slots  chan struct{}
	// Test seams are private; there is no exported arbitrary transport/resolver or
	// insecure/private-destination bypass. Production uses the OS resolver and TCP.
	lookup    func(context.Context, string) ([]net.IPAddr, error)
	dial      func(context.Context, string, string) (net.Conn, error)
	tlsConfig *tls.Config
}

func New(cfg Config) (*Client, error) {
	if cfg.MaxBytes <= 0 || cfg.MaxBytes > 1<<40 || cfg.MaxRedirects < 0 || cfg.MaxRedirects > 10 || cfg.MaxConcurrent < 1 || cfg.MaxConcurrent > 64 || len(cfg.AllowedHosts) > 256 {
		return nil, ErrURL
	}
	for _, d := range []time.Duration{cfg.ConnectTimeout, cfg.TLSHandshakeTimeout, cfg.HeaderTimeout, cfg.ReadIdleTimeout, cfg.TotalTimeout} {
		if d <= 0 || d > 24*time.Hour || d > cfg.TotalTimeout {
			return nil, ErrURL
		}
	}
	c := &Client{config: cfg, hosts: map[string]bool{}, slots: make(chan struct{}, cfg.MaxConcurrent),
		lookup:    (&net.Resolver{PreferGo: true}).LookupIPAddr,
		dial:      (&net.Dialer{Timeout: cfg.ConnectTimeout, KeepAlive: -1}).DialContext,
		tlsConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	for _, host := range cfg.AllowedHosts {
		u, err := parseURL("https://" + host + "/")
		if err != nil || u.Host != strings.ToLower(host) {
			return nil, ErrURL
		}
		c.hosts[u.Host] = true
	}
	c.config.AllowedHosts = nil // No caller-owned mutable slice survives construction.
	return c, nil
}

// Fetch keeps one total budget through DNS, redirects, TLS, body and the consuming
// callback. The callback MUST consume verified EOF before committing ownership. It
// must respect context and never retain the reader. Transport errors never contain
// URLs, queries, response bodies/headers or low-level network/TLS diagnostics.
// Consumer errors are returned unchanged; the consumer owns their sanitization.
// Each call creates a new snapshot; it never refreshes an existing object's bytes.
func (c *Client) Fetch(ctx context.Context, input Request, consume func(context.Context, int64, io.Reader) error) error {
	if err := input.Validate(); err != nil {
		return err
	}
	if consume == nil {
		return ErrURL
	}
	if err := ctx.Err(); err != nil {
		return transferError(ctx, err)
	}
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	default:
		return ErrBusy
	}
	ctx, cancel := context.WithTimeout(ctx, c.config.TotalTimeout)
	defer cancel()
	u, _ := parseURL(input.URL)
	for redirects := 0; ; redirects++ {
		if len(c.hosts) != 0 && !c.hosts[u.Host] {
			return ErrBlocked
		}
		response, transport, err := c.roundTrip(ctx, u)
		if err != nil {
			return transferError(ctx, err)
		}
		if isRedirect(response.StatusCode) {
			locations := response.Header.Values("Location")
			// Never drain an attacker-controlled redirect body; no connection is reused.
			_ = response.Body.Close()
			transport.CloseIdleConnections()
			if redirects >= c.config.MaxRedirects || len(locations) != 1 || len(locations[0]) > MaxURLBytes {
				return ErrURL
			}
			relative, err := url.Parse(locations[0])
			if err != nil {
				return ErrURL
			}
			u, err = parseURL(u.ResolveReference(relative).String())
			if err != nil {
				return err
			}
			continue
		}
		defer transport.CloseIdleConnections()
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK || response.Header.Get("Content-Range") != "" {
			return ErrFetch
		}
		encodings := response.Header.Values("Content-Encoding")
		if len(encodings) > 1 || len(encodings) == 1 && encodings[0] != "" && encodings[0] != "identity" {
			return ErrFetch
		}
		if response.ContentLength > c.config.MaxBytes {
			return ErrLimit
		}
		body := &verifiedReader{ctx: ctx, source: response.Body, max: c.config.MaxBytes,
			declared: response.ContentLength, expected: input.SHA256, hash: sha256.New()}
		if err := consume(ctx, response.ContentLength, body); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return transferError(ctx, err)
		}
		if !body.done {
			return ErrFetch
		}
		return nil
	}
}

func (c *Client) roundTrip(ctx context.Context, u *url.URL) (*http.Response, *http.Transport, error) {
	// Fresh HTTP/1 transport per hop: no cached connection bypasses DNS/peer checks;
	// no reused-connection GET retry, automatic redirects, cookie jar or Referer.
	t := &http.Transport{Proxy: nil, DisableKeepAlives: true, DisableCompression: true,
		TLSClientConfig: c.tlsConfig.Clone(), TLSHandshakeTimeout: c.config.TLSHandshakeTimeout,
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
		ResponseHeaderTimeout: c.config.HeaderTimeout, MaxResponseHeaderBytes: 32 << 10,
		ReadBufferSize: 4096, WriteBufferSize: 4096}
	t.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" || address != net.JoinHostPort(u.Hostname(), "443") {
			return nil, ErrBlocked
		}
		return c.connect(ctx, u.Hostname())
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, t, ErrURL
	}
	r.Header.Set("Accept-Encoding", "identity")
	r.Header.Set("User-Agent", "Compute-Relay-input/1")
	response, err := t.RoundTrip(r)
	if err != nil {
		t.CloseIdleConnections()
	}
	return response, t, err
}

func (c *Client) connect(ctx context.Context, host string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, c.config.ConnectTimeout)
	defer cancel()
	var addresses []net.IPAddr
	if ip, err := netip.ParseAddr(host); err == nil {
		addresses = []net.IPAddr{{IP: net.IP(ip.AsSlice())}}
	} else {
		var err error
		// Absolute DNS query: never append the host's private search suffix.
		addresses, err = c.lookup(ctx, host+".")
		if err != nil {
			return nil, transferError(ctx, err)
		}
	}
	if len(addresses) == 0 || len(addresses) > 16 {
		return nil, ErrBlocked
	}
	var selected netip.Addr
	for _, answer := range addresses {
		ip, ok := netip.AddrFromSlice(answer.IP)
		// net.IP commonly represents ordinary A answers as 16-byte mapped addresses.
		ip = ip.Unmap()
		if !ok || answer.Zone != "" || !publicIP(ip) {
			return nil, ErrBlocked
		}
		if !selected.IsValid() {
			selected = ip
		}
	}
	// Dial exactly one validated numeric address. No second DNS lookup, proxy, or
	// automatic alternate-source/connection retry is performed.
	connection, err := c.dial(ctx, "tcp", net.JoinHostPort(selected.String(), "443"))
	if err != nil {
		return nil, transferError(ctx, err)
	}
	if connection == nil {
		return nil, ErrFetch
	}
	peer, ok := connection.RemoteAddr().(*net.TCPAddr)
	if !ok {
		connection.Close()
		return nil, ErrBlocked
	}
	actual, valid := netip.AddrFromSlice(peer.IP)
	actual = actual.Unmap()
	if !valid || peer.Zone != "" || peer.Port != 443 || actual != selected || !publicIP(actual) {
		connection.Close()
		return nil, ErrBlocked
	}
	if err := ctx.Err(); err != nil {
		connection.Close()
		return nil, transferError(ctx, err)
	}
	return &idleConn{Conn: connection, timeout: c.config.ReadIdleTimeout}, nil
}

type idleConn struct {
	net.Conn
	timeout time.Duration
}

func (c *idleConn) Read(p []byte) (int, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

func isRedirect(code int) bool {
	return code == 301 || code == 302 || code == 303 || code == 307 || code == 308
}

func transferError(ctx context.Context, err error) error {
	if errors.Is(err, ErrBlocked) {
		return ErrBlocked
	}
	if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	var timeout net.Error
	if errors.Is(err, ErrTimeout) || errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout() {
		return ErrTimeout
	}
	return ErrFetch
}

type verifiedReader struct {
	ctx                  context.Context
	source               io.Reader
	max, declared, count int64
	expected             string
	hash                 hash.Hash
	done                 bool
	empty                int
	failure              error
}

func (r *verifiedReader) Read(p []byte) (n int, err error) {
	if r.failure != nil {
		return 0, r.failure
	}
	defer func() {
		if err != nil && err != io.EOF {
			r.failure = err
		}
	}()
	if err := r.ctx.Err(); err != nil {
		return 0, transferError(r.ctx, err)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.done {
		return 0, io.EOF
	}
	size := min(int64(len(p)), int64(BufferBytes), r.max-r.count+1)
	n, err = r.source.Read(p[:int(size)])
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return 0, transferError(r.ctx, ctxErr)
	}
	if n > 0 {
		r.count += int64(n)
		if r.count > r.max {
			return 0, ErrLimit
		}
		_, _ = r.hash.Write(p[:n])
		r.empty = 0
	} else if err == nil {
		r.empty++
		if r.empty >= 100 {
			return 0, ErrFetch
		}
	}
	if err == io.EOF {
		if r.declared >= 0 && r.count != r.declared {
			return n, ErrFetch
		}
		if r.expected != "" && r.expected != hex.EncodeToString(r.hash.Sum(nil)) {
			return n, ErrDigest
		}
		r.done = true
		return n, io.EOF
	}
	if err != nil {
		return n, transferError(r.ctx, err)
	}
	return n, nil
}
