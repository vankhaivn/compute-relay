// Package httpsinput downloads bounded public HTTPS inputs without ambient credentials.
// It has no provider, filesystem, workload-execution or durable-admission behavior.
package httpsinput

import (
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"net/url"
	"strings"
)

var (
	ErrURL     = errors.New("invalid public HTTPS input")
	ErrBlocked = errors.New("HTTPS destination is not permitted")
	ErrFetch   = errors.New("HTTPS input transfer failed")
	ErrTimeout = errors.New("HTTPS input time budget exceeded")
	ErrLimit   = errors.New("HTTPS input exceeds byte limit")
	ErrDigest  = errors.New("HTTPS input digest mismatch")
	ErrBusy    = errors.New("HTTPS input concurrency limit reached")
)

const MaxURLBytes = 4096

// Request supports public inputs only. Headers, cookies, userinfo and signed/private
// URL authentication are deliberately not a credential-delivery mechanism.
type Request struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256,omitempty"`
}

// String/GoString avoid accidentally logging query strings through ordinary formatting.
// JSON intentionally remains the request wire format, never an ordinary log field.
func (Request) String() string   { return "[HTTPS input redacted]" }
func (Request) GoString() string { return "[HTTPS input redacted]" }

// UnmarshalJSON rejects duplicate, case-aliased, unknown and null fields as well as
// trailing JSON. This is stricter than encoding/json's default struct field matching.
func (r *Request) UnmarshalJSON(data []byte) error {
	if len(data) > 6*MaxURLBytes+256 {
		return ErrURL
	}
	d := json.NewDecoder(strings.NewReader(string(data)))
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return ErrURL
	}
	var next Request
	seen := map[string]bool{}
	for d.More() {
		tok, err := d.Token()
		key, ok := tok.(string)
		if err != nil || !ok || seen[key] || (key != "url" && key != "sha256") {
			return ErrURL
		}
		seen[key] = true
		var value any
		if err := d.Decode(&value); err != nil {
			return ErrURL
		}
		text, ok := value.(string)
		if !ok || text == "" {
			return ErrURL
		}
		if key == "url" {
			next.URL = text
		} else {
			next.SHA256 = text
		}
	}
	if tok, err := d.Token(); err != nil || tok != json.Delim('}') {
		return ErrURL
	}
	if d.Decode(new(any)) != io.EOF || !seen["url"] || next.Validate() != nil {
		return ErrURL
	}
	*r = next
	return nil
}

func (r Request) Validate() error {
	if r.SHA256 != "" {
		if len(r.SHA256) != 64 {
			return ErrURL
		}
		for _, c := range r.SHA256 {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return ErrURL
			}
		}
	}
	_, err := parseURL(r.URL)
	return err
}

func parseURL(raw string) (*url.URL, error) {
	if !strings.HasPrefix(raw, "https://") || len(raw) > MaxURLBytes || strings.ContainsAny(raw, "\\#") {
		return nil, ErrURL
	}
	for _, c := range raw {
		if c <= 32 || c >= 127 {
			return nil, ErrURL
		}
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Opaque != "" || u.User != nil || u.Host == "" || u.ForceQuery {
		return nil, ErrURL
	}
	host := strings.ToLower(u.Hostname())
	if host == "" || strings.ContainsAny(host, "%\\") || strings.HasSuffix(u.Host, ":") || (u.Port() != "" && u.Port() != "443") {
		return nil, ErrURL
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if (ip.Is6() && !strings.HasPrefix(u.Host, "[")) || (ip.Is4() && strings.ContainsAny(u.Host, "[]")) {
			return nil, ErrURL
		}
		if !publicIP(ip) {
			return nil, ErrBlocked
		}
		if ip.Is6() {
			u.Host = "[" + host + "]"
		} else {
			u.Host = host
		}
	} else {
		if !dnsName(host) || strings.ContainsAny(u.Host, "[]") {
			return nil, ErrURL
		}
		for _, suffix := range []string{"localhost", "local", "internal", "home.arpa", "onion", "test", "invalid"} {
			if host == suffix || strings.HasSuffix(host, "."+suffix) {
				return nil, ErrBlocked
			}
		}
		u.Host = host
	}
	// Canonical port is 443, and TLS SNI/Host remain bound to the original DNS name.
	if u.Path == "" {
		u.Path = "/"
	}
	for _, c := range u.Path {
		if c < 32 || c == 127 || c == '\\' {
			return nil, ErrURL
		}
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, ErrURL
	}
	for key, values := range query {
		if credentialKey(key) {
			return nil, ErrURL
		}
		for _, text := range append([]string{key}, values...) {
			for _, c := range text {
				if c < 32 || c == 127 {
					return nil, ErrURL
				}
			}
		}
	}
	return u, nil
}

func dnsName(host string) bool {
	if len(host) > 253 || !strings.Contains(host, ".") {
		return false
	}
	labels := strings.Split(host, ".")
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	// Reject ambiguous inet_aton forms rather than let a resolver reinterpret them.
	for _, c := range labels[len(labels)-1] {
		if c >= 'a' && c <= 'z' {
			return true
		}
	}
	return false
}

func credentialKey(key string) bool {
	key = strings.ToLower(key)
	if strings.HasPrefix(key, "x-amz-") || strings.HasPrefix(key, "x-goog-") {
		return true
	}
	switch key {
	case "token", "access_token", "auth", "authorization", "api_key", "apikey", "key", "password", "secret", "signature", "sig", "awsaccesskeyid", "googleaccessid":
		return true
	}
	return false
}

// Conservative policy snapshot, 2026-09-14: reject all IANA special-purpose IPv4
// blocks (including their globally reachable exceptions), multicast/reserved space,
// transition/translation IPv6, and Azure's platform virtual IP. See ADR-0006.
var denied = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"), netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.31.196.0/24"), netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("192.175.48.0/24"), netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("168.63.129.16/32"),
	netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("2620:4f:8000::/48"),
	netip.MustParsePrefix("3fff::/20"),
}
var globalV6 = netip.MustParsePrefix("2000::/3")

func publicIP(ip netip.Addr) bool {
	if !ip.IsValid() || ip.Zone() != "" || ip.Is4In6() || !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false
	}
	if ip.Is6() && !globalV6.Contains(ip) {
		return false
	}
	for _, block := range denied {
		if block.Contains(ip) {
			return false
		}
	}
	return true
}
