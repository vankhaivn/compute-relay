package httpsinput

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
	"testing"
)

func TestURLPolicy(t *testing.T) {
	for _, raw := range []string{"https://example.org/data", "https://EXAMPLE.org:443?q=version&v=1", "https://93.184.216.34/data", "https://[2606:4700:4700::1111]/data", "https://example.org/data%20file"} {
		if _, err := parseURL(raw); err != nil {
			t.Fatalf("public input %q: %v", raw, err)
		}
	}
	for _, raw := range []string{
		"", "http://example.org/file", "file:///tmp/file", "ftp://example.org", "//example.org", "https:example.org",
		"https://user:password@example.org/file", "https://@example.org", "https://example.org:444/file", "https://example.org:0443", "https://example.org:",
		"https://localhost", "https://metadata.google.internal/", "https://host.local/", "https://router.home.arpa/", "https://example.org./file",
		"https://127.0.0.1/", "https://127.1/", "https://2130706433/", "https://0177.0.0.1/", "https://0x7f000001/", "https://0x7f.0.0.1/",
		"https://[::ffff:127.0.0.1]/", "https://[::ffff:93.184.216.34]/", "https://[fe80::1%25eth0]/", "https://[93.184.216.34]/",
		"https://example.org\\@127.0.0.1/", "https://example.org/file#fragment", "https://example.org/file#", "https://example.org/file?",
		"https://example.org/a\nb", " https://example.org", "https://exämple.org/", "https://example.org/%0a", "https://example.org/%5c", "https://example.org?x=%00",
		"https://example.org?x=%", "https://example.org?token=synthetic", "https://example.org?%61ccess_token=x", "https://example.org?X-Amz-Signature=x",
		"https://example.org?X-Goog-Credential=x", "https://example.org?sig=synthetic", "https://example.org/" + strings.Repeat("a", MaxURLBytes),
	} {
		if _, err := parseURL(raw); err == nil {
			t.Fatalf("unsafe input accepted: %q", raw)
		}
	}
}

func TestSpecialAddressPolicy(t *testing.T) {
	for _, raw := range []string{"0.1.2.3", "10.0.0.1", "100.64.0.1", "100.100.100.200", "127.0.0.1", "169.254.169.254", "172.16.0.1", "172.31.255.255", "192.0.0.9", "192.0.2.1", "192.168.1.1", "192.88.99.1", "198.18.0.1", "198.51.100.1", "203.0.113.1", "224.1.1.1", "255.255.255.255", "168.63.129.16", "::", "::1", "fc00::1", "fe80::1", "ff02::1", "64:ff9b::a00:1", "64:ff9b:1::1", "100::1", "2001::1", "2001:2::1", "2001:db8::1", "2002:7f00:1::", "3fff::1", "5f00::1", "::ffff:8.8.8.8", "2606:4700::1%eth0"} {
		if publicIP(netip.MustParseAddr(raw)) {
			t.Fatal("special address accepted", raw)
		}
	}
	for _, prefix := range denied {
		if publicIP(prefix.Addr()) {
			t.Fatal("denied block accepted", prefix)
		}
	}
	for _, raw := range []string{"93.184.216.34", "8.8.8.8", "172.32.0.1", "2606:4700:4700::1111", "2001:4860:4860::8888"} {
		if !publicIP(netip.MustParseAddr(raw)) {
			t.Fatal("public address rejected", raw)
		}
	}
	if publicIP(netip.Addr{}) {
		t.Fatal("invalid address accepted")
	}
}

func TestStrictRequestAndRedaction(t *testing.T) {
	good := `{"url":"https://example.org/file?q=synthetic-canary","sha256":"` + strings.Repeat("a", 64) + `"}`
	var r Request
	if err := json.Unmarshal([]byte(good), &r); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(fmt.Sprintf("%v %+v %#v", r, r, r), "canary") {
		t.Fatal("ordinary formatting leaked input")
	}
	for _, raw := range []string{
		`null`, `[]`, `{}`, `{"URL":"https://example.org"}`, `{"url":"https://example.org","url":"https://example.org"}`,
		`{"url":null}`, `{"url":12}`, `{"url":"https://example.org","headers":{}}`, `{"url":"https://example.org","sha256":null}`,
		`{"url":"https://example.org","sha256":""}`, `{"url":"https://example.org","sha256":"BAD"}`, good + ` {}`,
	} {
		before := r
		if err := json.Unmarshal([]byte(raw), &r); err == nil {
			t.Fatal("invalid request accepted", raw)
		}
		if r != before {
			t.Fatal("failed decoding mutated request")
		}
	}
}

func TestConfigBoundsAndAllowlistSnapshot(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.MaxBytes = 0 }, func(c *Config) { c.MaxBytes = 1 << 41 }, func(c *Config) { c.MaxRedirects = 11 },
		func(c *Config) { c.MaxConcurrent = 0 }, func(c *Config) { c.HeaderTimeout = 0 }, func(c *Config) { c.TotalTimeout = 1 },
		func(c *Config) { c.AllowedHosts = []string{"*.example.org"} }, func(c *Config) { c.AllowedHosts = []string{"example.org:443"} },
		func(c *Config) { c.AllowedHosts = []string{"127.0.0.1"} }, func(c *Config) { c.AllowedHosts = []string{"example.org/path"} },
	} {
		cfg := DefaultConfig()
		mutate(&cfg)
		if _, err := New(cfg); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	cfg := DefaultConfig()
	cfg.AllowedHosts = []string{"EXAMPLE.org"}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.AllowedHosts[0] = "other.org"
	if !c.hosts["example.org"] || c.hosts["other.org"] {
		t.Fatal("allowlist aliases caller memory")
	}
}

func FuzzRequestPolicy(f *testing.F) {
	for _, seed := range []string{`{"url":"https://example.org/file"}`, `{"url":"https://127.0.0.1"}`, `null`, `{"url":"https://example.org","url":"https://other.org"}`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 32768 {
			t.Skip()
		}
		var r Request
		if json.Unmarshal([]byte(raw), &r) == nil && r.Validate() != nil {
			t.Fatal("decoder/validator disagreement")
		}
	})
}
