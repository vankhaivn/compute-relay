package sshhost

import (
	"encoding/base64"
	"os/exec"
	"slices"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/hostcache"
)

func TestEmbeddedHelpersArmFiniteLifetimeBeforeIO(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	// Intercept alarm registration, then abort before any filesystem or stdin
	// access. A missing/moved alarm reaches a forbidden I/O/dispatch sentinel.
	const probe = `import base64,os,signal,sys
class Armed(BaseException): pass
expected_seconds=int(sys.argv[2])
def forbidden(*args,**kwargs): raise AssertionError('I/O before finite alarm')
def alarm(seconds):
 assert seconds==expected_seconds, seconds
 raise Armed()
source=base64.b64decode(sys.argv[1]); mode=sys.argv[3]
signal.alarm=alarm
ns={'__name__':'lifetime_probe'}
if mode=='writer':
 os.open=forbidden
 try: exec(source,ns)
 except Armed: pass
 else: raise AssertionError('writer did not arm alarm')
else:
 exec(source,ns)
 for name in ('scan','volume_scan','volume_batch'): ns[name]=forbidden
 sys.argv=['verifier','source',mode,'WzRd' if mode=='volumes' else '4'] if mode!='staged' else ['verifier']
 try: ns['main']()
 except Armed: pass
 else: raise AssertionError('verifier did not arm alarm')
`
	for _, test := range []struct{ mode, seconds, source string }{
		{"writer", "7200", cacheWriterSource},
		{"staged", "3600", hostVerifierSource},
		{"volume", "3600", hostVerifierSource},
		{"volumes", "3600", hostVerifierSource},
	} {
		t.Run(test.mode, func(t *testing.T) {
			command := exec.Command(python, "-B", "-E", "-s", "-c", probe, base64.StdEncoding.EncodeToString([]byte(test.source)), test.seconds, test.mode)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("finite lifetime not armed before I/O: %v %s", err, output)
			}
		})
	}
}

func TestEveryCacheHelperRequestsAutomaticRemoval(t *testing.T) {
	h, repo := cacheHarness(t)
	h.prepare(t)
	var generation hostcache.Generation
	for _, g := range repo.active {
		generation = g
		break
	}
	if _, err := h.adapter.observeVolume(t.Context(), docker{exec: h.host, binary: "/usr/local/bin/docker"}, generation); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, call := range h.host.wireCalls {
		if !slices.Contains(call, "run") {
			continue // The stopped-container verifier shell is exercised separately.
		}
		kind := ""
		if slices.Contains(call, cacheWriterBootstrap) {
			kind = "writer"
			if !slices.Contains(call, "--name") {
				t.Fatal("cache writer lost its fixed generation name")
			}
		} else if i := slices.Index(call, hostVerifierBootstrap); i >= 0 && i+2 < len(call) {
			kind = call[i+2]
		}
		if kind != "" {
			seen[kind] = true
			if !slices.Contains(call, "--rm") {
				t.Fatalf("%s helper can survive successful exit", kind)
			}
		}
	}
	for _, kind := range []string{"writer", "volume", "volumes"} {
		if !seen[kind] {
			t.Fatalf("%s helper was not exercised", kind)
		}
	}
}
