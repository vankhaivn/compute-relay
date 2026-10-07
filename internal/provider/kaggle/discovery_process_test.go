package kaggle

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDiscoveryProcessIsolationAndBoundedFailures(t *testing.T) {
	python := pythonForTest(t)
	// macOS /usr/bin/python3 is a developer-tool launcher that injects build
	// variables. Select its actual interpreter, as production configuration does.
	actual, err := exec.Command(python, "-I", "-c", "import sys;print(sys.executable)").Output()
	if err != nil || !filepath.IsAbs(strings.TrimSpace(string(actual))) {
		t.Fatal("cannot identify test interpreter")
	}
	python = strings.TrimSpace(string(actual))
	for _, name := range []string{"KAGGLE_API_TOKEN", "KAGGLE_USERNAME", "KAGGLE_KEY", "KAGGLE_CONFIG_DIR", "PYTHONPATH", "HTTPS_PROXY", "SSL_CERT_FILE"} {
		t.Setenv(name, "AMBIENT_CANARY")
	}
	raw, _ := json.Marshal(discoveryVerified())
	source := `import os,sys
assert sys.flags.isolated
allowed = {'HOME','USERPROFILE','TMPDIR','TMP','TEMP','LANG','LC_CTYPE','__CF_USER_TEXT_ENCODING'}
# Python normalizes Windows environment variable names to uppercase.
if os.name == 'nt':
    allowed |= {'SYSTEMROOT','WINDIR'}
assert set(os.environ) <= allowed
assert sys.stdin.buffer.read()==b'SYNTHETIC_STDIN'
assert len(sys.argv)==1
assert os.path.samefile(os.getcwd(),os.environ['HOME'])
print('` + string(raw) + `')
`
	if r, err := runDiscoverySource(context.Background(), python, []byte("SYNTHETIC_STDIN"), source); err != nil || r != discoveryVerified() {
		t.Fatal("discovery subprocess isolation failed", err)
	}
	for _, source := range []string{`print('SYNTHETIC_SECRET')`, `print('x'*20000)`, `import sys;sys.stderr.write('x'*20000)`, `raise ValueError('SYNTHETIC_SECRET')`} {
		if _, err := runDiscoverySource(context.Background(), python, nil, source); err != ErrProcess && err != ErrProtocol {
			t.Fatal("unbounded or raw failure", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = runDiscoverySource(ctx, python, nil, `import time;time.sleep(60)`)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 3*time.Second {
		t.Fatal("unbounded timeout", err)
	}
}

func TestDiscoveryEmbeddedQuotaDecoderAndStrictProtocol(t *testing.T) {
	python := pythonForTest(t)
	for _, tc := range []struct{ raw, status, limit, used, reserved string }{
		{`{"gpuQuota":{"totalTimeAllowed":"19.999999999s","timeUsed":"2.000000001s","timeReserved":"3.000000001s"}}`, "known", "19999999999", "2000000001", "3000000001"},
		{`{}`, "unknown", "", "", ""},
		{`{"gpuQuota":{"totalTimeAllowed":"10s","timeUsed":"0s"}}`, "unknown", "", "", ""},
		{`{"gpuQuota":{"isPayToScaleEnabled":true}}`, "unknown", "", "", ""},
	} {
		program := discoveryProgram()
		source := strings.Replace(program, "\nif __name__ == \"__main__\":", "\nmain=lambda:verified('fixture_user', quota_decoder.quota_result(json.loads("+strconv.Quote(tc.raw)+")))\nif __name__ == \"__main__\":", 1)
		if source == program {
			t.Fatal("fixture failed to replace network entry point")
		}
		r, err := runDiscoverySource(context.Background(), python, []byte("SYNTHETIC_TOKEN"), source)
		if err != nil || r.Account != "fixture_user" || r.QuotaStatus != tc.status || r.LimitNS != tc.limit || r.UsedNS != tc.used || r.ReservedNS != tc.reserved {
			t.Fatal("embedded discovery quota decoder failed", err)
		}
	}
	raw, _ := json.Marshal(discoveryVerified())
	for _, bad := range []string{
		strings.Replace(string(raw), `"protocol":1`, `"protocol":1,"protocol":1`, 1),
		strings.Replace(string(raw), `"account":"fixture_user"`, `"account":null`, 1),
		strings.Replace(string(raw), `"account":`, `"Account":`, 1),
		strings.Replace(string(raw), `"reserved_ns":""`, `"reserved_ns":"0"`, 1),
		strings.Replace(string(raw), `"status":"verified"`, `"status":"credential_rejected"`, 1),
		string(raw) + "{}",
	} {
		if _, err := runDiscoverySource(context.Background(), python, nil, "print("+strconv.Quote(bad)+")"); err != ErrProtocol {
			t.Fatal("invalid private wire response accepted", err)
		}
	}
}

func TestDiscoveryIndependentWatchdogDoesNotNeedParentCancellation(t *testing.T) {
	program := discoveryProgram()
	source := strings.Replace(program, "\nif __name__ == \"__main__\":", "\nimport time\nmain=lambda:time.sleep(60)\nif __name__ == \"__main__\":", 1)
	source = strings.Replace(source, "\n    raise SystemExit(bounded_main())", "\n    raise SystemExit(bounded_main(0.05))", 1)
	started := time.Now()
	_, err := runDiscoverySource(context.Background(), pythonForTest(t), nil, source)
	if err != ErrProcess || time.Since(started) > 3*time.Second {
		t.Fatal("independent watchdog failed", err)
	}
}
