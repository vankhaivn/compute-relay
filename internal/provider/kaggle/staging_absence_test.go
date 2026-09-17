package kaggle

import (
	"bytes"
	"os/exec"
	"strconv"
	"testing"
)

func TestEmbeddedStagingDatasetAbsenceClassificationIsNarrow(t *testing.T) {
	python := pythonForTest(t)
	script := "ns={'__name__':'absence_fixture'}\nexec(" + strconv.Quote(stagingSource) + ",ns)\n" + `
class Response:
    def __init__(self, status, content):
        self.status_code = status
        self._content = content
class Error:
    def __init__(self, status, content):
        self.response = Response(status, content)
missing = b'{"error":{"code":403,"message":"Permission \'datasets.get\' was denied","status":"PERMISSION_DENIED"}}'
other = b'{"error":{"code":403,"message":"Permission \'datasets.list\' was denied","status":"PERMISSION_DENIED"}}'
assert ns['missing_dataset_error'](Error(404, b''))
assert ns['missing_dataset_error'](Error(403, missing))
assert not ns['missing_dataset_error'](Error(403, other))
assert not ns['missing_dataset_error'](Error(403, b'{"error":{"code":403,"message":"Permission \'datasets.get\' was denied","status":"OTHER"}}'))
assert not ns['missing_dataset_error'](Error(403, b'not-json'))
assert not ns['missing_dataset_error'](Error(503, missing))
print('ok')
`
	cmd := exec.Command(python, "-I", "-c", script)
	var out, diagnostic bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &diagnostic
	if err := cmd.Run(); err != nil || out.String() != "ok\n" {
		t.Fatal("embedded dataset absence classification failed", err, out.String(), diagnostic.String())
	}
}
