package kaggle

import (
	"bytes"
	"os/exec"
	"strconv"
	"testing"
)

func TestEmbeddedStagingGoogleAPIsDestinationIsScoped(t *testing.T) {
	python := pythonForTest(t)
	script := "ns={'__name__':'destination_fixture'}\nexec(" + strconv.Quote(stagingSource) + ",ns)\n" + `
accepted = (
    'https://storage.googleapis.com/bucket/object?signature=fixture',
    'https://www.googleapis.com/upload/storage/v1/b/kaggle-data-sets/o?uploadType=resumable&upload_id=fixture',
    'https://www.googleapis.com:443/upload/storage/v1/b/kaggle-data-sets/o?uploadType=resumable&upload_id=fixture',
)
rejected = (
    'http://www.googleapis.com/upload/storage/v1/b/kaggle-data-sets/o?uploadType=resumable',
    'https://www.googleapis.com/storage/v1/b/kaggle-data-sets/o',
    'https://www.googleapis.com/upload/drive/v3/files',
    'https://www.googleapis.com/upload/storage/v1/b',
    'https://www.googleapis.com.evil.invalid/upload/storage/v1/b/kaggle-data-sets/o',
    'https://user:secret@www.googleapis.com/upload/storage/v1/b/kaggle-data-sets/o',
    'https://www.googleapis.com:444/upload/storage/v1/b/kaggle-data-sets/o',
    'https://www.googleapis.com/upload/storage/v1/b/kaggle-data-sets/o#fragment',
)
for url in accepted:
    assert ns['signed_url'](url) == url
for url in rejected:
    try:
        ns['signed_url'](url)
    except ValueError:
        pass
    else:
        raise AssertionError(url)
print('ok')
`
	cmd := exec.Command(python, "-I", "-c", script)
	var out, diagnostic bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &diagnostic
	if err := cmd.Run(); err != nil || out.String() != "ok\n" {
		t.Fatal("embedded staging destination validation failed", err, out.String(), diagnostic.String())
	}
}
