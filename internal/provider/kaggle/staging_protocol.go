package kaggle

import (
	"bytes"
	"io"
	"strings"
)

// Pipe EOF cannot distinguish an acknowledged source from an upstream read/Close
// failure. Emit this trailer only after stagingBody validates every source EOF and
// Close. The helper must consume it and EOF before issuing CreateDataset.
const stagingUploadComplete = "\x00compute-relay/staging-upload-complete/v1\n"

func stagingCreateInput(prefix, marker []byte, payload io.Reader) io.Reader {
	return io.MultiReader(bytes.NewReader(prefix), bytes.NewReader(marker), payload,
		strings.NewReader(stagingUploadComplete))
}
