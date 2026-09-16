package kaggleacceptance

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"sort"
	"time"

	gpusmoke "github.com/vankhaivn/compute-relay/examples/jobs/gpu-smoke"
	"github.com/vankhaivn/compute-relay/internal/packaging"
	"github.com/vankhaivn/compute-relay/internal/provider"
)

const jobSpecification = `{"api_version":"compute-connector/v1alpha1","name":"bounded-gpu-acceptance","profile":"acceptance","bundle":{"object_id":"acceptance_code"},"execution":{"kind":"python","command":["python","main.py"]},"inputs":[{"name":"challenge","source":{"kind":"object","object_id":"acceptance_input"},"target":"challenge.json"}],"outputs":[{"path":"result.json","kind":"file","required":true,"max_bytes":4096},{"path":"hardware.json","kind":"file","required":true,"max_bytes":4096}],"resources":{"accelerator":"gpu","minimum_gpu_count":1},"network":{"remote_internet":"disabled"},"timeouts":{"remote_wall_seconds":120,"setup_seconds":30,"finalization_grace_seconds":15}}`

func inputBytes(challenge string) ([]byte, error) {
	return json.Marshal(struct {
		Schema int `json:"schema"`
		Challenge string `json:"challenge"`
	}{1, challenge})
}
func bundleBytes() ([]byte, error) {
	sources := gpusmoke.Sources()
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	manifest := packaging.Manifest{Version:packaging.Version, Files:[]packaging.File{}}
	for _, name := range names {
		data := []byte(sources[name])
		manifest.Files = append(manifest.Files, packaging.File{Path:name, Bytes:int64(len(data)), SHA256:string(provider.Digest(data))})
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	var result bytes.Buffer
	gz := gzip.NewWriter(&result)
	gz.Header.OS = 255
	tarball := tar.NewWriter(gz)
	write := func(name string, content []byte) error {
		if err := tarball.WriteHeader(&tar.Header{Name:name, Size:int64(len(content)), Mode:0644, Typeflag:tar.TypeReg, ModTime:time.Unix(0,0).UTC(), Format:tar.FormatUSTAR}); err != nil {
			return err
		}
		_, err := tarball.Write(content)
		return err
	}
	if err := write(packaging.ManifestPath, raw); err != nil {
		return nil, err
	}
	for _, name := range names {
		if err := write("code/"+name, []byte(sources[name])); err != nil {
			return nil, err
		}
	}
	if err := tarball.Close(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return result.Bytes(), nil
}
