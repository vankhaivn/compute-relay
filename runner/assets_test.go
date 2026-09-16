package runnerassets

import (
	"io/fs"
	"reflect"
	"testing"
)

func TestSourcesMatchLockedRunnerAndAreIndependent(t *testing.T) {
	first, err := Sources()
	if err != nil || len(first) != 5 {
		t.Fatal("locked runner unavailable", err)
	}
	second, err := Sources()
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatal("runner snapshot is not stable", err)
	}
	first["main.py"] = "untrusted replacement"
	delete(first, "files.py")
	third, err := Sources()
	if err != nil || !reflect.DeepEqual(second, third) {
		t.Fatal("caller changed embedded runner", err)
	}
}

func TestEmbeddedAssetsExcludeCachesAndUnrelatedFiles(t *testing.T) {
	want := map[string]bool{"assets.lock.json": true}
	for _, name := range sourceNames {
		want["python/relay_runner/"+name] = true
	}
	count := 0
	err := fs.WalkDir(assets, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			count++
			if !want[path] {
				t.Errorf("unexpected embedded file: %s", path)
			}
		}
		return nil
	})
	if err != nil || count != len(want) {
		t.Fatal("embedded asset inventory differs", count, err)
	}
}
