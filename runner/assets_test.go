package runnerassets

import (
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
