package sqlite

import (
	"context"
	"testing"
)

// The 16 MiB provider/restart scenario lives in runtimehost. Keep the SQL boundary
// independent of file size: a partial stream retains its pin and publishes no rows.
func TestPartialTransferKeepsPinWithoutPublicationRows(t *testing.T) {
	f, _, p, blobs := collectionFixture(t)
	p.mode = "partial"
	if worked, err := collectionEngine(t, f, p, blobs, f.s).RunOnce(context.Background()); !worked || err == nil {
		t.Fatal("fault did not execute", err)
	}
	if controlCount(t, f.s, "SELECT count(*) FROM artifacts") != 0 || controlCount(t, f.s, "SELECT count(*) FROM collection_publications") != 0 || controlCount(t, f.s, "SELECT count(*) FROM collection_snapshots") != 1 {
		t.Fatal("partial transfer escaped publication or lost pin")
	}
}
