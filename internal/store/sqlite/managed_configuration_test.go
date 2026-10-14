package sqlite

import (
	"context"
	"fmt"
	"testing"

	"github.com/vankhaivn/compute-relay/internal/connections"
)

func configureBody(revision, wall int64) []byte {
	return []byte(fmt.Sprintf(`{"action":"configure","expected_revision":%d,"configuration":{"max_remote_wall_seconds":%d}}`, revision, wall))
}

func configureConnection(t testing.TB, f *managedStateFixture, current connections.Connection, key string, wall int64) connections.Operation {
	t.Helper()
	op, err := f.service.Submit(context.Background(), f.principals["a"], "a", current.ID, key, configureBody(current.Revision, wall))
	if err != nil || op.Status != "succeeded" || op.Action != "configure" {
		t.Fatal("configuration did not commit locally", op, err)
	}
	return op
}
