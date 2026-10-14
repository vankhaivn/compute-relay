package sqlite

import "testing"

func requireNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func snapshotTableCounts(t testing.TB, s *Store, tables ...string) map[string]int {
	t.Helper()
	counts := make(map[string]int, len(tables))
	for _, table := range tables {
		counts[table] = controlCount(t, s, "SELECT count(*) FROM "+table)
	}
	return counts
}

func requireUnchangedTableCounts(t testing.TB, s *Store, before map[string]int) {
	t.Helper()
	for table, count := range before {
		if got := controlCount(t, s, "SELECT count(*) FROM "+table); got != count {
			t.Fatalf("%s rows changed: %d to %d", table, count, got)
		}
	}
}
