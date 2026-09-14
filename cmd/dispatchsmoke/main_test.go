package main

import "testing"

func TestOfflineDispatchSmoke(t *testing.T) {
	if err := run(); err != nil {
		t.Fatal(err)
	}
}
