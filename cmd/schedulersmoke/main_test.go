package main

import "testing"

// Runs as an ordinary credential-free component test with the pinned driver.
func TestRun(t *testing.T) {
	if err := run(); err != nil {
		t.Fatal(err)
	}
}
