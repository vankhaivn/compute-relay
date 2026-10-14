package cli

import (
	"bytes"
	"testing"
)

func TestRemovedApplicationCommandsAreRejected(t *testing.T) {
	for _, command := range []string{"object", "job", "operation", "artifact", "profile"} {
		var out, diagnostic bytes.Buffer
		if Run([]string{command, "--help"}, &out, &diagnostic) != 2 || out.Len() != 0 {
			t.Fatal("removed command remained available", command)
		}
	}
}
