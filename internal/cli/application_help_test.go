package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestMainRoutesApplicationAndProfileHelpWithoutState(t *testing.T) {
	for _, command := range []string{"profile", "object", "job", "operation"} {
		var out, diagnostic bytes.Buffer
		if RunContext(context.Background(), []string{command, "--help"}, &out, &diagnostic) != 0 || out.Len() == 0 || diagnostic.Len() != 0 {
			t.Fatal("main route did not provide inert help", command)
		}
		out.Reset()
		if Run([]string{command, "status", "--token", "SYNTHETIC_SECRET"}, &out, &diagnostic) != 2 || out.Len() != 0 || strings.Contains(diagnostic.String(), "SYNTHETIC_SECRET") {
			t.Fatal("main route accepted or reflected invalid arguments", command)
		}
	}
}
