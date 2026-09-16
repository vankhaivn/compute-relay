package devtool

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const faultProposalHeading = "### 23.2 Required fault scenarios"

// Read the numbered list in the approved section, not matching prose elsewhere
// in the proposal. A new/reordered/removed case requires an explicit catalog
// update; a substring, appendix or stale duplicate must not qualify the matrix.
// This intentionally accepts the repository's simple list format, not arbitrary
// Markdown. LF and CRLF checkouts have identical meaning.
func proposalFaultScenarios(proposal []byte) ([]string, error) {
	lines := strings.Split(string(proposal), "\n")
	start := -1
	for i, line := range lines {
		if strings.TrimSpace(line) != faultProposalHeading {
			continue
		}
		if start != -1 {
			return nil, errors.New("duplicate proposal fault section")
		}
		start = i + 1
	}
	if start == -1 {
		return nil, errors.New("missing proposal fault section 23.2")
	}
	var scenarios []string
	intro := false
	for _, raw := range lines[start:] {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "#") {
			break
		}
		if line == "" {
			continue
		}
		if line == "Test at least:" && len(scenarios) == 0 && !intro {
			intro = true
			continue
		}
		prefix := strconv.Itoa(len(scenarios)+1) + ". "
		if !intro || !strings.HasPrefix(line, prefix) || strings.TrimSpace(strings.TrimPrefix(line, prefix)) == "" {
			return nil, fmt.Errorf("invalid numbered proposal fault case %d", len(scenarios)+1)
		}
		scenarios = append(scenarios, strings.TrimPrefix(line, prefix))
		if len(scenarios) > 25 {
			return nil, errors.New("proposal fault section contains more than 25 cases")
		}
	}
	if len(scenarios) != 25 {
		return nil, errors.New("proposal fault section must contain exactly 25 numbered cases")
	}
	return scenarios, nil
}
