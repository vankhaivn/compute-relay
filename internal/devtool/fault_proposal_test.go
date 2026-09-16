package devtool

import (
	"fmt"
	"strings"
	"testing"
)

func proposalList() string {
	var b strings.Builder
	fmt.Fprint(&b, faultProposalHeading+"\n\nTest at least:\n\n")
	for i := 1; i <= 25; i++ {
		fmt.Fprintf(&b, "%d. Case %d.\n", i, i)
	}
	fmt.Fprintln(&b, "\n### 23.3 Critical invariants to assert")
	return b.String()
}

func TestProposalFaultScenariosRequiresExactSection(t *testing.T) {
	for _, mode := range []string{"pass", "crlf", "appendix", "missing-heading", "duplicate-heading", "missing-case", "extra-case", "duplicate-number", "wrong-number", "empty-case", "partial-section", "wrapped-case", "repeated-intro"} {
		t.Run(mode, func(t *testing.T) {
			raw := proposalList()
			switch mode {
			case "crlf":
				raw = strings.ReplaceAll(raw, "\n", "\r\n")
			case "appendix":
				raw += "\n1. This appendix is not matrix evidence.\n"
			case "missing-heading":
				raw = strings.Replace(raw, faultProposalHeading, "### Other section", 1)
			case "duplicate-heading":
				raw += "\n" + proposalList()
			case "missing-case":
				raw = strings.Replace(raw, "25. Case 25.\n", "", 1)
			case "extra-case":
				raw = strings.Replace(raw, "25. Case 25.\n", "25. Case 25.\n26. Added case.\n", 1)
			case "duplicate-number":
				raw = strings.Replace(raw, "2. Case 2.", "1. Case 2.", 1)
			case "wrong-number":
				raw = strings.Replace(raw, "1. Case 1.", "2. Case 1.", 1)
			case "empty-case":
				raw = strings.Replace(raw, "25. Case 25.", "25. ", 1)
			case "partial-section":
				raw = strings.Replace(raw, "25. Case 25.", "### Another section\n25. Case 25.", 1)
			case "wrapped-case":
				raw = strings.Replace(raw, "1. Case 1.", "1. Case\n1.", 1)
			case "repeated-intro":
				raw = strings.Replace(raw, "Test at least:", "Test at least:\nTest at least:", 1)
			}
			cases, err := proposalFaultScenarios([]byte(raw))
			good := mode == "pass" || mode == "crlf" || mode == "appendix"
			if (err == nil) != good {
				t.Fatal("incorrect proposal qualification", err)
			}
			if good {
				for i, c := range cases {
					if c != fmt.Sprintf("Case %d.", i+1) {
						t.Fatal("case identity changed", c)
					}
				}
			}
		})
	}
}
