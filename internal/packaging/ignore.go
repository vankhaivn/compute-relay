package packaging

import (
	"bufio"
	"path"
	"strings"
)

func defaultExcluded(name string) bool {
	for _, part := range strings.Split(strings.ToLower(name), "/") {
		switch part {
		case ".git", ".hg", ".svn", ".ssh", ".aws", ".azure", ".kaggle", ".gnupg", ".config", ".cache", ".venv", "venv", "node_modules", "__pycache__", ".pytest_cache", ".mypy_cache", ".idea", ".vscode", ".ds_store", "dist", "build", "target", ".compute-relay", ".compute-connector", ".computeignore", "kaggle.json", "access_token", "credentials", "credentials.json", "id_rsa", "id_ed25519", ".netrc", ".npmrc", ".pypirc":
			return true
		}
		if strings.HasPrefix(part, ".env") || strings.HasSuffix(part, ".pem") || strings.HasSuffix(part, ".key") || strings.HasSuffix(part, ".pyc") {
			return true
		}
	}
	return false
}

type ignoreRules []string

// This is a deliberately small exclusion-only grammar, not a claim of Git compatibility.
// A slashless glob matches a component at any depth; slash patterns are root-relative.
// ** matches whole components. ! reinclusion is rejected, not silently ignored.
func parseIgnore(content string) (ignoreRules, error) {
	if len(content) > 64<<10 {
		return nil, ErrLimit
	}
	var rules ignoreRules
	scan := bufio.NewScanner(strings.NewReader(content))
	scan.Buffer(make([]byte, 1024), 2048)
	for scan.Scan() {
		rule := strings.TrimSpace(scan.Text())
		if rule == "" || strings.HasPrefix(rule, "#") {
			continue
		}
		if len(rules) >= 256 || len(rule) > 1024 {
			return nil, ErrLimit
		}
		if strings.HasPrefix(rule, "!") || strings.Contains(rule, `\`) || strings.HasPrefix(rule, "/") {
			return nil, ErrInvalid
		}
		rule = strings.TrimSuffix(rule, "/")
		if len(strings.Split(rule, "/")) > 64 {
			return nil, ErrLimit
		}
		for _, part := range strings.Split(rule, "/") {
			if part == "" || part == "." || part == ".." {
				return nil, ErrInvalid
			}
			if strings.Contains(part, "**") && part != "**" {
				return nil, ErrInvalid
			}
			if _, err := path.Match(part, ""); err != nil {
				return nil, ErrInvalid
			}
		}
		rules = append(rules, rule)
	}
	if scan.Err() != nil {
		return nil, ErrLimit
	}
	return rules, nil
}
func (rules ignoreRules) excludes(name string) bool {
	if defaultExcluded(name) {
		return true
	}
	parts := strings.Split(name, "/")
	for _, rule := range rules {
		if !strings.Contains(rule, "/") {
			for _, part := range parts {
				if ok, _ := path.Match(rule, part); ok {
					return true
				}
			}
		} else if matchParts(strings.Split(rule, "/"), parts) {
			return true
		}
	}
	return false
}
func matchParts(pattern, parts []string) bool {
	// Dynamic programming avoids exponential ** backtracking on attacker-supplied rules.
	dp := make([]bool, len(parts)+1)
	dp[0] = true
	for _, token := range pattern {
		next := make([]bool, len(parts)+1)
		if token == "**" {
			next[0] = dp[0]
			for j := 1; j <= len(parts); j++ {
				next[j] = dp[j] || next[j-1]
			}
		} else {
			for j := 1; j <= len(parts); j++ {
				ok, _ := path.Match(token, parts[j-1])
				next[j] = dp[j-1] && ok
			}
		}
		dp = next
	}
	// Any matched prefix excludes that directory and its descendants.
	for _, matched := range dp {
		if matched {
			return true
		}
	}
	return false
}
