package hosthealth

import (
	"regexp"
	"strings"
)

var (
	grantDefaultsHeader = regexp.MustCompile(`^Matching Defaults entries for ([A-Za-z0-9_.@-]+) on ([A-Za-z0-9_.@-]+):$`)
	grantCommandsHeader = regexp.MustCompile(`^User ([A-Za-z0-9_.@-]+) may run the following commands(?: on ([A-Za-z0-9_.@-]+))?:$`)
	grantRule           = regexp.MustCompile(`^\((root|ALL|ALL[ \t]*:[ \t]*ALL)\)[ \t]+(.+)$`)
	grantEnvName        = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	grantLiteralPath    = regexp.MustCompile(`^/[A-Za-z0-9_./-]+$`)
)

// ClassifyGrant describes passwordless command authority, not all listed access.
// sudoers applies the last matching rule; listpw=any allows a passwordless listing
// even when other rules require authentication. Recognize only complete simple
// listings with one final sampler grant. Unknown Defaults, tags and wrapping stay
// broader rather than implying that other listed commands require a password.
func ClassifyGrant(sample Sample) string {
	text := sourceText(sample)
	if text == "" {
		return "absent"
	}
	for _, ch := range text {
		if ch < ' ' && ch != '\n' && ch != '\t' {
			return "broader"
		}
	}
	var rules []string
	defaults, defaultsRead, commands := false, false, false
	var defaultsUser, defaultsHost string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		switch {
		case grantDefaultsHeader.MatchString(line):
			if defaults || commands || len(rules) != 0 {
				return "broader"
			}
			defaults = true
			header := grantDefaultsHeader.FindStringSubmatch(line)
			defaultsUser, defaultsHost = header[1], header[2]
		case grantCommandsHeader.MatchString(line):
			if commands || len(rules) != 0 || defaults && !defaultsRead {
				return "broader"
			}
			header := grantCommandsHeader.FindStringSubmatch(line)
			if header[1] == "root" || defaults && (header[1] != defaultsUser || header[2] != defaultsHost) {
				return "broader"
			}
			commands = true
		case strings.HasPrefix(line, "("):
			if defaults && !commands {
				return "broader"
			}
			rules = append(rules, line)
		case defaults && !commands && !defaultsRead:
			if !grantSafeDefaults(line) {
				return "broader"
			}
			defaultsRead = true
		default:
			return "broader"
		}
	}
	for i, rule := range rules {
		parts := grantRule.FindStringSubmatch(rule)
		if len(parts) != 3 {
			return "broader"
		}
		command := parts[2]
		if command == "ALL" || command == "PASSWD: ALL" {
			continue
		}
		if i == len(rules)-1 && (command == "NOPASSWD: /usr/bin/powermetrics" || command == "NOPASSWD: /usr/bin/powermetrics --samplers smc -n 1 -i 1000") {
			return "narrow"
		}
		return "broader"
	}
	return "broader"
}

// Only these fully parsed Defaults are known not to remove authentication from
// ordinary rules. In particular, !authenticate and exempt_group are not ignored.
func grantSafeDefaults(line string) bool {
	for _, entry := range strings.Split(line, ",") {
		entry = strings.TrimSpace(entry)
		switch entry {
		case "env_reset", "authenticate", "!log_allowed", "log_allowed", "listpw=any", "listpw=all", "listpw=always", "listpw=never":
			continue
		}
		if strings.HasPrefix(entry, "lecture_file=") && grantLiteralPath.MatchString(strings.TrimPrefix(entry, "lecture_file=")) {
			continue
		}
		if !strings.HasPrefix(entry, "env_keep+=") {
			return false
		}
		value := strings.TrimPrefix(entry, "env_keep+=")
		quoted := len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"'
		if quoted {
			value = value[1 : len(value)-1]
		}
		names := strings.Fields(value)
		if len(names) == 0 || !quoted && len(names) != 1 {
			return false
		}
		for _, name := range names {
			if !grantEnvName.MatchString(name) {
				return false
			}
		}
	}
	return true
}
