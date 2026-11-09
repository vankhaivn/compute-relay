package hostidentity

import (
	"regexp"
	"strings"
)

var platformUUID = regexp.MustCompile(`(?m)^\s*"IOPlatformUUID"\s*=\s*"([0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12})"\s*$`)

// DarwinPlatform digests the one IOPlatformUUID in `ioreg -rd1 -c IOPlatformExpertDevice`
// output. It refuses missing, repeated and all-zero identifiers.
func DarwinPlatform(ioreg []byte) (string, bool) {
	matches := platformUUID.FindAllSubmatch(ioreg, -1)
	if len(matches) != 1 {
		return "", false
	}
	id := strings.ToLower(string(matches[0][1]))
	if strings.ReplaceAll(id, "-", "") == strings.Repeat("0", 32) {
		return "", false
	}
	return Digest("darwin-platform-uuid", id), true
}
