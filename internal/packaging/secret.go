package packaging

import (
	"bytes"
	"regexp"
)

var secretPatterns = []*regexp.Regexp{
	regexp.MustCompile(`-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----`),
	regexp.MustCompile(`(?:KAGGLE_API_TOKEN|KAGGLE_KEY)["']?\s*[:=]\s*["']?[A-Za-z0-9_./+-]{16,}`),
	regexp.MustCompile(`cr1_[A-Za-z0-9_-]{43}`),
	regexp.MustCompile(`gh[pousr]_[A-Za-z0-9]{20,}`),
	regexp.MustCompile(`AKIA[A-Z0-9]{16}`),
}

// The bounded overlap catches obvious credentials crossing read boundaries. Detection is
// heuristic, not a general secret scanner; no matched bytes are included in errors.
type secretScanner struct{ tail []byte }

func (s *secretScanner) check(chunk []byte) error {
	b := make([]byte, 0, len(s.tail)+len(chunk))
	b = append(b, s.tail...)
	b = append(b, chunk...)
	for _, pattern := range secretPatterns {
		if pattern.Match(b) {
			return ErrSecret
		}
	}
	if bytes.Contains(b, []byte("-----BEGIN PGP PRIVATE KEY BLOCK-----")) {
		return ErrSecret
	}
	start := max(0, len(b)-512)
	s.tail = append(s.tail[:0], b[start:]...)
	return nil
}
