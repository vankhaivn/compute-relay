// Package hostidentity derives stable opaque identities from verified host metadata.
package hostidentity

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// Digest preserves the scheduling identity namespace across host observation
// and durable lifecycle checks. Native identifiers remain private evidence.
func Digest(kind, native string) string {
	hash := hmac.New(sha256.New, []byte("compute-relay/scheduling-resource/v1"))
	_, _ = hash.Write([]byte(kind + "\x00" + native))
	return hex.EncodeToString(hash.Sum(nil))
}
