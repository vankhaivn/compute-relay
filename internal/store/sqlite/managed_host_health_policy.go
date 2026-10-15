package sqlite

import (
	"github.com/vankhaivn/compute-relay/internal/hosthealth"
)

func managedHostHealthPolicy(providerType string, raw []byte) (*hosthealth.Policy, error) {
	if providerType != "ssh" {
		if len(raw) > 0 {
			return nil, ErrCorrupt
		}
		return nil, nil
	}
	p := hosthealth.LegacyPolicyV1()
	if len(raw) > 0 {
		if len(raw) > 2048 {
			return nil, ErrCorrupt
		}
		// Reuse the complete-object decoder to reject partial, unknown and null fields.
		wrapped := append([]byte(`{"host_health":`), raw...)
		wrapped = append(wrapped, '}')
		var err error
		p, err = hosthealth.PolicyFromRuntime(wrapped)
		if err != nil {
			return nil, ErrCorrupt
		}
	}
	return &p, nil
}
