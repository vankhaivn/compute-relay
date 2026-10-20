package sqlite

import (
	"github.com/vankhaivn/compute-relay/internal/retention"
	"time"
)

func assessInputRetention(p retention.Policy, now, recorded time.Time, refs []retention.Reference) (retention.Decision, error) {
	if _, err := p.Assess(now, recorded, refs); err != nil {
		return retention.Decision{}, err
	}
	allReleased := len(refs) > 0
	for i := range refs {
		if refs[i].InputReleased {
			refs[i].UpdatedAt = now.Add(-p.CompletedFor)
		} else {
			allReleased = false
		}
	}
	if allReleased {
		recorded = now.Add(-p.CompletedFor)
	}
	return p.Assess(now, recorded, refs)
}
