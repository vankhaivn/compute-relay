package hoststorage

import "time"

type Action string

const (
	OrphanContainer Action = "orphan_container"
	OrphanVolume    Action = "orphan_volume"
	OrphanImage     Action = "orphan_image"
	EvictCache      Action = "evict_cache"
	EvictImage      Action = "evict_image"
)

// Target is private immutable evidence selected by a durable cleanup journal.
// It is never serialized into public API receipts.
type Target struct {
	Scope  Scope
	Class  Kind
	ID     string
	Labels map[string]string
	Action Action
}

func (t Target) Valid() bool {
	if !t.Scope.Valid() || !(Object{Kind: t.Class, ID: t.ID, Labels: t.Labels}).Valid() ||
		t.Labels["io.compute-relay.installation"] != string(t.Scope.InstallationID) {
		return false
	}
	switch t.Action {
	case OrphanContainer:
		return t.Class == Container
	case OrphanVolume, EvictCache:
		return t.Class == Volume
	case OrphanImage, EvictImage:
		return t.Class == Image
	}
	return false
}

type TargetObservation struct {
	Present    bool
	Eligible   bool
	SizeBytes  int64
	SizeKnown  bool
	Reason     string
	ObservedAt time.Time
}

type DeleteOutcome string

const (
	DeleteConfirmed DeleteOutcome = "deleted"
	AbsentConfirmed DeleteOutcome = "absent"
	DeleteRetained  DeleteOutcome = "retained"
	DeleteUnknown   DeleteOutcome = "unknown"
)
