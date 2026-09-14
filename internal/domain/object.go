package domain

// ObjectMetadata describes verified immutable bytes. Physical paths are never API IDs.
// During BlobStore.Put only, Bytes=-1 means unspecified and SHA256="" means compute it.
// Returned/stored metadata must have a nonnegative byte count and canonical digest.
type ObjectMetadata struct {
	ID          ObjectID     `json:"object_id"`
	WorkspaceID WorkspaceID  `json:"workspace_id"`
	Bytes       int64        `json:"bytes"`
	SHA256      SHA256Digest `json:"sha256"`
}

func (m ObjectMetadata) Valid() bool {
	return m.ID.Valid() && m.WorkspaceID.Valid() && m.Bytes >= 0 && m.SHA256.Valid()
}
