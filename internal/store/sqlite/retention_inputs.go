package sqlite

// New admissions and explicit retries must not confuse retained metadata with
// retained bytes. Both preflight and commit use this query in their transaction;
// replay is resolved first. SQL insertion guards remain a second line of defense.
const retainedInputQuery = `SELECT o.workspace_id,o.object_id,o.bytes,o.sha256
 FROM objects o JOIN retention_inventory r
 ON r.kind='input' AND r.workspace_id=o.workspace_id AND r.object_id=o.object_id
 AND r.bytes=o.bytes AND r.sha256=o.sha256
 WHERE o.workspace_id=? AND o.object_id=? AND r.expired_at IS NULL`
