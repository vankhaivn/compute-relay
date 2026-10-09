-- A NULL override retains the managed adapter's startup default. Existing frozen
-- profiles, runtime configuration and job/attempt grants are never rewritten.
ALTER TABLE managed_connections ADD COLUMN max_remote_wall_seconds INTEGER
 CHECK(max_remote_wall_seconds IS NULL OR max_remote_wall_seconds BETWEEN 1 AND 86400);

-- Extend the action constraint without changing original receipts or replay keys.
CREATE TABLE managed_connection_operations_next (
 operation_id TEXT PRIMARY KEY,
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 connection_id TEXT NOT NULL REFERENCES managed_connections(connection_id),
 action TEXT NOT NULL CHECK(action IN ('create','check','replace_credential','disable','enable','remove','configure')),
 connection_revision INTEGER NOT NULL CHECK(connection_revision>0),
 token_id TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64),
 stage TEXT NOT NULL CHECK(stage IN ('waiting_secret','ready','done')),
 status TEXT NOT NULL CHECK(status IN ('accepted','running','succeeded','failed')),
 credential_key TEXT NOT NULL,
 receipt TEXT NOT NULL CHECK(length(receipt)<=4096),
 problem TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 UNIQUE(workspace_id,key_sha256)
) STRICT;
INSERT INTO managed_connection_operations_next SELECT * FROM managed_connection_operations;
DROP TABLE managed_connection_operations;
ALTER TABLE managed_connection_operations_next RENAME TO managed_connection_operations;
CREATE TRIGGER managed_operation_identity BEFORE UPDATE OF operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,fingerprint,credential_key,receipt,created_at ON managed_connection_operations
 BEGIN SELECT RAISE(ABORT,'immutable management operation identity'); END;
