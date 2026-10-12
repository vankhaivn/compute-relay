-- Extend the action constraint without changing original receipts or replay keys.
CREATE TABLE managed_connection_operations_next (
 operation_id TEXT PRIMARY KEY,
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 connection_id TEXT NOT NULL REFERENCES managed_connections(connection_id),
 action TEXT NOT NULL CHECK(action IN ('create','check','replace_credential','disable','enable','remove','configure','install_image')),
 connection_revision INTEGER NOT NULL CHECK(connection_revision>0),
 token_id TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64),
 stage TEXT NOT NULL CHECK(stage IN ('waiting_secret','ready','installing','done')),
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

ALTER TABLE managed_connection_operations ADD COLUMN installation BLOB NOT NULL DEFAULT X''
 CHECK(length(installation)<=1024);
CREATE TRIGGER managed_installation_identity BEFORE UPDATE OF installation ON managed_connection_operations
 BEGIN SELECT RAISE(ABORT,'immutable image installation plan'); END;

-- Keep admitted jobs on their original image policy, but require verification and
-- explicit installation before publishing any new SSH admission selection.
UPDATE profiles SET enabled=0 WHERE profile IN
 (SELECT c.current_profile FROM managed_connections c
  JOIN managed_profile_runtime r ON r.profile=c.current_profile
  WHERE c.provider_type='ssh' AND CASE WHEN json_valid(CAST(r.config AS TEXT)) THEN json_extract(CAST(r.config AS TEXT),'$.schema_version') END=1);
UPDATE managed_connections SET current_profile='' WHERE provider_type='ssh' AND current_profile IN
 (SELECT r.profile FROM managed_profile_runtime r
  WHERE CASE WHEN json_valid(CAST(r.config AS TEXT)) THEN json_extract(CAST(r.config AS TEXT),'$.schema_version') END=1);
