-- Protected credential bytes and fingerprint keys never enter this database.
CREATE TABLE managed_config (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 fingerprint_initialized INTEGER NOT NULL CHECK(fingerprint_initialized IN (0,1))
) STRICT;
INSERT INTO managed_config VALUES(1,0);
CREATE TABLE managed_connections (
 connection_id TEXT PRIMARY KEY,
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 provider_type TEXT NOT NULL,
 label TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 2147483647),
 authentication TEXT NOT NULL CHECK(authentication IN ('pending','verified','rejected','unavailable')),
 new_work TEXT NOT NULL CHECK(new_work IN ('enabled','disabled','removed')),
 canonical_account TEXT NOT NULL DEFAULT '',
 account_scope TEXT NOT NULL DEFAULT '',
 active_credential_key TEXT NOT NULL DEFAULT '',
 current_profile TEXT NOT NULL DEFAULT '',
 pending_operation TEXT NOT NULL DEFAULT '',
 updated_at TEXT NOT NULL
) STRICT;
CREATE INDEX managed_connections_workspace ON managed_connections(workspace_id,connection_id);
CREATE TABLE managed_connection_operations (
 operation_id TEXT PRIMARY KEY,
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 connection_id TEXT NOT NULL REFERENCES managed_connections(connection_id),
 action TEXT NOT NULL CHECK(action IN ('create','check','replace_credential','disable','enable','remove')),
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
CREATE TRIGGER managed_operation_identity BEFORE UPDATE OF operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,fingerprint,credential_key,receipt,created_at ON managed_connection_operations
 BEGIN SELECT RAISE(ABORT,'immutable management operation identity'); END;
CREATE TABLE managed_credential_generations (
 credential_key TEXT PRIMARY KEY,
 connection_id TEXT NOT NULL REFERENCES managed_connections(connection_id),
 created_at TEXT NOT NULL
) STRICT;
CREATE TABLE managed_secret_deletions (
 credential_key TEXT PRIMARY KEY REFERENCES managed_credential_generations(credential_key)
) STRICT;
CREATE TRIGGER managed_account_identity BEFORE UPDATE OF canonical_account,account_scope ON managed_connections
 WHEN OLD.canonical_account!='' AND (NEW.canonical_account!=OLD.canonical_account OR NEW.account_scope!=OLD.account_scope)
 BEGIN SELECT RAISE(ABORT,'managed account identity is immutable'); END;
-- Local offline administration cannot overwrite generated managed profile aliases.
CREATE TRIGGER managed_profile_repoint BEFORE UPDATE OF revision ON profiles
 WHEN OLD.revision!=NEW.revision AND EXISTS(SELECT 1 FROM managed_connections c WHERE c.connection_id=json_extract((SELECT snapshot FROM profile_revisions WHERE profile=OLD.profile AND revision=OLD.revision),'$.binding.ProviderInstanceID'))
 BEGIN SELECT RAISE(ABORT,'managed profile alias cannot be repointed'); END;
-- Provider-owned, bounded non-secret runtime policy freezes alongside the generic
-- profile. Local interpreter paths and credential values are never stored here.
CREATE TABLE managed_profile_runtime (
 profile TEXT NOT NULL,
 revision TEXT NOT NULL,
 config BLOB NOT NULL CHECK(length(config)<=8192),
 PRIMARY KEY(profile,revision),
 FOREIGN KEY(profile,revision) REFERENCES profile_revisions(profile,revision)
) STRICT;
CREATE TRIGGER immutable_managed_profile_runtime BEFORE UPDATE ON managed_profile_runtime
 BEGIN SELECT RAISE(ABORT,'immutable managed runtime policy'); END;
