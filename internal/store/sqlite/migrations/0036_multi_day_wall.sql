-- Relax stored wall budgets to the provider-neutral 604800-second contract maximum.
-- Each provider still enforces its own advertised maximum before a row is written.
-- Existing values, grants and original receipts are copied verbatim.
PRAGMA defer_foreign_keys=ON;

-- A column CHECK cannot be altered in place: move the connection override to a new column.
ALTER TABLE managed_connections ADD COLUMN max_remote_wall_seconds_v36 INTEGER
 CHECK(max_remote_wall_seconds_v36 IS NULL OR max_remote_wall_seconds_v36 BETWEEN 1 AND 604800);
UPDATE managed_connections SET max_remote_wall_seconds_v36=max_remote_wall_seconds;
ALTER TABLE managed_connections DROP COLUMN max_remote_wall_seconds;
ALTER TABLE managed_connections RENAME COLUMN max_remote_wall_seconds_v36 TO max_remote_wall_seconds;

-- Rebuild attempt grants. Deleting rows first lets reinsertion settle every deferred
-- receipt and preparation reference counted by the delete.
CREATE TEMP TABLE _v36_attempt_grants AS SELECT workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,consumed_at,request,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at,reserved_bytes,ready_until_ms,ready_revision,ready_at,storage_released,metadata,expires_at_ms,revoked_at,revocation_key_sha256 FROM attempt_grants;

DELETE FROM attempt_grants;
DROP TABLE attempt_grants;

CREATE TABLE attempt_grants (
 workspace_id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('execution','prepare','start','secret')),
 authorization_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 status TEXT NOT NULL CHECK(status IN ('granted','consumed','revoked')),
 consumed_at TEXT,
 request TEXT CHECK(request IS NULL OR length(request)<=4096),
 binding TEXT CHECK(binding IS NULL OR length(binding)<=8192),
 account_scope TEXT,
 inputs_sha256 TEXT CHECK(inputs_sha256 IS NULL OR length(inputs_sha256)=64),
 attempt_nonce TEXT CHECK(attempt_nonce IS NULL OR length(attempt_nonce)=64),
 max_remote_wall_seconds INTEGER CHECK(max_remote_wall_seconds IS NULL OR max_remote_wall_seconds BETWEEN 1 AND 604800),
 granting_token_id TEXT,
 created_at TEXT,
 reserved_bytes INTEGER CHECK(reserved_bytes IS NULL OR reserved_bytes BETWEEN 1 AND 4399824896),
 ready_until_ms INTEGER,
 ready_revision TEXT CHECK(ready_revision IS NULL OR length(ready_revision)=64),
 ready_at TEXT,
 storage_released INTEGER NOT NULL DEFAULT 0 CHECK(storage_released IN (0,1)),
 metadata TEXT CHECK(metadata IS NULL OR length(metadata)<=4096),
 expires_at_ms INTEGER,
 revoked_at TEXT,
 revocation_key_sha256 TEXT CHECK(revocation_key_sha256 IS NULL OR length(revocation_key_sha256)=64),
 receipt_namespace TEXT GENERATED ALWAYS AS ('grant') STORED,
 preparation_kind TEXT GENERATED ALWAYS AS (CASE WHEN kind IN ('start','secret') THEN 'prepare' END) STORED,
 revocation_namespace TEXT GENERATED ALWAYS AS ('revoke') STORED,
 PRIMARY KEY(workspace_id,kind,authorization_id),
 UNIQUE(workspace_id,kind,job_id,attempt_id),
 UNIQUE(workspace_id,kind,authorization_id,job_id,attempt_id),
 UNIQUE(workspace_id,kind,key_sha256),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,preparation_kind,job_id,attempt_id) REFERENCES attempt_grants(workspace_id,kind,job_id,attempt_id),
 FOREIGN KEY(workspace_id,receipt_namespace,kind,key_sha256,authorization_id)
  REFERENCES receipts(workspace_id,namespace,scope,key_sha256,subject_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(workspace_id,revocation_namespace,authorization_id,revocation_key_sha256,authorization_id)
  REFERENCES receipts(workspace_id,namespace,scope,key_sha256,subject_id) DEFERRABLE INITIALLY DEFERRED,
 CHECK((status='consumed')=(consumed_at IS NOT NULL)),
 CHECK(consumed_at IS NULL OR revoked_at IS NULL),
 CHECK((revoked_at IS NULL)=(revocation_key_sha256 IS NULL)),
 CHECK((ready_revision IS NULL)=(ready_at IS NULL)),
 CHECK(ready_revision IS NULL OR consumed_at IS NOT NULL),
 CHECK((kind IN ('execution','prepare') AND binding IS NOT NULL AND account_scope IS NOT NULL AND inputs_sha256 IS NOT NULL AND attempt_nonce IS NOT NULL)
  OR (kind IN ('start','secret') AND binding IS NULL AND account_scope IS NULL AND inputs_sha256 IS NULL AND attempt_nonce IS NULL)),
 CHECK((kind='execution' AND max_remote_wall_seconds IS NOT NULL AND granting_token_id IS NOT NULL AND created_at IS NOT NULL)
  OR (kind!='execution' AND max_remote_wall_seconds IS NULL AND granting_token_id IS NULL AND created_at IS NULL)),
 CHECK((kind IN ('prepare','start') AND request IS NOT NULL) OR (kind IN ('execution','secret') AND request IS NULL)),
 CHECK((kind='prepare' AND reserved_bytes IS NOT NULL AND ready_until_ms IS NOT NULL AND status!='revoked')
  OR (kind!='prepare' AND reserved_bytes IS NULL AND ready_until_ms IS NULL AND ready_revision IS NULL AND ready_at IS NULL AND storage_released=0)),
 CHECK((kind='secret' AND metadata IS NOT NULL AND expires_at_ms IS NOT NULL AND (status='revoked')=(revoked_at IS NOT NULL))
  OR (kind!='secret' AND metadata IS NULL AND expires_at_ms IS NULL AND revoked_at IS NULL AND revocation_key_sha256 IS NULL)),
 CHECK(kind!='start' OR status!='revoked')
) STRICT;

INSERT INTO attempt_grants(workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,consumed_at,request,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at,reserved_bytes,ready_until_ms,ready_revision,ready_at,storage_released,metadata,expires_at_ms,revoked_at,revocation_key_sha256)
 SELECT workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,consumed_at,request,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at,reserved_bytes,ready_until_ms,ready_revision,ready_at,storage_released,metadata,expires_at_ms,revoked_at,revocation_key_sha256
 FROM _v36_attempt_grants ORDER BY kind!='prepare';
DROP TABLE _v36_attempt_grants;

CREATE INDEX grant_account_holds ON attempt_grants(kind,account_scope,storage_released);
CREATE TRIGGER immutable_attempt_grant BEFORE UPDATE OF workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,request,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at,reserved_bytes,ready_until_ms,metadata,expires_at_ms ON attempt_grants
 BEGIN SELECT RAISE(ABORT,'immutable attempt grant'); END;
CREATE TRIGGER grant_consume_once BEFORE UPDATE OF status,consumed_at ON attempt_grants
 WHEN OLD.status!='granted' OR NEW.status NOT IN ('consumed','revoked') OR OLD.consumed_at IS NOT NULL OR OLD.revoked_at IS NOT NULL
 BEGIN SELECT RAISE(ABORT,'attempt grant is one shot'); END;
CREATE TRIGGER grant_revoke_once BEFORE UPDATE OF revoked_at,revocation_key_sha256 ON attempt_grants
 WHEN OLD.kind!='secret' OR OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL OR OLD.consumed_at IS NOT NULL OR NEW.status!='revoked'
 BEGIN SELECT RAISE(ABORT,'workload secret revocation is final'); END;
CREATE TRIGGER grant_ready_once BEFORE UPDATE OF ready_revision,ready_at ON attempt_grants
 WHEN OLD.kind!='prepare' OR OLD.ready_revision IS NOT NULL OR NEW.ready_revision IS NULL OR NEW.ready_at IS NULL
 BEGIN SELECT RAISE(ABORT,'readiness is immutable'); END;
CREATE TRIGGER grant_release_once BEFORE UPDATE OF storage_released ON attempt_grants
 WHEN OLD.kind!='prepare' OR OLD.storage_released!=0 OR NEW.storage_released!=1
 BEGIN SELECT RAISE(ABORT,'storage release is final'); END;
