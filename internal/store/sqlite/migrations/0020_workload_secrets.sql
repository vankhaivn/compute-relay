CREATE TABLE workload_secret_authorizations (
 workspace_id TEXT NOT NULL,
 authorization_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 request_hmac TEXT NOT NULL CHECK(length(request_hmac)=64),
 metadata TEXT NOT NULL CHECK(length(metadata)<=4096),
 original_receipt TEXT NOT NULL CHECK(length(original_receipt)<=4096),
 expires_at_ms INTEGER NOT NULL,
 consumed_at TEXT,
 revoked_at TEXT,
 revocation_key_sha256 TEXT CHECK(revocation_key_sha256 IS NULL OR length(revocation_key_sha256)=64),
 revocation_receipt TEXT CHECK(revocation_receipt IS NULL OR length(revocation_receipt)<=4096),
 PRIMARY KEY(workspace_id,authorization_id),
 UNIQUE(workspace_id,key_sha256),
 UNIQUE(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES preparation_authorizations(workspace_id,job_id,attempt_id),
 CHECK(consumed_at IS NULL OR revoked_at IS NULL),
 CHECK((revoked_at IS NULL)=(revocation_key_sha256 IS NULL)),
 CHECK((revoked_at IS NULL)=(revocation_receipt IS NULL))
) STRICT;
CREATE TRIGGER immutable_workload_secret_authorization BEFORE UPDATE OF workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_hmac,metadata,original_receipt,expires_at_ms ON workload_secret_authorizations BEGIN SELECT RAISE(ABORT,'immutable workload secret authorization'); END;
CREATE TRIGGER workload_secret_consume_once BEFORE UPDATE OF consumed_at ON workload_secret_authorizations WHEN OLD.consumed_at IS NOT NULL OR NEW.consumed_at IS NULL OR OLD.revoked_at IS NOT NULL BEGIN SELECT RAISE(ABORT,'workload secret consumption is one shot'); END;
CREATE TRIGGER workload_secret_revoke_once BEFORE UPDATE OF revoked_at,revocation_key_sha256,revocation_receipt ON workload_secret_authorizations WHEN OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL OR OLD.consumed_at IS NOT NULL BEGIN SELECT RAISE(ABORT,'workload secret revocation is final'); END;
