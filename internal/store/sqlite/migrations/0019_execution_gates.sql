CREATE TABLE preparation_authorizations (
 workspace_id TEXT NOT NULL,
 authorization_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 request TEXT NOT NULL CHECK(length(request)<=4096),
 original_receipt TEXT NOT NULL CHECK(length(original_receipt)<=4096),
 binding TEXT NOT NULL CHECK(length(binding)<=8192),
 account_scope TEXT NOT NULL,
 inputs_sha256 TEXT NOT NULL CHECK(length(inputs_sha256)=64),
 attempt_nonce TEXT NOT NULL CHECK(length(attempt_nonce)=64),
 reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes BETWEEN 1 AND 4399824896),
 ready_until_ms INTEGER NOT NULL,
 consumed_at TEXT,
 ready_revision TEXT CHECK(ready_revision IS NULL OR length(ready_revision)=64),
 ready_at TEXT,
 storage_released INTEGER NOT NULL DEFAULT 0 CHECK(storage_released IN (0,1)),
 PRIMARY KEY(workspace_id,authorization_id),
 UNIQUE(workspace_id,key_sha256),
 UNIQUE(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 CHECK((ready_revision IS NULL)=(ready_at IS NULL)),
 CHECK(ready_revision IS NULL OR consumed_at IS NOT NULL)
) STRICT;
CREATE INDEX preparation_account_holds ON preparation_authorizations(account_scope,storage_released);
CREATE TRIGGER immutable_preparation_authorization BEFORE UPDATE OF workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_sha256,request,original_receipt,binding,account_scope,inputs_sha256,attempt_nonce,reserved_bytes,ready_until_ms ON preparation_authorizations BEGIN SELECT RAISE(ABORT,'immutable preparation authorization'); END;
CREATE TRIGGER preparation_consume_once BEFORE UPDATE OF consumed_at ON preparation_authorizations WHEN OLD.consumed_at IS NOT NULL OR NEW.consumed_at IS NULL BEGIN SELECT RAISE(ABORT,'preparation authorization is one shot'); END;
CREATE TRIGGER preparation_ready_once BEFORE UPDATE OF ready_revision,ready_at ON preparation_authorizations WHEN OLD.ready_revision IS NOT NULL OR NEW.ready_revision IS NULL OR NEW.ready_at IS NULL BEGIN SELECT RAISE(ABORT,'readiness is immutable'); END;
CREATE TRIGGER preparation_release_once BEFORE UPDATE OF storage_released ON preparation_authorizations WHEN OLD.storage_released!=0 OR NEW.storage_released!=1 BEGIN SELECT RAISE(ABORT,'storage release is final'); END;

CREATE TABLE start_authorizations (
 workspace_id TEXT NOT NULL,
 authorization_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 request TEXT NOT NULL CHECK(length(request)<=4096),
 original_receipt TEXT NOT NULL CHECK(length(original_receipt)<=4096),
 consumed_at TEXT,
 PRIMARY KEY(workspace_id,authorization_id),
 UNIQUE(workspace_id,key_sha256),
 UNIQUE(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES preparation_authorizations(workspace_id,job_id,attempt_id)
) STRICT;
CREATE TRIGGER immutable_start_authorization BEFORE UPDATE OF workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_sha256,request,original_receipt ON start_authorizations BEGIN SELECT RAISE(ABORT,'immutable start authorization'); END;
CREATE TRIGGER start_consume_once BEFORE UPDATE OF consumed_at ON start_authorizations WHEN OLD.consumed_at IS NOT NULL OR NEW.consumed_at IS NULL BEGIN SELECT RAISE(ABORT,'start authorization is one shot'); END;
