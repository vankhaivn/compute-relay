CREATE TABLE execution_authorizations (
 workspace_id TEXT NOT NULL,
 authorization_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 binding TEXT NOT NULL CHECK(length(binding)<=8192),
 account_scope TEXT NOT NULL,
 inputs_sha256 TEXT NOT NULL CHECK(length(inputs_sha256)=64),
 attempt_nonce TEXT NOT NULL CHECK(length(attempt_nonce)=64),
 max_remote_wall_seconds INTEGER NOT NULL CHECK(max_remote_wall_seconds BETWEEN 1 AND 86400),
 granting_token_id TEXT NOT NULL,
 created_at TEXT NOT NULL,
 original_receipt TEXT NOT NULL CHECK(length(original_receipt)<=4096),
 status TEXT NOT NULL CHECK(status IN ('granted','consumed','revoked')),
 consumed_at TEXT,
 PRIMARY KEY(workspace_id,authorization_id),
 UNIQUE(workspace_id,key_sha256),
 UNIQUE(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 CHECK((status='consumed')=(consumed_at IS NOT NULL))
) STRICT;
CREATE INDEX execution_authorization_account ON execution_authorizations(account_scope,status);
CREATE TRIGGER immutable_execution_authorization BEFORE UPDATE OF workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_sha256,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at,original_receipt ON execution_authorizations BEGIN SELECT RAISE(ABORT,'immutable execution authorization'); END;
CREATE TRIGGER execution_authorization_one_shot BEFORE UPDATE OF status,consumed_at ON execution_authorizations WHEN OLD.status!='granted' OR NEW.status NOT IN ('consumed','revoked') BEGIN SELECT RAISE(ABORT,'execution authorization is one shot'); END;
