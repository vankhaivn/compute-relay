-- Original JSON is copied verbatim. Receipt scope keeps independent public keys
-- independent; cleanup already occupies its eventual operation-kind namespace.
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
 max_remote_wall_seconds INTEGER CHECK(max_remote_wall_seconds IS NULL OR max_remote_wall_seconds BETWEEN 1 AND 86400),
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
CREATE INDEX grant_account_holds ON attempt_grants(kind,account_scope,storage_released);

CREATE TABLE managed_connection_operations_v27 (
 operation_id TEXT PRIMARY KEY,
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 connection_id TEXT NOT NULL REFERENCES managed_connections(connection_id),
 action TEXT NOT NULL CHECK(action IN ('create','check','replace_credential','disable','enable','remove','configure','install_image')),
 connection_revision INTEGER NOT NULL CHECK(connection_revision>0),
 token_id TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 stage TEXT NOT NULL CHECK(stage IN ('waiting_secret','ready','installing','done')),
 status TEXT NOT NULL CHECK(status IN ('accepted','running','succeeded','failed')),
 credential_key TEXT NOT NULL,
 problem TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 installation BLOB NOT NULL CHECK(length(installation)<=1024),
 receipt_namespace TEXT GENERATED ALWAYS AS ('connection') STORED,
 receipt_scope TEXT GENERATED ALWAYS AS ('') STORED,
 UNIQUE(workspace_id,key_sha256),
 FOREIGN KEY(workspace_id,receipt_namespace,receipt_scope,key_sha256,operation_id)
  REFERENCES receipts(workspace_id,namespace,scope,key_sha256,subject_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE receipts (
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 namespace TEXT NOT NULL CHECK(namespace IN ('admission','operation','connection','grant','revoke')),
 scope TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 canonical_version TEXT NOT NULL,
 subject_id TEXT NOT NULL,
 job_id TEXT,
 attempt_id TEXT,
 receipt TEXT NOT NULL CHECK(length(receipt) BETWEEN 1 AND 4096),
 operation_id TEXT GENERATED ALWAYS AS (CASE WHEN namespace='operation' AND scope!='cleanup' THEN subject_id END) STORED,
 cleanup_operation_id TEXT GENERATED ALWAYS AS (CASE WHEN namespace='operation' AND scope='cleanup' THEN subject_id END) STORED,
 connection_operation_id TEXT GENERATED ALWAYS AS (CASE WHEN namespace='connection' THEN subject_id END) STORED,
 grant_kind TEXT GENERATED ALWAYS AS (CASE WHEN namespace='grant' THEN scope WHEN namespace='revoke' THEN 'secret' END) STORED,
 authorization_id TEXT GENERATED ALWAYS AS (CASE WHEN namespace IN ('grant','revoke') THEN subject_id END) STORED,
 PRIMARY KEY(workspace_id,namespace,scope,key_sha256),
 UNIQUE(workspace_id,namespace,scope,key_sha256,subject_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES operations(workspace_id,operation_id),
 FOREIGN KEY(workspace_id,cleanup_operation_id) REFERENCES cleanup_operations(workspace_id,operation_id),
 FOREIGN KEY(connection_operation_id) REFERENCES managed_connection_operations_v27(operation_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(workspace_id,grant_kind,authorization_id,job_id,attempt_id) REFERENCES attempt_grants(workspace_id,kind,authorization_id,job_id,attempt_id) DEFERRABLE INITIALLY DEFERRED,
 CHECK((namespace='connection' AND scope='' AND job_id IS NULL AND attempt_id IS NULL)
  OR (namespace!='connection' AND job_id IS NOT NULL AND attempt_id IS NOT NULL)),
 CHECK(namespace!='admission' OR scope='job.create' AND subject_id=job_id),
 CHECK(namespace!='operation' OR scope IN ('cancel','retry_compute','reconcile','collect','cleanup')),
 CHECK(namespace!='grant' OR scope IN ('execution','prepare','start','secret')),
 CHECK(namespace!='revoke' OR scope=subject_id)
) STRICT;
CREATE INDEX receipt_subject ON receipts(workspace_id,namespace,scope,subject_id);

INSERT INTO managed_connection_operations_v27(operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,stage,status,credential_key,problem,created_at,updated_at,installation)
 SELECT operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,stage,status,credential_key,problem,created_at,updated_at,installation FROM managed_connection_operations;
INSERT INTO receipts(workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt)
 SELECT workspace_id,'admission',operation,key_sha256,request_sha256,canonical_version,job_id,job_id,attempt_id,receipt FROM idempotency;
INSERT INTO receipts(workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt)
 SELECT r.workspace_id,'operation',r.kind,r.key_sha256,r.request_sha256,r.canonical_version,r.operation_id,o.job_id,o.attempt_id,r.receipt FROM operation_idempotency r JOIN operations o ON o.workspace_id=r.workspace_id AND o.operation_id=r.operation_id;
INSERT INTO receipts(workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt)
 SELECT r.workspace_id,'operation','cleanup',r.key_sha256,r.request_sha256,r.canonical_version,r.operation_id,o.job_id,o.attempt_id,r.receipt FROM cleanup_idempotency r JOIN cleanup_operations o ON o.workspace_id=r.workspace_id AND o.operation_id=r.operation_id;
INSERT INTO receipts(workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,receipt)
 SELECT workspace_id,'connection','',key_sha256,fingerprint,'',operation_id,receipt FROM managed_connection_operations;
INSERT INTO receipts(workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt)
 SELECT workspace_id,'grant','execution',key_sha256,request_sha256,'',authorization_id,job_id,attempt_id,original_receipt FROM execution_authorizations;
INSERT INTO receipts(workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt)
 SELECT workspace_id,'grant','prepare',key_sha256,request_sha256,'',authorization_id,job_id,attempt_id,original_receipt FROM preparation_authorizations;
INSERT INTO receipts(workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt)
 SELECT workspace_id,'grant','start',key_sha256,request_sha256,'',authorization_id,job_id,attempt_id,original_receipt FROM start_authorizations;
INSERT INTO receipts(workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt)
 SELECT workspace_id,'grant','secret',key_sha256,request_hmac,'',authorization_id,job_id,attempt_id,original_receipt FROM workload_secret_authorizations;
INSERT INTO receipts(workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt)
 SELECT workspace_id,'revoke',authorization_id,revocation_key_sha256,key_sha256,'',authorization_id,job_id,attempt_id,revocation_receipt FROM workload_secret_authorizations WHERE revoked_at IS NOT NULL;

INSERT INTO attempt_grants(workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,consumed_at,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at)
 SELECT workspace_id,'execution',authorization_id,job_id,attempt_id,key_sha256,status,consumed_at,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at FROM execution_authorizations;
INSERT INTO attempt_grants(workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,consumed_at,request,binding,account_scope,inputs_sha256,attempt_nonce,reserved_bytes,ready_until_ms,ready_revision,ready_at,storage_released)
 SELECT workspace_id,'prepare',authorization_id,job_id,attempt_id,key_sha256,CASE WHEN consumed_at IS NULL THEN 'granted' ELSE 'consumed' END,consumed_at,request,binding,account_scope,inputs_sha256,attempt_nonce,reserved_bytes,ready_until_ms,ready_revision,ready_at,storage_released FROM preparation_authorizations;
INSERT INTO attempt_grants(workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,consumed_at,request)
 SELECT workspace_id,'start',authorization_id,job_id,attempt_id,key_sha256,CASE WHEN consumed_at IS NULL THEN 'granted' ELSE 'consumed' END,consumed_at,request FROM start_authorizations;
INSERT INTO attempt_grants(workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,consumed_at,metadata,expires_at_ms,revoked_at,revocation_key_sha256)
 SELECT workspace_id,'secret',authorization_id,job_id,attempt_id,key_sha256,CASE WHEN consumed_at IS NOT NULL THEN 'consumed' WHEN revoked_at IS NOT NULL THEN 'revoked' ELSE 'granted' END,consumed_at,metadata,expires_at_ms,revoked_at,revocation_key_sha256 FROM workload_secret_authorizations;
DROP TABLE start_authorizations;
DROP TABLE workload_secret_authorizations;
DROP TABLE preparation_authorizations;
DROP TABLE execution_authorizations;
DROP TABLE idempotency;
DROP TRIGGER cleanup_receipt_keep;
DROP TABLE cleanup_idempotency;
DROP TABLE operation_idempotency;
DROP TABLE managed_connection_operations;
ALTER TABLE managed_connection_operations_v27 RENAME TO managed_connection_operations;

CREATE TRIGGER managed_operation_identity BEFORE UPDATE OF operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,credential_key,created_at,installation ON managed_connection_operations
 BEGIN SELECT RAISE(ABORT,'immutable management operation identity'); END;
CREATE TRIGGER immutable_receipt BEFORE UPDATE ON receipts
 BEGIN SELECT RAISE(ABORT,'immutable original receipt'); END;
CREATE TRIGGER receipt_keep BEFORE DELETE ON receipts
 BEGIN SELECT RAISE(ABORT,'receipts cannot be forgotten'); END;
CREATE TRIGGER receipt_owner BEFORE INSERT ON receipts
 WHEN (NEW.namespace='operation' AND NEW.scope!='cleanup' AND NOT EXISTS(SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id AND o.operation_id=NEW.subject_id AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id AND o.kind=NEW.scope))
 OR (NEW.namespace='operation' AND NEW.scope='cleanup' AND NOT EXISTS(SELECT 1 FROM cleanup_operations o WHERE o.workspace_id=NEW.workspace_id AND o.operation_id=NEW.subject_id AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id))
 OR (NEW.namespace='connection' AND NOT EXISTS(SELECT 1 FROM managed_connection_operations o WHERE o.workspace_id=NEW.workspace_id AND o.operation_id=NEW.subject_id))
 BEGIN SELECT RAISE(ABORT,'receipt owner mismatch'); END;
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
