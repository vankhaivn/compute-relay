-- Merge explicit cleanup into the operation identity/receipt namespace. Original
-- request, record and receipt JSON is never decoded or rewritten by this migration.
-- Foreign keys remain enabled. Empty the snapshotted dependent graph before its
-- schema is rebuilt, so deferred receipt/grant cycles cannot strand DROP counters.
PRAGMA defer_foreign_keys=ON;

CREATE TEMP TABLE _v28_operations AS SELECT operation_seq,workspace_id,operation_id,job_id,attempt_id,kind,status,revision,created_at,request_sha256,request_json,record,new_attempt_id,cancel_started FROM operations;

CREATE TEMP TABLE _v28_cleanup_resources AS SELECT workspace_id,operation_id,resource_id,ordinal,target,target_sha256,state,reason FROM cleanup_resources;

CREATE TEMP TABLE _v28_collection_publications AS SELECT workspace_id,job_id,attempt_id,operation_id,snapshot_sha256,phase,file_count,verified_at FROM collection_publications;

CREATE TEMP TABLE _v28_artifacts AS SELECT workspace_id,job_id,attempt_id,artifact_id,object_id,path,role,bytes,sha256,media_type FROM artifacts;

CREATE TEMP TABLE _v28_retention_expirations AS SELECT workspace_id,job_id,attempt_id,expired_at FROM retention_expirations;

CREATE TEMP TABLE _v28_work_leases AS SELECT kind,key,generation,owner,fence,until_ms,held,operation_id,attempt_revision FROM work_leases;

CREATE TEMP TABLE _v28_collection_progress AS SELECT workspace_id,job_id,attempt_id,operation_id,generation,fence,stage,progress FROM collection_progress;

CREATE TEMP TABLE _v28_attempt_grants AS SELECT workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,consumed_at,request,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at,reserved_bytes,ready_until_ms,ready_revision,ready_at,storage_released,metadata,expires_at_ms,revoked_at,revocation_key_sha256 FROM attempt_grants;

CREATE TEMP TABLE _v28_managed_connection_operations AS SELECT operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,stage,status,credential_key,problem,created_at,updated_at,installation FROM managed_connection_operations;

CREATE TEMP TABLE _v28_receipts AS SELECT workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt FROM receipts;

CREATE TEMP TABLE _v28_cleanup_operations AS SELECT cleanup_seq,workspace_id,operation_id,job_id,attempt_id,resource_id,status,revision,created_at,request_sha256,request_json,record FROM cleanup_operations;

CREATE TEMP TABLE _v28_operation_sequence AS SELECT max(COALESCE((SELECT max(operation_seq) FROM operations),0),COALESCE((SELECT seq FROM sqlite_sequence WHERE name='operations'),0)) AS base;

DROP TRIGGER operation_identity;

DROP TRIGGER operation_terminal;

DROP TRIGGER operation_revision;

DROP TRIGGER operation_cancel_once;

DROP TRIGGER operation_retry_identity;

DROP TRIGGER event_operation_owner;

DROP TRIGGER immutable_collection_publication;

DROP TRIGGER immutable_artifact;

DROP TRIGGER collection_publication_owner;

DROP TRIGGER retention_artifact_keep;

DROP TRIGGER retention_expiration_identity;

DROP TRIGGER retention_expiration_keep;

DROP TRIGGER cleanup_resource_owner;

DROP TRIGGER cleanup_operation_namespace;

DROP TRIGGER operation_cleanup_namespace;

DROP TRIGGER cleanup_identity;

DROP TRIGGER cleanup_terminal;

DROP TRIGGER cleanup_revision;

DROP TRIGGER cleanup_keep;

DROP TRIGGER cleanup_resource_snapshot_owner;

DROP TRIGGER cleanup_resource_snapshot_identity;

DROP TRIGGER cleanup_resource_snapshot_state;

DROP TRIGGER cleanup_resource_snapshot_keep;

DROP TRIGGER retention_artifact_insert;

DROP TRIGGER work_lease_insert;

DROP TRIGGER work_lease_update;

DROP TRIGGER work_lease_keep;

DROP TRIGGER collection_progress_owner_insert;

DROP TRIGGER collection_progress_owner_update;

DROP TRIGGER managed_operation_identity;

DROP TRIGGER immutable_receipt;

DROP TRIGGER receipt_keep;

DROP TRIGGER receipt_owner;

DROP TRIGGER immutable_attempt_grant;

DROP TRIGGER grant_consume_once;

DROP TRIGGER grant_revoke_once;

DROP TRIGGER grant_ready_once;

DROP TRIGGER grant_release_once;

DELETE FROM collection_progress;

DELETE FROM artifacts;

DELETE FROM retention_expirations;

DELETE FROM cleanup_resources;

DELETE FROM work_leases;

DELETE FROM collection_publications;

DELETE FROM attempt_grants;

DELETE FROM managed_connection_operations;

DELETE FROM receipts;

DELETE FROM cleanup_operations;

DELETE FROM operations;

DROP TABLE collection_progress;

DROP TABLE artifacts;

DROP TABLE retention_expirations;

DROP TABLE cleanup_resources;

DROP TABLE work_leases;

DROP TABLE collection_publications;

DROP TABLE attempt_grants;

DROP TABLE managed_connection_operations;

DROP TABLE receipts;

DROP TABLE cleanup_operations;

DROP TABLE operations;

CREATE TABLE operations (
 operation_seq INTEGER PRIMARY KEY AUTOINCREMENT,
 workspace_id TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('cancel','retry_compute','reconcile','collect','cleanup')),
 status TEXT NOT NULL CHECK(status IN ('accepted','running','succeeded','failed','manual_required')),
 revision INTEGER NOT NULL CHECK(revision>0),
 created_at TEXT NOT NULL,
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 request_json TEXT NOT NULL CHECK(length(request_json)<=4096),
 record TEXT NOT NULL CHECK(length(record)<=8192),
 new_attempt_id TEXT,
 cancel_started INTEGER NOT NULL DEFAULT 0 CHECK(cancel_started IN (0,1)),
 resource_id TEXT REFERENCES provider_resources(resource_id),
 UNIQUE(workspace_id,operation_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,new_attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 CHECK((kind='cleanup')=(resource_id IS NOT NULL)),
 CHECK(kind!='cleanup' OR status!='manual_required'),
 CHECK(kind!='cleanup' OR COALESCE((status IN ('accepted','running') AND json_extract(record,'$.Effect')='pending')
 OR (status='succeeded' AND json_extract(record,'$.Effect') IN ('remote_deleted','already_absent'))
 OR (status='failed' AND json_extract(record,'$.Effect')='failed'),0)),
 CHECK(new_attempt_id IS NULL OR (kind='retry_compute' AND new_attempt_id!=attempt_id)),
 CHECK(cancel_started=0 OR kind='cancel'),
 CHECK(COALESCE(json_extract(record,'$.Operation.ID')=operation_id,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.WorkspaceID')=workspace_id,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.JobID')=job_id,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.AttemptID')=attempt_id,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.Kind')=kind,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.Status')=status,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.Revision')=revision,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.CreatedAt')=created_at,0)),
 CHECK(COALESCE(json_extract(record,'$.Replay')=0,0)),
 CHECK(COALESCE(json_extract(record,'$.NewAttemptID'),'')=COALESCE(new_attempt_id,''))
) STRICT;

CREATE TABLE cleanup_resources (
 workspace_id TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 resource_id TEXT NOT NULL REFERENCES provider_resources(resource_id),
 ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 1),
 target TEXT NOT NULL CHECK(length(target)<=16384 AND json_valid(target)),
 target_sha256 TEXT NOT NULL CHECK(length(target_sha256)=64),
 state TEXT NOT NULL CHECK(state IN ('pending','running','deleted','absent','retained','failed','unknown')),
 reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 96),
 PRIMARY KEY(workspace_id,operation_id,resource_id),
 UNIQUE(workspace_id,operation_id,ordinal),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES operations(workspace_id,operation_id)
) STRICT;

CREATE TABLE collection_publications (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 snapshot_sha256 TEXT NOT NULL CHECK(length(snapshot_sha256)=64),
 phase TEXT NOT NULL CHECK(phase IN ('completed','failed','timed_out','cancelled','setup_failed','resource_check_failed')),
 file_count INTEGER NOT NULL CHECK(file_count BETWEEN 1 AND 10004),
 verified_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES collection_snapshots(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES operations(workspace_id,operation_id)
) STRICT;

CREATE TABLE artifacts (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 artifact_id TEXT NOT NULL,
 object_id TEXT NOT NULL,
 path TEXT NOT NULL,
 role TEXT NOT NULL CHECK(role IN ('output','manifest','log','provenance')),
 bytes INTEGER NOT NULL CHECK(bytes>=0 AND bytes<=4294967296),
 sha256 TEXT NOT NULL CHECK(length(sha256)=64),
 media_type TEXT NOT NULL,
 PRIMARY KEY(workspace_id,artifact_id),
 UNIQUE(workspace_id,job_id,attempt_id,path),
 UNIQUE(workspace_id,object_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES collection_publications(workspace_id,job_id,attempt_id)
) STRICT;

CREATE TABLE retention_expirations (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 expired_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES collection_publications(workspace_id,job_id,attempt_id)
) STRICT;

CREATE TABLE work_leases (
 kind TEXT NOT NULL CHECK(kind IN ('dispatch','collection','cleanup')),
 key TEXT NOT NULL CHECK(COALESCE(json_valid(key) AND json_type(key)='array' AND
 CASE kind
 WHEN 'dispatch' THEN json_array_length(key)=1 AND json_type(key,'$[0]')='integer'
  AND json_extract(key,'$[0]')>0 AND key=json_array(json_extract(key,'$[0]'))
 WHEN 'collection' THEN json_array_length(key)=3 AND json_type(key,'$[0]')='text'
  AND json_type(key,'$[1]')='text' AND json_type(key,'$[2]')='text'
  AND key=json_array(json_extract(key,'$[0]'),json_extract(key,'$[1]'),json_extract(key,'$[2]'))
 WHEN 'cleanup' THEN json_array_length(key)=2 AND json_type(key,'$[0]')='text'
  AND json_type(key,'$[1]')='text' AND key=json_array(json_extract(key,'$[0]'),json_extract(key,'$[1]')) END,0)),
 generation INTEGER NOT NULL CHECK(generation>0),
 owner TEXT NOT NULL,
 fence TEXT NOT NULL,
 until_ms INTEGER NOT NULL CHECK(until_ms>=0),
 held INTEGER NOT NULL CHECK(held IN (0,1)),
 operation_id TEXT,
 attempt_revision INTEGER,
 queue_seq INTEGER GENERATED ALWAYS AS (CASE WHEN kind='dispatch' THEN json_extract(key,'$[0]') END) STORED,
 workspace_id TEXT GENERATED ALWAYS AS (CASE WHEN kind!='dispatch' THEN json_extract(key,'$[0]') END) STORED,
 job_id TEXT GENERATED ALWAYS AS (CASE WHEN kind='collection' THEN json_extract(key,'$[1]') END) STORED,
 attempt_id TEXT GENERATED ALWAYS AS (CASE WHEN kind='collection' THEN json_extract(key,'$[2]') END) STORED,
 cleanup_operation_id TEXT GENERATED ALWAYS AS (CASE WHEN kind='cleanup' THEN json_extract(key,'$[1]') END) STORED,
 PRIMARY KEY(kind,key),
 FOREIGN KEY(queue_seq) REFERENCES scheduler_queue(queue_seq),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES operations(workspace_id,operation_id),
 FOREIGN KEY(workspace_id,cleanup_operation_id) REFERENCES operations(workspace_id,operation_id),
 CHECK((kind='collection' AND operation_id IS NOT NULL AND attempt_revision IS NOT NULL AND attempt_revision>0)
  OR (kind!='collection' AND operation_id IS NULL AND attempt_revision IS NULL)),
 CHECK((kind='dispatch' AND ((held=1 AND length(owner) BETWEEN 1 AND 128 AND length(fence)=64 AND until_ms>0)
  OR (held=0 AND ((owner='' AND fence='' AND until_ms=0) OR (length(owner) BETWEEN 1 AND 128 AND length(fence)=64 AND until_ms>0)))))
  OR (kind='collection' AND owner='' AND length(fence)>=32)
  OR (kind='cleanup' AND owner='' AND length(fence)=64 AND until_ms>0))
) STRICT;

CREATE TABLE "collection_progress" (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0),
 fence TEXT NOT NULL CHECK(length(fence)>=32),
 stage TEXT NOT NULL CHECK(stage IN ('discovering','transferring','verifying','available','failed')),
 progress TEXT NOT NULL CHECK(length(progress)<=2048 AND json_valid(progress)),
 lease_kind TEXT GENERATED ALWAYS AS ('collection') STORED,
 lease_key TEXT GENERATED ALWAYS AS (json_array(workspace_id,job_id,attempt_id)) STORED,
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(lease_kind,lease_key) REFERENCES work_leases(kind,key),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES operations(workspace_id,operation_id)
) STRICT;

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

CREATE TABLE "managed_connection_operations" (
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
 operation_id TEXT GENERATED ALWAYS AS (CASE WHEN namespace='operation' THEN subject_id END) STORED,
 connection_operation_id TEXT GENERATED ALWAYS AS (CASE WHEN namespace='connection' THEN subject_id END) STORED,
 grant_kind TEXT GENERATED ALWAYS AS (CASE WHEN namespace='grant' THEN scope WHEN namespace='revoke' THEN 'secret' END) STORED,
 authorization_id TEXT GENERATED ALWAYS AS (CASE WHEN namespace IN ('grant','revoke') THEN subject_id END) STORED,
 PRIMARY KEY(workspace_id,namespace,scope,key_sha256),
 UNIQUE(workspace_id,namespace,scope,key_sha256,subject_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES operations(workspace_id,operation_id),
 FOREIGN KEY(connection_operation_id) REFERENCES "managed_connection_operations"(operation_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(workspace_id,grant_kind,authorization_id,job_id,attempt_id) REFERENCES attempt_grants(workspace_id,kind,authorization_id,job_id,attempt_id) DEFERRABLE INITIALLY DEFERRED,
 CHECK((namespace='connection' AND scope='' AND job_id IS NULL AND attempt_id IS NULL)
  OR (namespace!='connection' AND job_id IS NOT NULL AND attempt_id IS NOT NULL)),
 CHECK(namespace!='admission' OR scope='job.create' AND subject_id=job_id),
 CHECK(namespace!='operation' OR scope IN ('cancel','retry_compute','reconcile','collect','cleanup')),
 CHECK(namespace!='grant' OR scope IN ('execution','prepare','start','secret')),
 CHECK(namespace!='revoke' OR scope=subject_id)
) STRICT;

INSERT INTO operations(operation_seq,workspace_id,operation_id,job_id,attempt_id,kind,status,revision,created_at,request_sha256,request_json,record,new_attempt_id,cancel_started) SELECT operation_seq,workspace_id,operation_id,job_id,attempt_id,kind,status,revision,created_at,request_sha256,request_json,record,new_attempt_id,cancel_started FROM _v28_operations ORDER BY operation_seq;

INSERT INTO operations(operation_seq,workspace_id,operation_id,job_id,attempt_id,kind,status,revision,created_at,request_sha256,request_json,record,resource_id)
 SELECT (SELECT base FROM _v28_operation_sequence)+row_number() OVER (ORDER BY cleanup_seq),workspace_id,operation_id,job_id,attempt_id,'cleanup',status,revision,created_at,request_sha256,request_json,record,resource_id
 FROM _v28_cleanup_operations ORDER BY cleanup_seq;
-- Preserve the existing AUTOINCREMENT high-water mark even when cleanup is empty.
INSERT INTO sqlite_sequence(name,seq) SELECT 'operations',base FROM _v28_operation_sequence WHERE NOT EXISTS(SELECT 1 FROM sqlite_sequence WHERE name='operations');
UPDATE sqlite_sequence SET seq=max(seq,(SELECT base FROM _v28_operation_sequence)) WHERE name='operations';

INSERT INTO cleanup_resources(workspace_id,operation_id,resource_id,ordinal,target,target_sha256,state,reason) SELECT workspace_id,operation_id,resource_id,ordinal,target,target_sha256,state,reason FROM _v28_cleanup_resources;

INSERT INTO collection_publications(workspace_id,job_id,attempt_id,operation_id,snapshot_sha256,phase,file_count,verified_at) SELECT workspace_id,job_id,attempt_id,operation_id,snapshot_sha256,phase,file_count,verified_at FROM _v28_collection_publications;

INSERT INTO artifacts(workspace_id,job_id,attempt_id,artifact_id,object_id,path,role,bytes,sha256,media_type) SELECT workspace_id,job_id,attempt_id,artifact_id,object_id,path,role,bytes,sha256,media_type FROM _v28_artifacts;

INSERT INTO retention_expirations(workspace_id,job_id,attempt_id,expired_at) SELECT workspace_id,job_id,attempt_id,expired_at FROM _v28_retention_expirations;

INSERT INTO work_leases(kind,key,generation,owner,fence,until_ms,held,operation_id,attempt_revision) SELECT kind,key,generation,owner,fence,until_ms,held,operation_id,attempt_revision FROM _v28_work_leases;

INSERT INTO collection_progress(workspace_id,job_id,attempt_id,operation_id,generation,fence,stage,progress) SELECT workspace_id,job_id,attempt_id,operation_id,generation,fence,stage,progress FROM _v28_collection_progress;

INSERT INTO attempt_grants(workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,consumed_at,request,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at,reserved_bytes,ready_until_ms,ready_revision,ready_at,storage_released,metadata,expires_at_ms,revoked_at,revocation_key_sha256) SELECT workspace_id,kind,authorization_id,job_id,attempt_id,key_sha256,status,consumed_at,request,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at,reserved_bytes,ready_until_ms,ready_revision,ready_at,storage_released,metadata,expires_at_ms,revoked_at,revocation_key_sha256 FROM _v28_attempt_grants;

INSERT INTO managed_connection_operations(operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,stage,status,credential_key,problem,created_at,updated_at,installation) SELECT operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,stage,status,credential_key,problem,created_at,updated_at,installation FROM _v28_managed_connection_operations;

INSERT INTO receipts(workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt) SELECT workspace_id,namespace,scope,key_sha256,request_sha256,canonical_version,subject_id,job_id,attempt_id,receipt FROM _v28_receipts;

CREATE UNIQUE INDEX operation_one_cancel ON operations(workspace_id,job_id,attempt_id) WHERE kind='cancel';

CREATE UNIQUE INDEX operation_one_retry ON operations(workspace_id,job_id,attempt_id) WHERE kind='retry_compute';

CREATE UNIQUE INDEX operation_pending ON operations(workspace_id,job_id,attempt_id,kind) WHERE status IN ('accepted','running');

CREATE INDEX operation_work ON operations(kind,status,operation_seq);

CREATE INDEX work_lease_capacity ON work_leases(kind,held,until_ms);

CREATE INDEX grant_account_holds ON attempt_grants(kind,account_scope,storage_released);

CREATE INDEX receipt_subject ON receipts(workspace_id,namespace,scope,subject_id);

CREATE TRIGGER operation_identity BEFORE UPDATE OF workspace_id,operation_id,job_id,attempt_id,kind,resource_id,created_at,request_sha256,request_json ON operations
 BEGIN SELECT RAISE(ABORT,'immutable control identity'); END;

CREATE TRIGGER operation_terminal BEFORE UPDATE ON operations
 WHEN OLD.status IN ('succeeded','failed','manual_required')
 BEGIN SELECT RAISE(ABORT,'immutable terminal control'); END;

CREATE TRIGGER operation_revision BEFORE UPDATE ON operations
 WHEN NEW.revision!=OLD.revision+1 OR NEW.operation_seq!=OLD.operation_seq
 BEGIN SELECT RAISE(ABORT,'invalid control revision'); END;

CREATE TRIGGER operation_cancel_once BEFORE UPDATE OF cancel_started ON operations
 WHEN OLD.cancel_started=1 AND NEW.cancel_started!=1
 BEGIN SELECT RAISE(ABORT,'remote cancellation intent cannot be reset'); END;

CREATE TRIGGER operation_retry_identity BEFORE UPDATE OF new_attempt_id ON operations
 WHEN OLD.new_attempt_id IS NOT NULL AND NEW.new_attempt_id IS NOT OLD.new_attempt_id
 BEGIN SELECT RAISE(ABORT,'immutable retry result'); END;

CREATE TRIGGER event_operation_owner BEFORE INSERT ON events
 WHEN NEW.operation_id!='' AND NOT EXISTS (
 SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id
 AND o.operation_id=NEW.operation_id AND o.job_id=NEW.job_id
 AND (o.attempt_id=NEW.attempt_id OR o.new_attempt_id=NEW.attempt_id))
 BEGIN SELECT RAISE(ABORT,'event control ownership mismatch'); END;

CREATE TRIGGER immutable_collection_publication BEFORE UPDATE ON collection_publications
 BEGIN SELECT RAISE(ABORT,'immutable collection publication'); END;

CREATE TRIGGER immutable_artifact BEFORE UPDATE ON artifacts
 BEGIN SELECT RAISE(ABORT,'immutable verified artifact'); END;

CREATE TRIGGER collection_publication_owner BEFORE INSERT ON collection_publications
 WHEN NOT EXISTS (SELECT 1 FROM collection_snapshots s WHERE s.workspace_id=NEW.workspace_id
 AND s.job_id=NEW.job_id AND s.attempt_id=NEW.attempt_id AND s.snapshot_sha256=NEW.snapshot_sha256)
 OR NOT EXISTS (SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id
 AND o.operation_id=NEW.operation_id AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id AND o.kind='collect')
 BEGIN SELECT RAISE(ABORT,'collection publication owner mismatch'); END;

CREATE TRIGGER retention_artifact_keep BEFORE DELETE ON artifacts
 BEGIN SELECT RAISE(ABORT,'artifact metadata survives byte expiry'); END;

CREATE TRIGGER retention_expiration_identity BEFORE UPDATE ON retention_expirations
 BEGIN SELECT RAISE(ABORT,'immutable result expiry'); END;

CREATE TRIGGER retention_expiration_keep BEFORE DELETE ON retention_expirations
 BEGIN SELECT RAISE(ABORT,'result expiry cannot be forgotten'); END;

CREATE TRIGGER cleanup_resource_owner BEFORE INSERT ON operations
 WHEN NEW.kind='cleanup' AND NOT EXISTS (SELECT 1 FROM provider_resources r WHERE r.resource_id=NEW.resource_id
 AND r.workspace_id=NEW.workspace_id AND r.job_id=NEW.job_id AND r.attempt_id=NEW.attempt_id
 AND r.purpose='staging' AND r.resource_ref!='')
 BEGIN SELECT RAISE(ABORT,'cleanup target is not an owned attempt resource'); END;

CREATE TRIGGER cleanup_resource_snapshot_owner BEFORE INSERT ON cleanup_resources
 WHEN NEW.state!='pending' OR NEW.reason!='awaiting_worker' OR NOT EXISTS (SELECT 1 FROM operations c JOIN provider_resources r
 ON r.workspace_id=c.workspace_id AND r.job_id=c.job_id AND r.attempt_id=c.attempt_id
 WHERE c.workspace_id=NEW.workspace_id AND c.operation_id=NEW.operation_id AND r.resource_id=NEW.resource_id
 AND c.kind='cleanup' AND c.status IN ('accepted','running') AND r.resource_ref!='')
 BEGIN SELECT RAISE(ABORT,'cleanup resource is not an owned original target'); END;

CREATE TRIGGER cleanup_resource_snapshot_identity BEFORE UPDATE OF workspace_id,operation_id,resource_id,ordinal,target,target_sha256 ON cleanup_resources
 BEGIN SELECT RAISE(ABORT,'immutable cleanup target snapshot'); END;

CREATE TRIGGER cleanup_resource_snapshot_state BEFORE UPDATE OF state,reason ON cleanup_resources
 WHEN OLD.state IN ('deleted','absent','retained','failed','unknown')
 OR NOT ((OLD.state='pending' AND NEW.state IN ('running','retained','failed','unknown'))
 OR (OLD.state='running' AND NEW.state IN ('running','deleted','absent','retained','failed','unknown')))
 BEGIN SELECT RAISE(ABORT,'invalid cleanup resource transition'); END;

CREATE TRIGGER cleanup_resource_snapshot_keep BEFORE DELETE ON cleanup_resources
 BEGIN SELECT RAISE(ABORT,'cleanup resource history cannot be forgotten'); END;

CREATE TRIGGER retention_artifact_insert BEFORE INSERT ON artifacts
 WHEN NOT EXISTS(SELECT 1 FROM retention_inventory r WHERE r.kind='result'
 AND r.workspace_id=NEW.workspace_id AND r.object_id=NEW.object_id
 AND r.bytes=NEW.bytes AND r.sha256=NEW.sha256 AND r.expired_at IS NULL)
 BEGIN SELECT RAISE(ABORT,'artifact retention registration required'); END;

CREATE TRIGGER work_lease_insert BEFORE INSERT ON work_leases
 WHEN NEW.generation!=1
 OR (NEW.kind='collection' AND NOT EXISTS (SELECT 1 FROM operations o
  WHERE o.workspace_id=NEW.workspace_id AND o.operation_id=NEW.operation_id
  AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id AND o.kind='collect'))
 OR (NEW.kind='cleanup' AND NOT EXISTS (SELECT 1 FROM operations c
  WHERE c.workspace_id=NEW.workspace_id AND c.operation_id=NEW.cleanup_operation_id AND c.kind='cleanup' AND c.status='running'))
 BEGIN SELECT RAISE(ABORT,'work lease owner mismatch'); END;

CREATE TRIGGER work_lease_update BEFORE UPDATE ON work_leases
 WHEN NEW.kind!=OLD.kind OR NEW.key!=OLD.key
 OR NEW.generation<OLD.generation OR NEW.generation>OLD.generation+1
 OR (NEW.generation=OLD.generation AND (NEW.owner!=OLD.owner OR NEW.fence!=OLD.fence
  OR NEW.operation_id IS NOT OLD.operation_id OR NEW.attempt_revision IS NOT OLD.attempt_revision
  OR OLD.held!=1 OR NEW.until_ms<OLD.until_ms OR (NEW.held=0 AND NEW.until_ms!=OLD.until_ms)))
 OR (NEW.generation=OLD.generation+1 AND (NEW.fence=OLD.fence OR NEW.held!=1))
 OR (NEW.kind='collection' AND NOT EXISTS (SELECT 1 FROM operations o
  WHERE o.workspace_id=NEW.workspace_id AND o.operation_id=NEW.operation_id
  AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id AND o.kind='collect'))
 OR (NEW.kind='cleanup' AND NOT EXISTS (SELECT 1 FROM operations c
  WHERE c.workspace_id=NEW.workspace_id AND c.operation_id=NEW.cleanup_operation_id AND c.kind='cleanup' AND c.status='running'))
 BEGIN SELECT RAISE(ABORT,'invalid work lease transition'); END;

CREATE TRIGGER work_lease_keep BEFORE DELETE ON work_leases
 BEGIN SELECT RAISE(ABORT,'work lease history cannot be forgotten'); END;

CREATE TRIGGER collection_progress_owner_insert BEFORE INSERT ON collection_progress
 WHEN NOT EXISTS (SELECT 1 FROM work_leases c WHERE c.kind='collection' AND c.key=NEW.lease_key
 AND c.operation_id=NEW.operation_id AND c.generation=NEW.generation AND c.fence=NEW.fence AND c.held=1)
 BEGIN SELECT RAISE(ABORT,'collection progress owner mismatch'); END;

CREATE TRIGGER collection_progress_owner_update BEFORE UPDATE ON collection_progress
 WHEN NEW.workspace_id!=OLD.workspace_id OR NEW.job_id!=OLD.job_id OR NEW.attempt_id!=OLD.attempt_id
 OR NOT EXISTS (SELECT 1 FROM work_leases c WHERE c.kind='collection' AND c.key=NEW.lease_key
 AND c.operation_id=NEW.operation_id AND c.generation=NEW.generation AND c.fence=NEW.fence AND c.held=1)
 OR (NEW.generation=OLD.generation AND (NEW.operation_id!=OLD.operation_id OR NEW.fence!=OLD.fence
 OR json_extract(NEW.progress,'$.bytes_completed')<json_extract(OLD.progress,'$.bytes_completed')
 OR json_extract(NEW.progress,'$.bytes_received')<json_extract(OLD.progress,'$.bytes_received')))
 BEGIN SELECT RAISE(ABORT,'invalid collection progress transition'); END;

CREATE TRIGGER managed_operation_identity BEFORE UPDATE OF operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,credential_key,created_at,installation ON managed_connection_operations
 BEGIN SELECT RAISE(ABORT,'immutable management operation identity'); END;

CREATE TRIGGER immutable_receipt BEFORE UPDATE ON receipts
 BEGIN SELECT RAISE(ABORT,'immutable original receipt'); END;

CREATE TRIGGER receipt_keep BEFORE DELETE ON receipts
 BEGIN SELECT RAISE(ABORT,'receipts cannot be forgotten'); END;

CREATE TRIGGER receipt_owner BEFORE INSERT ON receipts
 WHEN (NEW.namespace='operation' AND NOT EXISTS(SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id AND o.operation_id=NEW.subject_id AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id AND o.kind=NEW.scope))
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

CREATE TRIGGER operation_cleanup_transition BEFORE UPDATE ON operations
 WHEN OLD.kind='cleanup' AND NOT ((OLD.status='accepted' AND NEW.status IN ('running','failed'))
 OR (OLD.status='running' AND NEW.status IN ('succeeded','failed')))
 BEGIN SELECT RAISE(ABORT,'invalid cleanup transition'); END;
CREATE TRIGGER operation_cleanup_keep BEFORE DELETE ON operations WHEN OLD.kind='cleanup'
 BEGIN SELECT RAISE(ABORT,'cleanup history cannot be forgotten'); END;

DROP TABLE _v28_operations;

DROP TABLE _v28_cleanup_resources;

DROP TABLE _v28_collection_publications;

DROP TABLE _v28_artifacts;

DROP TABLE _v28_retention_expirations;

DROP TABLE _v28_work_leases;

DROP TABLE _v28_collection_progress;

DROP TABLE _v28_attempt_grants;

DROP TABLE _v28_managed_connection_operations;

DROP TABLE _v28_receipts;

DROP TABLE _v28_cleanup_operations;

DROP TABLE _v28_operation_sequence;
