PRAGMA application_id=1129466969;
PRAGMA user_version=22;
CREATE TABLE schema_migrations (
     version INTEGER PRIMARY KEY CHECK(version>0), name TEXT NOT NULL UNIQUE,
     sha256 TEXT NOT NULL CHECK(length(sha256)=64), applied_at TEXT NOT NULL
   ) STRICT;
CREATE TABLE runtime_installation (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    installation_id TEXT NOT NULL UNIQUE CHECK (length(installation_id) BETWEEN 1 AND 128),
    created_at TEXT NOT NULL
) STRICT;
CREATE TABLE workspaces (
    workspace_id TEXT PRIMARY KEY NOT NULL CHECK (length(workspace_id) BETWEEN 1 AND 128),
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    allowed_profiles TEXT NOT NULL CHECK (length(allowed_profiles) BETWEEN 2 AND 20000)
) STRICT;
CREATE TABLE api_token_hashes (
    token_id TEXT PRIMARY KEY NOT NULL CHECK (length(token_id) BETWEEN 1 AND 128),
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    digest BLOB NOT NULL UNIQUE CHECK (length(digest) = 32),
    scopes TEXT NOT NULL CHECK (length(scopes) BETWEEN 2 AND 100),
    created_at TEXT NOT NULL,
    expires_at TEXT,
    revoked INTEGER NOT NULL DEFAULT 0 CHECK (revoked IN (0, 1))
) STRICT;
CREATE TABLE objects (
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    object_id TEXT NOT NULL CHECK (length(object_id) BETWEEN 1 AND 128),
    bytes INTEGER NOT NULL CHECK (bytes >= 0),
    sha256 TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
    PRIMARY KEY (workspace_id, object_id)
) STRICT;
CREATE TABLE profile_revisions (
 profile TEXT NOT NULL,
 revision TEXT NOT NULL,
 snapshot TEXT NOT NULL CHECK(length(snapshot)<=8192),
 PRIMARY KEY(profile,revision)
) STRICT;
CREATE TABLE profiles (
 profile TEXT PRIMARY KEY,
 revision TEXT NOT NULL,
 enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
 FOREIGN KEY(profile,revision) REFERENCES profile_revisions(profile,revision)
) STRICT;
CREATE TABLE jobs (
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 job_id TEXT NOT NULL,
 request TEXT NOT NULL CHECK(length(request)<=1048576),
 canonical_version TEXT NOT NULL,
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 profile TEXT NOT NULL,
 profile_revision TEXT NOT NULL,
 active_attempt_id TEXT NOT NULL,
 created_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,job_id),
 FOREIGN KEY(profile,profile_revision) REFERENCES profile_revisions(profile,revision),
 FOREIGN KEY(workspace_id,job_id,active_attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE attempts (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 number INTEGER NOT NULL CHECK(number>0),
 nonce TEXT NOT NULL CHECK(length(nonce)=64),
 state TEXT NOT NULL CHECK(length(state)<=4096),
 orchestration TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision>0),
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 UNIQUE(workspace_id,attempt_id),
 UNIQUE(workspace_id,job_id,number),
 FOREIGN KEY(workspace_id,job_id) REFERENCES jobs(workspace_id,job_id)
) STRICT;
CREATE TABLE job_objects (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 role TEXT NOT NULL,
 object_id TEXT NOT NULL,
 bytes INTEGER NOT NULL CHECK(bytes>=0),
 sha256 TEXT NOT NULL CHECK(length(sha256)=64),
 PRIMARY KEY(workspace_id,job_id,role),
 FOREIGN KEY(workspace_id,job_id) REFERENCES jobs(workspace_id,job_id),
 FOREIGN KEY(workspace_id,object_id) REFERENCES objects(workspace_id,object_id)
) STRICT;
CREATE TABLE events (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 event_id TEXT NOT NULL,
 sequence INTEGER NOT NULL CHECK(sequence>0),
 attempt_id TEXT NOT NULL,
 operation_id TEXT NOT NULL DEFAULT '',
 type TEXT NOT NULL,
 occurred_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,job_id,sequence),
 UNIQUE(workspace_id,event_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
CREATE TABLE idempotency (
 workspace_id TEXT NOT NULL,
 operation TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 canonical_version TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 receipt TEXT NOT NULL CHECK(length(receipt)<=4096),
 PRIMARY KEY(workspace_id,operation,key_sha256),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
CREATE TABLE scheduler_control (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 settings TEXT NOT NULL CHECK(length(settings)<=4096),
 last_workspace TEXT NOT NULL DEFAULT '',
 clock_ms INTEGER NOT NULL DEFAULT 0 CHECK(clock_ms>=0)
) STRICT;
CREATE TABLE scheduler_queue (
 queue_seq INTEGER PRIMARY KEY AUTOINCREMENT,
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 dispatch_barrier INTEGER NOT NULL DEFAULT 0 CHECK(dispatch_barrier IN (0,1)),
 not_before_ms INTEGER NOT NULL DEFAULT 0 CHECK(not_before_ms>=0),
 UNIQUE(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
CREATE TABLE scheduler_leases (
 queue_seq INTEGER PRIMARY KEY REFERENCES scheduler_queue(queue_seq),
 generation INTEGER NOT NULL CHECK(generation>0),
 owner TEXT NOT NULL,
 fence TEXT NOT NULL,
 until_ms INTEGER NOT NULL CHECK(until_ms>=0),
 held INTEGER NOT NULL CHECK(held IN (0,1)),
 CHECK((held=1 AND length(owner) BETWEEN 1 AND 128 AND length(fence)=64 AND until_ms>0)
 OR (held=0 AND owner='' AND fence='' AND until_ms=0))
) STRICT;
CREATE TABLE scheduler_accounts (
 account_scope TEXT PRIMARY KEY,
 policy TEXT NOT NULL CHECK(length(policy)<=2048)
) STRICT;
CREATE TABLE scheduler_quotas (
 account_scope TEXT NOT NULL,
 resource TEXT NOT NULL CHECK(resource IN ('cpu','gpu')),
 observation TEXT NOT NULL CHECK(length(observation)<=4096),
 observed_ms INTEGER NOT NULL CHECK(observed_ms>=0),
 exhausted INTEGER NOT NULL CHECK(exhausted IN (0,1)),
 PRIMARY KEY(account_scope,resource)
) STRICT;
CREATE TABLE provider_resources (
 resource_id TEXT PRIMARY KEY,
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 instance_id TEXT NOT NULL,
 account_scope TEXT NOT NULL,
 purpose TEXT NOT NULL CHECK(purpose IN ('staging','execution')),
 operation_id TEXT NOT NULL UNIQUE,
 resource_key TEXT NOT NULL,
 identity TEXT NOT NULL CHECK(length(identity)<=4096),
 plan_sha256 TEXT NOT NULL CHECK(length(plan_sha256)=64),
 resource_ref TEXT NOT NULL DEFAULT '' CHECK(length(resource_ref)<=4096),
 readiness TEXT NOT NULL DEFAULT 'unknown' CHECK(readiness IN ('unknown','pending','ready')),
 cleanup_state TEXT NOT NULL DEFAULT 'pinned' CHECK(cleanup_state='pinned'),
 created_at TEXT NOT NULL,
 UNIQUE(workspace_id,job_id,attempt_id,purpose),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
CREATE TABLE submission_intents (
 intent_id TEXT PRIMARY KEY,
 queue_seq INTEGER NOT NULL UNIQUE REFERENCES scheduler_queue(queue_seq),
 identity TEXT NOT NULL CHECK(length(identity)<=4096),
 prepared TEXT NOT NULL CHECK(length(prepared)<=8192),
 status TEXT NOT NULL CHECK(status IN ('started','accepted','rejected','unknown')),
 remote_ref TEXT NOT NULL DEFAULT '' CHECK(length(remote_ref)<=8192),
 created_at TEXT NOT NULL,
 CHECK((status='accepted' AND remote_ref!='') OR (status!='accepted' AND remote_ref=''))
) STRICT;
CREATE TABLE "dispatch_journals" (
 queue_seq INTEGER PRIMARY KEY REFERENCES scheduler_queue(queue_seq),
 version INTEGER NOT NULL CHECK(version>=0),
 phase TEXT NOT NULL CHECK(phase IN ('local','staging','ready','submitting','submitted','rejected','collectible','attention','failed','prevented')),
 journal TEXT NOT NULL CHECK(length(journal)<=2097152),
 CHECK(json_extract(journal,'$.version')=version),
 CHECK(json_extract(journal,'$.phase')=phase)
) STRICT;
CREATE TABLE operations (
 operation_seq INTEGER PRIMARY KEY AUTOINCREMENT,
 workspace_id TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('cancel','retry_compute','reconcile','collect')),
 status TEXT NOT NULL CHECK(status IN ('accepted','running','succeeded','failed','manual_required')),
 revision INTEGER NOT NULL CHECK(revision>0),
 created_at TEXT NOT NULL,
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 request_json TEXT NOT NULL CHECK(length(request_json)<=4096),
 record TEXT NOT NULL CHECK(length(record)<=8192),
 new_attempt_id TEXT,
 cancel_started INTEGER NOT NULL DEFAULT 0 CHECK(cancel_started IN (0,1)),
 UNIQUE(workspace_id,operation_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,new_attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
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
CREATE TABLE operation_idempotency (
 workspace_id TEXT NOT NULL,
 kind TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 canonical_version TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 receipt TEXT NOT NULL CHECK(length(receipt)<=4096),
 PRIMARY KEY(workspace_id,kind,key_sha256),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES operations(workspace_id,operation_id)
) STRICT;
CREATE TABLE collection_leases (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0),
 fence TEXT NOT NULL CHECK(length(fence)>=32),
 until_ms INTEGER NOT NULL,
 attempt_revision INTEGER NOT NULL CHECK(attempt_revision>0),
 held INTEGER NOT NULL CHECK(held IN (0,1)),
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES operations(workspace_id,operation_id)
) STRICT;
CREATE TABLE collection_snapshots (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 snapshot_sha256 TEXT NOT NULL CHECK(length(snapshot_sha256)=64),
 snapshot TEXT NOT NULL CHECK(length(CAST(snapshot AS BLOB))<=16777216),
 pinned_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
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
CREATE TABLE retention_inventory (
 inventory_seq INTEGER PRIMARY KEY AUTOINCREMENT,
 kind TEXT NOT NULL CHECK(kind IN ('input','result')),
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 object_id TEXT NOT NULL,
 bytes INTEGER NOT NULL CHECK(bytes>=0),
 sha256 TEXT NOT NULL CHECK(length(sha256)=64),
 recorded_at TEXT NOT NULL,
 expired_at TEXT,
 deleted_at TEXT,
 UNIQUE(kind,workspace_id,object_id),
 CHECK(deleted_at IS NULL OR expired_at IS NOT NULL)
) STRICT;
CREATE TABLE retention_holds (
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 target_kind TEXT NOT NULL CHECK(target_kind IN ('input','job','attempt')),
 target_id TEXT NOT NULL,
 hold_id TEXT NOT NULL,
 active INTEGER NOT NULL CHECK(active IN (0,1)),
 updated_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,target_kind,target_id,hold_id)
) STRICT;
CREATE TABLE retention_expirations (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 expired_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES collection_publications(workspace_id,job_id,attempt_id)
) STRICT;
CREATE TABLE retention_audit (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 workspace_id TEXT NOT NULL,
 target_kind TEXT NOT NULL,
 target_id TEXT NOT NULL,
 action TEXT NOT NULL CHECK(action IN ('hold','release','expire','delete')),
 occurred_at TEXT NOT NULL
) STRICT;
CREATE TABLE retention_cursor (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 inventory_seq INTEGER NOT NULL CHECK(inventory_seq>=0)
) STRICT;
CREATE TABLE cleanup_previews (
 resource_id TEXT PRIMARY KEY REFERENCES provider_resources(resource_id),
 plan_sha256 TEXT NOT NULL CHECK(length(plan_sha256)=64),
 outcome TEXT NOT NULL CHECK(outcome IN ('would_delete','already_absent','retained')),
 observed_at TEXT NOT NULL
) STRICT;
CREATE TABLE retention_roots (
 kind TEXT PRIMARY KEY CHECK(kind IN ('input','result')),
 blob_identity TEXT NOT NULL UNIQUE CHECK(length(blob_identity) BETWEEN 1 AND 128)
) STRICT;
CREATE TABLE managed_config (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 fingerprint_initialized INTEGER NOT NULL CHECK(fingerprint_initialized IN (0,1))
) STRICT;
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
, max_remote_wall_seconds INTEGER
 CHECK(max_remote_wall_seconds IS NULL OR max_remote_wall_seconds BETWEEN 1 AND 86400), host_evidence BLOB NOT NULL DEFAULT X''
 CHECK(length(host_evidence)<=1024), max_active_attempts INTEGER NOT NULL DEFAULT 0
 CHECK(max_active_attempts BETWEEN 0 AND 64), max_processes INTEGER NOT NULL DEFAULT 0
 CHECK(max_processes BETWEEN 0 AND 65536)) STRICT;
CREATE TABLE managed_credential_generations (
 credential_key TEXT PRIMARY KEY,
 connection_id TEXT NOT NULL REFERENCES managed_connections(connection_id),
 created_at TEXT NOT NULL
) STRICT;
CREATE TABLE managed_secret_deletions (
 credential_key TEXT PRIMARY KEY REFERENCES managed_credential_generations(credential_key)
) STRICT;
CREATE TABLE managed_profile_runtime (
 profile TEXT NOT NULL,
 revision TEXT NOT NULL,
 config BLOB NOT NULL CHECK(length(config)<=8192),
 PRIMARY KEY(profile,revision),
 FOREIGN KEY(profile,revision) REFERENCES profile_revisions(profile,revision)
) STRICT;
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
CREATE TABLE collection_progress (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0),
 fence TEXT NOT NULL CHECK(length(fence)>=32),
 stage TEXT NOT NULL CHECK(stage IN ('discovering','transferring','verifying','available','failed')),
 progress TEXT NOT NULL CHECK(length(progress)<=2048 AND json_valid(progress)),
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES collection_leases(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES operations(workspace_id,operation_id)
) STRICT;
CREATE TABLE preparation_progress (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0),
 progress TEXT NOT NULL CHECK(length(progress)<=1024 AND json_valid(progress)),
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
CREATE TABLE cleanup_operations (
 cleanup_seq INTEGER PRIMARY KEY AUTOINCREMENT,
 workspace_id TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 resource_id TEXT NOT NULL REFERENCES provider_resources(resource_id),
 status TEXT NOT NULL CHECK(status IN ('accepted','running','succeeded','failed')),
 revision INTEGER NOT NULL CHECK(revision>0),
 created_at TEXT NOT NULL,
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 request_json TEXT NOT NULL CHECK(length(request_json)<=4096),
 record TEXT NOT NULL CHECK(length(record)<=8192),
 UNIQUE(workspace_id,operation_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 CHECK(COALESCE(json_extract(record,'$.Operation.ID')=operation_id,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.WorkspaceID')=workspace_id,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.JobID')=job_id,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.AttemptID')=attempt_id,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.Kind')='cleanup',0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.Status')=status,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.Revision')=revision,0)),
 CHECK(COALESCE(json_extract(record,'$.Operation.CreatedAt')=created_at,0)),
 CHECK(COALESCE(json_extract(record,'$.Replay')=0,0)),
 CHECK(COALESCE((status IN ('accepted','running') AND json_extract(record,'$.Effect')='pending')
 OR (status='succeeded' AND json_extract(record,'$.Effect') IN ('remote_deleted','already_absent'))
 OR (status='failed' AND json_extract(record,'$.Effect')='failed'),0))
) STRICT;
CREATE TABLE cleanup_idempotency (
 workspace_id TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 canonical_version TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 receipt TEXT NOT NULL CHECK(length(receipt)<=4096),
 PRIMARY KEY(workspace_id,key_sha256),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES cleanup_operations(workspace_id,operation_id)
) STRICT;
CREATE TABLE cleanup_leases (
 workspace_id TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0),
 fence TEXT NOT NULL CHECK(length(fence)=64),
 until_ms INTEGER NOT NULL CHECK(until_ms>0),
 PRIMARY KEY(workspace_id,operation_id),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES cleanup_operations(workspace_id,operation_id)
) STRICT;
CREATE TABLE "managed_connection_operations" (
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
 updated_at TEXT NOT NULL, installation BLOB NOT NULL DEFAULT X''
 CHECK(length(installation)<=1024),
 UNIQUE(workspace_id,key_sha256)
) STRICT;
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
 FOREIGN KEY(workspace_id,operation_id) REFERENCES cleanup_operations(workspace_id,operation_id)
) STRICT;
CREATE TABLE managed_resource_accounts (
 account_scope TEXT PRIMARY KEY,
 resource_kind TEXT NOT NULL CHECK(length(resource_kind) BETWEEN 1 AND 128),
 resource_scope TEXT NOT NULL DEFAULT '' CHECK(resource_scope='' OR length(resource_scope)=73)
) STRICT;
CREATE TABLE scheduler_resource_reservations (
 queue_seq INTEGER PRIMARY KEY REFERENCES scheduler_queue(queue_seq),
 resource_kind TEXT NOT NULL CHECK(length(resource_kind) BETWEEN 1 AND 128),
 resource_scope TEXT NOT NULL CHECK(length(resource_scope)=73)
) STRICT;
INSERT INTO "schema_migrations" VALUES(1,'0001_identity.sql','20d73957c03ded0f24ac743e23ed6975246f2a2aa50de8f2cd5706e999c56a1d','2026-10-14T00:58:14.211321Z');
INSERT INTO "schema_migrations" VALUES(2,'0002_workspace_objects.sql','23fa96b850befd5ca1fb8db6d9e8697a0322624476d66e65a13c29fbec7d44ae','2026-10-14T00:58:14.211625Z');
INSERT INTO "schema_migrations" VALUES(3,'0003_admission.sql','e20173c79349c7ced7b79c357e9cf3abedc78c295f0e340c48a44b719a59031d','2026-10-14T00:58:14.212372Z');
INSERT INTO "schema_migrations" VALUES(4,'0004_scheduler.sql','0f700afbb305c80df33c425651ffc5189651af7738233129cb3aa4637d4b1575','2026-10-14T00:58:14.213029Z');
INSERT INTO "schema_migrations" VALUES(5,'0005_dispatch.sql','6151f59b2b362e1b73543fffc0a8a1653d41284177cdf0f9b01b8093dfd37d54','2026-10-14T00:58:14.213595Z');
INSERT INTO "schema_migrations" VALUES(6,'0006_operations.sql','d2f23ab35dd7dde59e49aa2e60b42d524ac35f0f9fc4d68db9107c801a5876b7','2026-10-14T00:58:14.21595Z');
INSERT INTO "schema_migrations" VALUES(7,'0007_collection.sql','7035b159fe7947a6ee55056e48a2fb1c80b2e0bba54c97515d48bae261a533b1','2026-10-14T00:58:14.216628Z');
INSERT INTO "schema_migrations" VALUES(8,'0008_retention.sql','3d2c2a6678dca327a87d6779d35efd30db1275f68c060cbbb79fdcf8c995b6ad','2026-10-14T00:58:14.217597Z');
INSERT INTO "schema_migrations" VALUES(9,'0009_retention_roots.sql','4ea9f136b568646613467716ca0f67662e2f4f4bfc891ed3154c49c4b4b81e78','2026-10-14T00:58:14.217756Z');
INSERT INTO "schema_migrations" VALUES(10,'0010_managed_connections.sql','ebc2183edb541d214823ad2caff163c2fdfebbac6c931e5664cefd181f13dbc5','2026-10-14T00:58:14.218361Z');
INSERT INTO "schema_migrations" VALUES(11,'0011_execution_authorization.sql','56c7114ae536dd74a58fcf4f9238183c90ee8725c5aa70603327feb6133deed6','2026-10-14T00:58:14.218654Z');
INSERT INTO "schema_migrations" VALUES(12,'0012_collection_progress.sql','1856d9995c682f374c040307df13fbe2b911cda27d6dde40c5349b27772400df','2026-10-14T00:58:14.218927Z');
INSERT INTO "schema_migrations" VALUES(13,'0013_connection_configuration.sql','521a2da75f70fd51e47d3c17640c70261a35dcc81c2dc098a69cde618dd123e0','2026-10-14T00:58:14.223542Z');
INSERT INTO "schema_migrations" VALUES(14,'0014_preparation_progress.sql','cefc2b8c6f80a12c1e0104b90b0f1dd7d2b50b38d209ec0df1a81db16e415fe6','2026-10-14T00:58:14.223646Z');
INSERT INTO "schema_migrations" VALUES(15,'0015_remote_cleanup.sql','f49f069e0e9090f68d9f715046ed7d9f373c841ec4b2d35c6417ed52a5ede8b8','2026-10-14T00:58:14.224517Z');
INSERT INTO "schema_migrations" VALUES(16,'0016_host_evidence.sql','d7e6de95216edde30e285f65519b03929975f4d404c596b22b2423a970c9da11','2026-10-14T00:58:14.22572Z');
INSERT INTO "schema_migrations" VALUES(17,'0017_execution_limits.sql','55c1e35f710ae013377fb8593e11ae7acb88576f5229b660c20c7f60a00ba67c','2026-10-14T00:58:14.227984Z');
INSERT INTO "schema_migrations" VALUES(18,'0018_image_installation.sql','96399646976406dd8afb6a77ead738f41402d952c79b6554fe22d1ea3aa8f015','2026-10-14T00:58:14.232782Z');
INSERT INTO "schema_migrations" VALUES(19,'0019_execution_gates.sql','c4ef339a684c860a2f950c068227723279424e5d934acc00bd7b2abe988862cb','2026-10-14T00:58:14.233308Z');
INSERT INTO "schema_migrations" VALUES(20,'0020_workload_secrets.sql','e4eb39f7cbd07af139d136eb8f81af26cf970844fe46bcc767545732885b148a','2026-10-14T00:58:14.233584Z');
INSERT INTO "schema_migrations" VALUES(21,'0021_cleanup_resources.sql','18d1a698cf1617f23b01ecef1f26b7081ee7a2561eed71443e1f6de7d0595d6b','2026-10-14T00:58:14.2339Z');
INSERT INTO "schema_migrations" VALUES(22,'0022_scheduling_resources.sql','c881039f8249a851bc82efb672f11f297b6e34ba23c0892ca8dae875c531b046','2026-10-14T00:58:14.234237Z');
INSERT INTO "runtime_installation" VALUES(1,'rt_d3ed567cd940c7ddb6160c77a7b49ccd','2026-10-14T00:58:14.234254Z');
INSERT INTO "workspaces" VALUES('a',1,'["fixture"]');
INSERT INTO "workspaces" VALUES('b',1,'["fixture"]');
INSERT INTO "api_token_hashes" VALUES('tok_66bb8f351117c1e3e3fbc861ccc1c1f1','a',X'AFC5EBC001F4049ABE3EB0EA20F820525A740BB257CB656A805FDA3DFEFE8779','["execute","operate","read","write"]','2026-10-14T00:58:14.318565Z',NULL,0);
INSERT INTO "objects" VALUES('a','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "objects" VALUES('a','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "objects" VALUES('b','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "objects" VALUES('b','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "profile_revisions" VALUES('fixture','one','{"binding":{"Profile":"fixture","ProviderInstanceID":"fake_1","ConfigurationRevision":"one"},"account_scope":"shared_account","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}');
INSERT INTO "profile_revisions" VALUES('fixture','managed','{"binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"account_scope":"shared_account","credential_ref":"vault:con_fixture","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}');
INSERT INTO "profiles" VALUES('fixture','managed',1);
INSERT INTO "jobs" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"result_collection":"manual","timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}}','compute-relay/job-request/v1','51248eb2350c459fa1417d9b154fac7d06e227f1e006d6daf1f4d432525c732b','fixture','managed','att_d1909a9cb724c93c76464facf17d3c0a','2026-10-14T00:58:14.321456Z');
INSERT INTO "jobs" VALUES('a','job_4ff5df457357d717326105c514dab367','{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"result_collection":"manual","timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}}','compute-relay/job-request/v1','51248eb2350c459fa1417d9b154fac7d06e227f1e006d6daf1f4d432525c732b','fixture','managed','att_bdd76f7ad52573268d0343c061adfbde','2026-10-14T00:58:14.338969Z');
INSERT INTO "jobs" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}}','compute-relay/job-request/v1','c516e672576037a05a7e5b2c18b6bd441803a549363fb75b6635514e07d9c245','fixture','managed','att_4bdb352efbe53dd4efb2568bfefab62d','2026-10-14T00:58:14.433335Z');
INSERT INTO "jobs" VALUES('a','job_801a6082d8e1ed37e27307df750267ec','{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}}','compute-relay/job-request/v1','c516e672576037a05a7e5b2c18b6bd441803a549363fb75b6635514e07d9c245','fixture','managed','att_e8c0893a294f147b21c11bd9a7836e35','2026-10-14T00:58:14.446626Z');
INSERT INTO "jobs" VALUES('a','job_77836ad480042b78f65c55d564a78612','{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}}','compute-relay/job-request/v1','c516e672576037a05a7e5b2c18b6bd441803a549363fb75b6635514e07d9c245','fixture','managed','att_a82fb63abe21b558a62f43baf4e1b388','2026-10-14T00:58:14.44755Z');
INSERT INTO "attempts" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','att_d1909a9cb724c93c76464facf17d3c0a',1,'385a1ea51e20fd46dd83ba540575b4edfc574ac4b83d9c6bad09c2958e4c9cf5','{"Orchestration":"collecting","Execution":"succeeded","Result":"not_available","Cancellation":"not_requested","RemoteActivity":"inactive","ReleaseEvidence":"not_observable","DeadlineExceeded":false}','collecting',6,'2026-10-14T00:58:14.321456Z','2026-10-14T01:38:15.251Z');
INSERT INTO "attempts" VALUES('a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde',1,'4c2c16f5c30a7b93c5f16027d7ede9ad630c8c112697fceaefb9d70557333dd2','{"Orchestration":"succeeded","Execution":"succeeded","Result":"available","Cancellation":"not_requested","RemoteActivity":"inactive","ReleaseEvidence":"not_observable","DeadlineExceeded":false}','succeeded',8,'2026-10-14T00:58:14.338969Z','2026-10-14T02:18:15.251Z');
INSERT INTO "attempts" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','att_4bdb352efbe53dd4efb2568bfefab62d',1,'dfa89b0d20e64489bb10bd4af02799c1c020a051a6df1e0f1a3e2a8a67e344f0','{"Orchestration":"running","Execution":"running","Result":"not_available","Cancellation":"not_requested","RemoteActivity":"active","ReleaseEvidence":"not_observable","DeadlineExceeded":false}','running',5,'2026-10-14T00:58:14.433335Z','2026-10-14T02:48:15.251Z');
INSERT INTO "attempts" VALUES('a','job_801a6082d8e1ed37e27307df750267ec','att_e8c0893a294f147b21c11bd9a7836e35',1,'69e0a7186dbb02acd05adcc364f357bfb4ba59b56224bed2dcd24803bd22915f','{"Orchestration":"queued","Execution":"not_submitted","Result":"not_available","Cancellation":"not_requested","RemoteActivity":"not_started","ReleaseEvidence":"unknown","DeadlineExceeded":false}','queued',1,'2026-10-14T00:58:14.446626Z','2026-10-14T00:58:14.446626Z');
INSERT INTO "attempts" VALUES('a','job_77836ad480042b78f65c55d564a78612','att_a82fb63abe21b558a62f43baf4e1b388',1,'f7da033db4b5db8f5a8f77bba59df51d48e4025478ae5098a903ba986e96bf65','{"Orchestration":"queued","Execution":"not_submitted","Result":"not_available","Cancellation":"not_requested","RemoteActivity":"not_started","ReleaseEvidence":"unknown","DeadlineExceeded":false}','queued',1,'2026-10-14T00:58:14.44755Z','2026-10-14T00:58:14.44755Z');
INSERT INTO "job_objects" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','bundle','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "job_objects" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','input:0','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "job_objects" VALUES('a','job_4ff5df457357d717326105c514dab367','bundle','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "job_objects" VALUES('a','job_4ff5df457357d717326105c514dab367','input:0','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "job_objects" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','bundle','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "job_objects" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','input:0','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "job_objects" VALUES('a','job_801a6082d8e1ed37e27307df750267ec','bundle','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "job_objects" VALUES('a','job_801a6082d8e1ed37e27307df750267ec','input:0','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "job_objects" VALUES('a','job_77836ad480042b78f65c55d564a78612','bundle','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "job_objects" VALUES('a','job_77836ad480042b78f65c55d564a78612','input:0','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_a81991fa74e39ea95a330cf7a8b91852',1,'att_d1909a9cb724c93c76464facf17d3c0a','','job.accepted','2026-10-14T00:58:14.321456Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_f2d9a78edd2f0a137e982867a02756ce',2,'att_d1909a9cb724c93c76464facf17d3c0a','','scheduler.claimed','2026-10-14T01:08:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_b5e6bd034831ad858e6d8e232a68875c',3,'att_d1909a9cb724c93c76464facf17d3c0a','','inputs.ready','2026-10-14T01:08:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_cddf36fd307bc75086fbab3a9767c82e',4,'att_d1909a9cb724c93c76464facf17d3c0a','','preparation.intent_recorded','2026-10-14T01:08:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_e0a4617067ed271723ec78ead70a5e5f',5,'att_d1909a9cb724c93c76464facf17d3c0a','','provider.staging.ready','2026-10-14T01:08:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_6791e4893a560b6b0055495365344a20',6,'att_d1909a9cb724c93c76464facf17d3c0a','','scheduler.released','2026-10-14T01:08:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_f035eb9676e2a1f5e5004d7794ec8a09',7,'att_d1909a9cb724c93c76464facf17d3c0a','','scheduler.claimed','2026-10-14T01:18:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_9a4334a8c872e9cd2ecd5295bcd9d7e7',8,'att_d1909a9cb724c93c76464facf17d3c0a','','submission.intent_recorded','2026-10-14T01:18:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_ac54b7f4b2e86eb7a92e1f3257a74c8d',9,'att_d1909a9cb724c93c76464facf17d3c0a','','submission.accepted','2026-10-14T01:18:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_ca522f561e448846cc7091531eb3c072',10,'att_d1909a9cb724c93c76464facf17d3c0a','','scheduler.released','2026-10-14T01:18:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_ea06ee075f8fc7c856b1966b9070bdca',11,'att_d1909a9cb724c93c76464facf17d3c0a','','scheduler.claimed','2026-10-14T01:28:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_1bd4137635f38b39b2cbd7399fe5e8d7',12,'att_d1909a9cb724c93c76464facf17d3c0a','','execution.observed','2026-10-14T01:28:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_7e40425418af88f4fa7fde299a4f1d4c',13,'att_d1909a9cb724c93c76464facf17d3c0a','','scheduler.released','2026-10-14T01:28:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_6fcafd9e92e105347c362219723af35f',14,'att_d1909a9cb724c93c76464facf17d3c0a','','scheduler.claimed','2026-10-14T01:38:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_12a6613ab3e734c8d6ab11da0c7ae05a',15,'att_d1909a9cb724c93c76464facf17d3c0a','','execution.observed','2026-10-14T01:38:15.251Z');
INSERT INTO "events" VALUES('a','job_5964d6def72399c0a3bbd0a05717f7de','evt_699ad83e8cde6164a60fdb25307ebfde',16,'att_d1909a9cb724c93c76464facf17d3c0a','','scheduler.released','2026-10-14T01:38:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_e997761dfc916603281703c94d78270b',1,'att_bdd76f7ad52573268d0343c061adfbde','','job.accepted','2026-10-14T00:58:14.338969Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_4844f9914addef5adc7193cece96eee2',2,'att_bdd76f7ad52573268d0343c061adfbde','','scheduler.claimed','2026-10-14T01:48:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_b847742f508547169674319c7f074e4d',3,'att_bdd76f7ad52573268d0343c061adfbde','','inputs.ready','2026-10-14T01:48:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_7feb6ba1d40393be07d672d8e3a58300',4,'att_bdd76f7ad52573268d0343c061adfbde','','preparation.intent_recorded','2026-10-14T01:48:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_bf71b257e7ad6b6582c4429d9df25a09',5,'att_bdd76f7ad52573268d0343c061adfbde','','provider.staging.ready','2026-10-14T01:48:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_2a8c8ec23bfee5b834387370e4f38b3e',6,'att_bdd76f7ad52573268d0343c061adfbde','','scheduler.released','2026-10-14T01:48:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_7c5240388c00951b1357789bf0a81cca',7,'att_bdd76f7ad52573268d0343c061adfbde','','scheduler.claimed','2026-10-14T01:58:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_96b622eea31baee46a8365c829d1a19c',8,'att_bdd76f7ad52573268d0343c061adfbde','','submission.intent_recorded','2026-10-14T01:58:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_ac25b44c3ae0d5a7ae16b91a842afb7c',9,'att_bdd76f7ad52573268d0343c061adfbde','','submission.accepted','2026-10-14T01:58:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_dfec0852a5fbf7e4fdc1e6917232b8e5',10,'att_bdd76f7ad52573268d0343c061adfbde','','scheduler.released','2026-10-14T01:58:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_f8cb7e0ba31a5818e0c96a9572d8cbea',11,'att_bdd76f7ad52573268d0343c061adfbde','','scheduler.claimed','2026-10-14T02:08:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_ab02ac2add570fa13c35b0b09867da5d',12,'att_bdd76f7ad52573268d0343c061adfbde','','execution.observed','2026-10-14T02:08:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_0ee48c6ec99f3740b44013a11ef2f752',13,'att_bdd76f7ad52573268d0343c061adfbde','','scheduler.released','2026-10-14T02:08:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_d67a9ad7397ea30d2b00ea1ad9b89b5e',14,'att_bdd76f7ad52573268d0343c061adfbde','','scheduler.claimed','2026-10-14T02:18:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_fb502bc11811598d18e8e7d577a507b2',15,'att_bdd76f7ad52573268d0343c061adfbde','','execution.observed','2026-10-14T02:18:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_732b7e3b27ceae558429162facd65313',16,'att_bdd76f7ad52573268d0343c061adfbde','','scheduler.released','2026-10-14T02:18:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_b6f2851c9dcb70a8bfa95e6929a57fa7',17,'att_bdd76f7ad52573268d0343c061adfbde','op_db5c6e961c439caba8664b58ae35ba44','operation.accepted','2026-10-14T02:18:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_64e1139464946bbdf35840a08c8eb64f',18,'att_bdd76f7ad52573268d0343c061adfbde','op_db5c6e961c439caba8664b58ae35ba44','collection.requested','2026-10-14T02:18:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_03de6f17bf4441deefcd5cb13bd1b57c',19,'att_bdd76f7ad52573268d0343c061adfbde','op_db5c6e961c439caba8664b58ae35ba44','collection.requested','2026-10-14T02:18:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_ad98fe8c3edfb2614559da9d21167dc9',20,'att_bdd76f7ad52573268d0343c061adfbde','op_db5c6e961c439caba8664b58ae35ba44','artifact.verified','2026-10-14T02:18:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_9ab1b8993ad30975cf0803a9c6a625f9',21,'att_bdd76f7ad52573268d0343c061adfbde','op_db5c6e961c439caba8664b58ae35ba44','job.completed','2026-10-14T02:18:15.251Z');
INSERT INTO "events" VALUES('a','job_4ff5df457357d717326105c514dab367','evt_2f86d8606512e993b4cfad461468b713',22,'att_bdd76f7ad52573268d0343c061adfbde','op_db5c6e961c439caba8664b58ae35ba44','operation.completed','2026-10-14T02:18:15.251Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_fcc713fb16d3945e2196b4110d50d4b6',1,'att_4bdb352efbe53dd4efb2568bfefab62d','','job.accepted','2026-10-14T00:58:14.433335Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_8300e63efaa2057d17031c49e82e4535',2,'att_4bdb352efbe53dd4efb2568bfefab62d','','scheduler.claimed','2026-10-14T02:28:15.251Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_247f3d25921477753fa0b84746d7cdc8',3,'att_4bdb352efbe53dd4efb2568bfefab62d','','inputs.ready','2026-10-14T02:28:15.251Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_bfd87296af9c268b9a1bdeeb8e2ee9f8',4,'att_4bdb352efbe53dd4efb2568bfefab62d','','preparation.intent_recorded','2026-10-14T02:28:15.251Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_ed1d8590c4f241767dcfedee5a9b6336',5,'att_4bdb352efbe53dd4efb2568bfefab62d','','provider.staging.ready','2026-10-14T02:28:15.251Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_635397d16b0c8c88327520ff440134ea',6,'att_4bdb352efbe53dd4efb2568bfefab62d','','scheduler.released','2026-10-14T02:28:15.251Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_f2e6be87396db7e6d1f0c7279f8d10ec',7,'att_4bdb352efbe53dd4efb2568bfefab62d','','scheduler.claimed','2026-10-14T02:38:15.251Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_d5ed4449d411b602fa5f3466fdb6b378',8,'att_4bdb352efbe53dd4efb2568bfefab62d','','submission.intent_recorded','2026-10-14T02:38:15.251Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_4974c9bc8c3d71f5cf4d8e29f7956d10',9,'att_4bdb352efbe53dd4efb2568bfefab62d','','submission.accepted','2026-10-14T02:38:15.251Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_589b2ace0dee17a6ebe886bd595c1b6d',10,'att_4bdb352efbe53dd4efb2568bfefab62d','','scheduler.released','2026-10-14T02:38:15.251Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_7cfbef6952aa5b26db0c066ca315bdcf',11,'att_4bdb352efbe53dd4efb2568bfefab62d','','scheduler.claimed','2026-10-14T02:48:15.251Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_65a5e6ed7b23a91907ddd6c72281daa7',12,'att_4bdb352efbe53dd4efb2568bfefab62d','','execution.observed','2026-10-14T02:48:15.251Z');
INSERT INTO "events" VALUES('a','job_3a2d0ddc123b4299759733212182de7d','evt_bd14cb045563d8004251463fcd26f049',13,'att_4bdb352efbe53dd4efb2568bfefab62d','','scheduler.released','2026-10-14T02:48:15.251Z');
INSERT INTO "events" VALUES('a','job_801a6082d8e1ed37e27307df750267ec','evt_5cef4edb561e219b67edc4ce6809bf29',1,'att_e8c0893a294f147b21c11bd9a7836e35','','job.accepted','2026-10-14T00:58:14.446626Z');
INSERT INTO "events" VALUES('a','job_77836ad480042b78f65c55d564a78612','evt_eba417ba205764e39c26a577bb8835ab',1,'att_a82fb63abe21b558a62f43baf4e1b388','','job.accepted','2026-10-14T00:58:14.44755Z');
INSERT INTO "idempotency" VALUES('a','job.create','1fef4edf1b2475e5a29f279a5dbe86c2d535d4c7be31414cee1bc57cc62f9ff9','51248eb2350c459fa1417d9b154fac7d06e227f1e006d6daf1f4d432525c732b','compute-relay/job-request/v1','job_5964d6def72399c0a3bbd0a05717f7de','att_d1909a9cb724c93c76464facf17d3c0a','{"job_id":"job_5964d6def72399c0a3bbd0a05717f7de","attempt_id":"att_d1909a9cb724c93c76464facf17d3c0a","status":"queued","created_at":"2026-10-14T00:58:14.321456Z","idempotency_replay":false,"links":{"self":"/v1/workspaces/a/jobs/job_5964d6def72399c0a3bbd0a05717f7de","events":"/v1/workspaces/a/jobs/job_5964d6def72399c0a3bbd0a05717f7de/events","attempts":"/v1/workspaces/a/jobs/job_5964d6def72399c0a3bbd0a05717f7de/attempts","artifacts":"/v1/workspaces/a/jobs/job_5964d6def72399c0a3bbd0a05717f7de/artifacts"}}');
INSERT INTO "idempotency" VALUES('a','job.create','782c401a4476ff2d60ce99b82e856067c191363c382f5dd47f7a80e142b4e420','51248eb2350c459fa1417d9b154fac7d06e227f1e006d6daf1f4d432525c732b','compute-relay/job-request/v1','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','{"job_id":"job_4ff5df457357d717326105c514dab367","attempt_id":"att_bdd76f7ad52573268d0343c061adfbde","status":"queued","created_at":"2026-10-14T00:58:14.338969Z","idempotency_replay":false,"links":{"self":"/v1/workspaces/a/jobs/job_4ff5df457357d717326105c514dab367","events":"/v1/workspaces/a/jobs/job_4ff5df457357d717326105c514dab367/events","attempts":"/v1/workspaces/a/jobs/job_4ff5df457357d717326105c514dab367/attempts","artifacts":"/v1/workspaces/a/jobs/job_4ff5df457357d717326105c514dab367/artifacts"}}');
INSERT INTO "idempotency" VALUES('a','job.create','29d1667047ed802d85e84f9c813d355c41d61fd0a442da5eb736dba4ed4311dc','c516e672576037a05a7e5b2c18b6bd441803a549363fb75b6635514e07d9c245','compute-relay/job-request/v1','job_3a2d0ddc123b4299759733212182de7d','att_4bdb352efbe53dd4efb2568bfefab62d','{"job_id":"job_3a2d0ddc123b4299759733212182de7d","attempt_id":"att_4bdb352efbe53dd4efb2568bfefab62d","status":"queued","created_at":"2026-10-14T00:58:14.433335Z","idempotency_replay":false,"links":{"self":"/v1/workspaces/a/jobs/job_3a2d0ddc123b4299759733212182de7d","events":"/v1/workspaces/a/jobs/job_3a2d0ddc123b4299759733212182de7d/events","attempts":"/v1/workspaces/a/jobs/job_3a2d0ddc123b4299759733212182de7d/attempts","artifacts":"/v1/workspaces/a/jobs/job_3a2d0ddc123b4299759733212182de7d/artifacts"}}');
INSERT INTO "idempotency" VALUES('a','job.create','b21194ebbf3067cde6fe28afa6cf0730b7b025c4215aacb3c25417d73554c93e','c516e672576037a05a7e5b2c18b6bd441803a549363fb75b6635514e07d9c245','compute-relay/job-request/v1','job_801a6082d8e1ed37e27307df750267ec','att_e8c0893a294f147b21c11bd9a7836e35','{"job_id":"job_801a6082d8e1ed37e27307df750267ec","attempt_id":"att_e8c0893a294f147b21c11bd9a7836e35","status":"queued","created_at":"2026-10-14T00:58:14.446626Z","idempotency_replay":false,"links":{"self":"/v1/workspaces/a/jobs/job_801a6082d8e1ed37e27307df750267ec","events":"/v1/workspaces/a/jobs/job_801a6082d8e1ed37e27307df750267ec/events","attempts":"/v1/workspaces/a/jobs/job_801a6082d8e1ed37e27307df750267ec/attempts","artifacts":"/v1/workspaces/a/jobs/job_801a6082d8e1ed37e27307df750267ec/artifacts"}}');
INSERT INTO "idempotency" VALUES('a','job.create','8a3961f2163bbd092703531e37b2ed534439e2aff7ab09653d3d27e5b216f0e5','c516e672576037a05a7e5b2c18b6bd441803a549363fb75b6635514e07d9c245','compute-relay/job-request/v1','job_77836ad480042b78f65c55d564a78612','att_a82fb63abe21b558a62f43baf4e1b388','{"job_id":"job_77836ad480042b78f65c55d564a78612","attempt_id":"att_a82fb63abe21b558a62f43baf4e1b388","status":"queued","created_at":"2026-10-14T00:58:14.44755Z","idempotency_replay":false,"links":{"self":"/v1/workspaces/a/jobs/job_77836ad480042b78f65c55d564a78612","events":"/v1/workspaces/a/jobs/job_77836ad480042b78f65c55d564a78612/events","attempts":"/v1/workspaces/a/jobs/job_77836ad480042b78f65c55d564a78612/attempts","artifacts":"/v1/workspaces/a/jobs/job_77836ad480042b78f65c55d564a78612/artifacts"}}');
INSERT INTO "scheduler_control" VALUES(1,'{"paused":false,"max_workers":4,"max_active_per_account":1,"lease_duration_ns":30000000000,"quota_fresh_for_ns":300000000000,"blocked_recheck_ns":300000000000}','a',1791946095251);
INSERT INTO "scheduler_queue" VALUES(1,'a','job_5964d6def72399c0a3bbd0a05717f7de','att_d1909a9cb724c93c76464facf17d3c0a',1,1791941913251);
INSERT INTO "scheduler_queue" VALUES(2,'a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde',1,1791944313251);
INSERT INTO "scheduler_queue" VALUES(3,'a','job_3a2d0ddc123b4299759733212182de7d','att_4bdb352efbe53dd4efb2568bfefab62d',1,1791946113251);
INSERT INTO "scheduler_queue" VALUES(4,'a','job_801a6082d8e1ed37e27307df750267ec','att_e8c0893a294f147b21c11bd9a7836e35',0,0);
INSERT INTO "scheduler_queue" VALUES(5,'a','job_77836ad480042b78f65c55d564a78612','att_a82fb63abe21b558a62f43baf4e1b388',0,0);
INSERT INTO "scheduler_leases" VALUES(1,4,'','',0,0);
INSERT INTO "scheduler_leases" VALUES(2,4,'','',0,0);
INSERT INTO "scheduler_leases" VALUES(3,3,'','',0,0);
INSERT INTO "provider_resources" VALUES('res_staging_si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9','a','job_5964d6def72399c0a3bbd0a05717f7de','att_d1909a9cb724c93c76464facf17d3c0a','con_fixture','shared_account','staging','prep_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9','cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e','{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_5964d6def72399c0a3bbd0a05717f7de","AttemptID":"att_d1909a9cb724c93c76464facf17d3c0a","InstanceID":"con_fixture","IntentID":"si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9","ResourceKey":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Nonce":"385a1ea51e20fd46dd83ba540575b4edfc574ac4b83d9c6bad09c2958e4c9cf5","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','7e7e6a7fa12ab23f467a2645c65fb2f1753cec24c0b80c4a86817e50b1da345d','cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e','ready','pinned','2026-10-14T01:08:15.251Z');
INSERT INTO "provider_resources" VALUES('res_execution_si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9','a','job_5964d6def72399c0a3bbd0a05717f7de','att_d1909a9cb724c93c76464facf17d3c0a','con_fixture','shared_account','execution','si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9','cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e','{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_5964d6def72399c0a3bbd0a05717f7de","AttemptID":"att_d1909a9cb724c93c76464facf17d3c0a","InstanceID":"con_fixture","IntentID":"si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9","ResourceKey":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Nonce":"385a1ea51e20fd46dd83ba540575b4edfc574ac4b83d9c6bad09c2958e4c9cf5","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','7e7e6a7fa12ab23f467a2645c65fb2f1753cec24c0b80c4a86817e50b1da345d','{"resource":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","version":"fixture-1"}','ready','pinned','2026-10-14T01:18:15.251Z');
INSERT INTO "provider_resources" VALUES('res_staging_si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd','a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','con_fixture','shared_account','staging','prep_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd','cr-757de4856e03dd38061afea99eedd3a34f317805','{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_4ff5df457357d717326105c514dab367","AttemptID":"att_bdd76f7ad52573268d0343c061adfbde","InstanceID":"con_fixture","IntentID":"si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd","ResourceKey":"cr-757de4856e03dd38061afea99eedd3a34f317805","Nonce":"4c2c16f5c30a7b93c5f16027d7ede9ad630c8c112697fceaefb9d70557333dd2","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','f717cdcd75389aa3ada2c21c6bc8d8abc5323a921b2ba6a516857b17698111d7','cr-757de4856e03dd38061afea99eedd3a34f317805','ready','pinned','2026-10-14T01:48:15.251Z');
INSERT INTO "provider_resources" VALUES('res_execution_si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd','a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','con_fixture','shared_account','execution','si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd','cr-757de4856e03dd38061afea99eedd3a34f317805','{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_4ff5df457357d717326105c514dab367","AttemptID":"att_bdd76f7ad52573268d0343c061adfbde","InstanceID":"con_fixture","IntentID":"si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd","ResourceKey":"cr-757de4856e03dd38061afea99eedd3a34f317805","Nonce":"4c2c16f5c30a7b93c5f16027d7ede9ad630c8c112697fceaefb9d70557333dd2","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','f717cdcd75389aa3ada2c21c6bc8d8abc5323a921b2ba6a516857b17698111d7','{"resource":"cr-757de4856e03dd38061afea99eedd3a34f317805","version":"fixture-1"}','ready','pinned','2026-10-14T01:58:15.251Z');
INSERT INTO "provider_resources" VALUES('res_staging_si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae','a','job_3a2d0ddc123b4299759733212182de7d','att_4bdb352efbe53dd4efb2568bfefab62d','con_fixture','shared_account','staging','prep_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae','cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e','{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_3a2d0ddc123b4299759733212182de7d","AttemptID":"att_4bdb352efbe53dd4efb2568bfefab62d","InstanceID":"con_fixture","IntentID":"si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae","ResourceKey":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Nonce":"dfa89b0d20e64489bb10bd4af02799c1c020a051a6df1e0f1a3e2a8a67e344f0","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','bfabb9a99da1704e87ce9af4c795195d0ced07ca4b618ed49b732d4d5485c817','cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e','ready','pinned','2026-10-14T02:28:15.251Z');
INSERT INTO "provider_resources" VALUES('res_execution_si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae','a','job_3a2d0ddc123b4299759733212182de7d','att_4bdb352efbe53dd4efb2568bfefab62d','con_fixture','shared_account','execution','si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae','cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e','{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_3a2d0ddc123b4299759733212182de7d","AttemptID":"att_4bdb352efbe53dd4efb2568bfefab62d","InstanceID":"con_fixture","IntentID":"si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae","ResourceKey":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Nonce":"dfa89b0d20e64489bb10bd4af02799c1c020a051a6df1e0f1a3e2a8a67e344f0","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','bfabb9a99da1704e87ce9af4c795195d0ced07ca4b618ed49b732d4d5485c817','{"resource":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","version":"fixture-1"}','ready','pinned','2026-10-14T02:38:15.251Z');
INSERT INTO "submission_intents" VALUES('si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9',1,'{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_5964d6def72399c0a3bbd0a05717f7de","AttemptID":"att_d1909a9cb724c93c76464facf17d3c0a","InstanceID":"con_fixture","IntentID":"si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9","ResourceKey":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Nonce":"385a1ea51e20fd46dd83ba540575b4edfc574ac4b83d9c6bad09c2958e4c9cf5","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_5964d6def72399c0a3bbd0a05717f7de","AttemptID":"att_d1909a9cb724c93c76464facf17d3c0a","InstanceID":"con_fixture","IntentID":"si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9","ResourceKey":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Nonce":"385a1ea51e20fd46dd83ba540575b4edfc574ac4b83d9c6bad09c2958e4c9cf5","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"PreparationID":"prep_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9","PlanSHA256":"7e7e6a7fa12ab23f467a2645c65fb2f1753cec24c0b80c4a86817e50b1da345d","Resource":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Ready":true,"Private":true}','accepted','{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_5964d6def72399c0a3bbd0a05717f7de","AttemptID":"att_d1909a9cb724c93c76464facf17d3c0a","InstanceID":"con_fixture","IntentID":"si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9","ResourceKey":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Nonce":"385a1ea51e20fd46dd83ba540575b4edfc574ac4b83d9c6bad09c2958e4c9cf5","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Version":"fixture-1"}','2026-10-14T01:18:15.251Z');
INSERT INTO "submission_intents" VALUES('si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd',2,'{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_4ff5df457357d717326105c514dab367","AttemptID":"att_bdd76f7ad52573268d0343c061adfbde","InstanceID":"con_fixture","IntentID":"si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd","ResourceKey":"cr-757de4856e03dd38061afea99eedd3a34f317805","Nonce":"4c2c16f5c30a7b93c5f16027d7ede9ad630c8c112697fceaefb9d70557333dd2","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_4ff5df457357d717326105c514dab367","AttemptID":"att_bdd76f7ad52573268d0343c061adfbde","InstanceID":"con_fixture","IntentID":"si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd","ResourceKey":"cr-757de4856e03dd38061afea99eedd3a34f317805","Nonce":"4c2c16f5c30a7b93c5f16027d7ede9ad630c8c112697fceaefb9d70557333dd2","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"PreparationID":"prep_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd","PlanSHA256":"f717cdcd75389aa3ada2c21c6bc8d8abc5323a921b2ba6a516857b17698111d7","Resource":"cr-757de4856e03dd38061afea99eedd3a34f317805","Ready":true,"Private":true}','accepted','{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_4ff5df457357d717326105c514dab367","AttemptID":"att_bdd76f7ad52573268d0343c061adfbde","InstanceID":"con_fixture","IntentID":"si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd","ResourceKey":"cr-757de4856e03dd38061afea99eedd3a34f317805","Nonce":"4c2c16f5c30a7b93c5f16027d7ede9ad630c8c112697fceaefb9d70557333dd2","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-757de4856e03dd38061afea99eedd3a34f317805","Version":"fixture-1"}','2026-10-14T01:58:15.251Z');
INSERT INTO "submission_intents" VALUES('si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae',3,'{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_3a2d0ddc123b4299759733212182de7d","AttemptID":"att_4bdb352efbe53dd4efb2568bfefab62d","InstanceID":"con_fixture","IntentID":"si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae","ResourceKey":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Nonce":"dfa89b0d20e64489bb10bd4af02799c1c020a051a6df1e0f1a3e2a8a67e344f0","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_3a2d0ddc123b4299759733212182de7d","AttemptID":"att_4bdb352efbe53dd4efb2568bfefab62d","InstanceID":"con_fixture","IntentID":"si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae","ResourceKey":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Nonce":"dfa89b0d20e64489bb10bd4af02799c1c020a051a6df1e0f1a3e2a8a67e344f0","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"PreparationID":"prep_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae","PlanSHA256":"bfabb9a99da1704e87ce9af4c795195d0ced07ca4b618ed49b732d4d5485c817","Resource":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Ready":true,"Private":true}','accepted','{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_3a2d0ddc123b4299759733212182de7d","AttemptID":"att_4bdb352efbe53dd4efb2568bfefab62d","InstanceID":"con_fixture","IntentID":"si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae","ResourceKey":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Nonce":"dfa89b0d20e64489bb10bd4af02799c1c020a051a6df1e0f1a3e2a8a67e344f0","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Version":"fixture-1"}','2026-10-14T02:38:15.251Z');
INSERT INTO "dispatch_journals" VALUES(1,6,'collectible','{"version":6,"phase":"collectible","plan":{"Job":{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_5964d6def72399c0a3bbd0a05717f7de","AttemptID":"att_d1909a9cb724c93c76464facf17d3c0a","InstanceID":"con_fixture","IntentID":"si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9","ResourceKey":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Nonce":"385a1ea51e20fd46dd83ba540575b4edfc574ac4b83d9c6bad09c2958e4c9cf5","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"Specification":{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"result_collection":"manual","timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}},"SpecificationSHA256":"f8582f7adf3c1ef026959a65cd6a3c83640306318914d75cb924331af1931b9b","Required":["batch_execution","execution_timeout","private_input_staging","python","remote_network_control","strong_execution_identity"],"WallSeconds":10,"Inputs":{"bundle":{"object_id":"code","workspace_id":"a","bytes":327,"sha256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338"},"inputs":[{"name":"data","target":"data.txt","object":{"object_id":"data","workspace_id":"a","bytes":12,"sha256":"23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040"}}]}},"VerifyAfterStart":null},"preparation_id":"prep_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9","prepared":{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_5964d6def72399c0a3bbd0a05717f7de","AttemptID":"att_d1909a9cb724c93c76464facf17d3c0a","InstanceID":"con_fixture","IntentID":"si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9","ResourceKey":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Nonce":"385a1ea51e20fd46dd83ba540575b4edfc574ac4b83d9c6bad09c2958e4c9cf5","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"PreparationID":"prep_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9","PlanSHA256":"7e7e6a7fa12ab23f467a2645c65fb2f1753cec24c0b80c4a86817e50b1da345d","Resource":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Ready":true,"Private":true},"submit_started":true,"remote":{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_5964d6def72399c0a3bbd0a05717f7de","AttemptID":"att_d1909a9cb724c93c76464facf17d3c0a","InstanceID":"con_fixture","IntentID":"si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9","ResourceKey":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Nonce":"385a1ea51e20fd46dd83ba540575b4edfc574ac4b83d9c6bad09c2958e4c9cf5","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Version":"fixture-1"},"observation":{"Remote":{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_5964d6def72399c0a3bbd0a05717f7de","AttemptID":"att_d1909a9cb724c93c76464facf17d3c0a","InstanceID":"con_fixture","IntentID":"si_2853853ed8e9d44e6bc296b89b64f62f81177b5e1613f0fa96849f42d72e9ce9","ResourceKey":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Nonce":"385a1ea51e20fd46dd83ba540575b4edfc574ac4b83d9c6bad09c2958e4c9cf5","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-2853853ed8e9d44e6bc296b89b64f62f81177b5e","Version":"fixture-1"},"Execution":"succeeded","RawState":"succeeded","RemoteActivity":"inactive","ReleaseEvidence":"not_observable","ObservedAt":"2026-10-14T01:38:15.251Z"},"failures":0}');
INSERT INTO "dispatch_journals" VALUES(2,7,'collectible','{"version":7,"phase":"collectible","plan":{"Job":{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_4ff5df457357d717326105c514dab367","AttemptID":"att_bdd76f7ad52573268d0343c061adfbde","InstanceID":"con_fixture","IntentID":"si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd","ResourceKey":"cr-757de4856e03dd38061afea99eedd3a34f317805","Nonce":"4c2c16f5c30a7b93c5f16027d7ede9ad630c8c112697fceaefb9d70557333dd2","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"Specification":{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"result_collection":"manual","timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}},"SpecificationSHA256":"f8582f7adf3c1ef026959a65cd6a3c83640306318914d75cb924331af1931b9b","Required":["batch_execution","execution_timeout","private_input_staging","python","remote_network_control","strong_execution_identity"],"WallSeconds":10,"Inputs":{"bundle":{"object_id":"code","workspace_id":"a","bytes":327,"sha256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338"},"inputs":[{"name":"data","target":"data.txt","object":{"object_id":"data","workspace_id":"a","bytes":12,"sha256":"23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040"}}]}},"VerifyAfterStart":null},"preparation_id":"prep_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd","prepared":{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_4ff5df457357d717326105c514dab367","AttemptID":"att_bdd76f7ad52573268d0343c061adfbde","InstanceID":"con_fixture","IntentID":"si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd","ResourceKey":"cr-757de4856e03dd38061afea99eedd3a34f317805","Nonce":"4c2c16f5c30a7b93c5f16027d7ede9ad630c8c112697fceaefb9d70557333dd2","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"PreparationID":"prep_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd","PlanSHA256":"f717cdcd75389aa3ada2c21c6bc8d8abc5323a921b2ba6a516857b17698111d7","Resource":"cr-757de4856e03dd38061afea99eedd3a34f317805","Ready":true,"Private":true},"submit_started":true,"remote":{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_4ff5df457357d717326105c514dab367","AttemptID":"att_bdd76f7ad52573268d0343c061adfbde","InstanceID":"con_fixture","IntentID":"si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd","ResourceKey":"cr-757de4856e03dd38061afea99eedd3a34f317805","Nonce":"4c2c16f5c30a7b93c5f16027d7ede9ad630c8c112697fceaefb9d70557333dd2","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-757de4856e03dd38061afea99eedd3a34f317805","Version":"fixture-1"},"observation":{"Remote":{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_4ff5df457357d717326105c514dab367","AttemptID":"att_bdd76f7ad52573268d0343c061adfbde","InstanceID":"con_fixture","IntentID":"si_757de4856e03dd38061afea99eedd3a34f317805d46099ea706f0ba81cb52bbd","ResourceKey":"cr-757de4856e03dd38061afea99eedd3a34f317805","Nonce":"4c2c16f5c30a7b93c5f16027d7ede9ad630c8c112697fceaefb9d70557333dd2","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-757de4856e03dd38061afea99eedd3a34f317805","Version":"fixture-1"},"Execution":"succeeded","RawState":"succeeded","RemoteActivity":"inactive","ReleaseEvidence":"not_observable","ObservedAt":"2026-10-14T02:18:15.251Z"},"failures":0}');
INSERT INTO "dispatch_journals" VALUES(3,5,'submitted','{"version":5,"phase":"submitted","plan":{"Job":{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_3a2d0ddc123b4299759733212182de7d","AttemptID":"att_4bdb352efbe53dd4efb2568bfefab62d","InstanceID":"con_fixture","IntentID":"si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae","ResourceKey":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Nonce":"dfa89b0d20e64489bb10bd4af02799c1c020a051a6df1e0f1a3e2a8a67e344f0","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"Specification":{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}},"SpecificationSHA256":"1ccf6ccab4061e2d40bca9f611c1281f37592b7ec822c1b7e1256f89cb4c421c","Required":["batch_execution","execution_timeout","private_input_staging","python","remote_network_control","strong_execution_identity"],"WallSeconds":10,"Inputs":{"bundle":{"object_id":"code","workspace_id":"a","bytes":327,"sha256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338"},"inputs":[{"name":"data","target":"data.txt","object":{"object_id":"data","workspace_id":"a","bytes":12,"sha256":"23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040"}}]}},"VerifyAfterStart":null},"preparation_id":"prep_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae","prepared":{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_3a2d0ddc123b4299759733212182de7d","AttemptID":"att_4bdb352efbe53dd4efb2568bfefab62d","InstanceID":"con_fixture","IntentID":"si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae","ResourceKey":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Nonce":"dfa89b0d20e64489bb10bd4af02799c1c020a051a6df1e0f1a3e2a8a67e344f0","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"PreparationID":"prep_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae","PlanSHA256":"bfabb9a99da1704e87ce9af4c795195d0ced07ca4b618ed49b732d4d5485c817","Resource":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Ready":true,"Private":true},"submit_started":true,"remote":{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_3a2d0ddc123b4299759733212182de7d","AttemptID":"att_4bdb352efbe53dd4efb2568bfefab62d","InstanceID":"con_fixture","IntentID":"si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae","ResourceKey":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Nonce":"dfa89b0d20e64489bb10bd4af02799c1c020a051a6df1e0f1a3e2a8a67e344f0","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Version":"fixture-1"},"observation":{"Remote":{"Identity":{"InstallationID":"rt_d3ed567cd940c7ddb6160c77a7b49ccd","WorkspaceID":"a","JobID":"job_3a2d0ddc123b4299759733212182de7d","AttemptID":"att_4bdb352efbe53dd4efb2568bfefab62d","InstanceID":"con_fixture","IntentID":"si_c6064a5c4221ac2234101b2c02a315b5a67b6e2e58e5844caaf4fb4a5aae4aae","ResourceKey":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Nonce":"dfa89b0d20e64489bb10bd4af02799c1c020a051a6df1e0f1a3e2a8a67e344f0","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-c6064a5c4221ac2234101b2c02a315b5a67b6e2e","Version":"fixture-1"},"Execution":"running","RawState":"running","RemoteActivity":"active","ReleaseEvidence":"not_observable","ObservedAt":"2026-10-14T02:48:15.251Z"},"failures":0}');
INSERT INTO "operations" VALUES(1,'a','op_db5c6e961c439caba8664b58ae35ba44','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','collect','succeeded',2,'2026-10-14T02:18:15.251Z','0556d2ec45e963710c6e090ae35a3d7ee727d240ad79ef907750097d290da2b7','{"attempt_id":"att_bdd76f7ad52573268d0343c061adfbde","reason":"explicit operator request"}','{"Operation":{"ID":"op_db5c6e961c439caba8664b58ae35ba44","WorkspaceID":"a","JobID":"job_4ff5df457357d717326105c514dab367","AttemptID":"att_bdd76f7ad52573268d0343c061adfbde","Kind":"collect","Status":"succeeded","Revision":2,"Failure":null,"CreatedAt":"2026-10-14T02:18:15.251Z","UpdatedAt":"2026-10-14T02:18:15.251Z"},"Effect":"results_available","NewAttemptID":"","TerminationConfirmed":false,"Replay":false}',NULL,0);
INSERT INTO "operation_idempotency" VALUES('a','collect','cecddcbb0af060ef17ed3c720c7487313ca84c2661d6eec7c9abe7646e036ffc','0556d2ec45e963710c6e090ae35a3d7ee727d240ad79ef907750097d290da2b7','control/v1','op_db5c6e961c439caba8664b58ae35ba44','{"Operation":{"ID":"op_db5c6e961c439caba8664b58ae35ba44","WorkspaceID":"a","JobID":"job_4ff5df457357d717326105c514dab367","AttemptID":"att_bdd76f7ad52573268d0343c061adfbde","Kind":"collect","Status":"accepted","Revision":1,"Failure":null,"CreatedAt":"2026-10-14T02:18:15.251Z","UpdatedAt":"2026-10-14T02:18:15.251Z"},"Effect":"collection_requested","NewAttemptID":"","TerminationConfirmed":false,"Replay":false}');
INSERT INTO "collection_leases" VALUES('a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','op_db5c6e961c439caba8664b58ae35ba44',1,'2e9d5591a31451e41d726726b0991abec17d8a64cdf01f2c2c93693efa827b3d',1791944925251,7,0);
INSERT INTO "collection_snapshots" VALUES('a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','95ada61d3460acb2c73988e84e4f3ab25173794afe2dd0ec50e340b0ab852e34','{"manifest":"{\"artifacts\":[{\"bytes\":57344,\"path\":\"answer.json\",\"sha256\":\"4ddfc63fe431bec0b2e3b39fcf81babbacce5b66bfe1a3f8170659b533e53373\"}],\"attempt_id\":\"att_bdd76f7ad52573268d0343c061adfbde\",\"attempt_nonce\":\"4c2c16f5c30a7b93c5f16027d7ede9ad630c8c112697fceaefb9d70557333dd2\",\"bundle_sha256\":\"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338\",\"error\":null,\"exit_code\":0,\"finished_at\":\"2026-10-14T02:18:15Z\",\"input_manifest_sha256\":\"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592\",\"job_id\":\"job_4ff5df457357d717326105c514dab367\",\"manifest_version\":\"1\",\"phase\":\"completed\",\"resource_check\":{\"gpu_required\":false,\"gpu_verified\":false},\"runner_version\":\"offline-fixture\",\"started_at\":\"2026-10-14T02:18:14Z\",\"timed_out\":false}","files":[{"artifact_id":"art_672a61f83bd2b744d9aeb47ad7a49545c78ccb8942c0a7c98a643f8c556d8cc6","path":"control/environment.json","role":"provenance","object":{"object_id":"art_672a61f83bd2b744d9aeb47ad7a49545c78ccb8942c0a7c98a643f8c556d8cc6","workspace_id":"a","bytes":16,"sha256":"ffbc2dfc402782325da71132100e74ff511d1585dd80e4ea196ed4bcace3fef2"},"media_type":"application/json"},{"artifact_id":"art_6fa2734b20077720a710210b0fe09a210457011caa6a915bcd2232a68b55fee1","path":"control/execution-result.json","role":"manifest","object":{"object_id":"art_6fa2734b20077720a710210b0fe09a210457011caa6a915bcd2232a68b55fee1","workspace_id":"a","bytes":742,"sha256":"0109b0b5ba38e2e52987e258bed2a7a0e5a4775b3657ef85f1f868cef6e26180"},"media_type":"application/json"},{"artifact_id":"art_478d1ebeebd3ba9c96e7353dd33ae2206c7775b13fffada7852398318824b3a6","path":"control/stderr.log","role":"log","object":{"object_id":"art_478d1ebeebd3ba9c96e7353dd33ae2206c7775b13fffada7852398318824b3a6","workspace_id":"a","bytes":0,"sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},"media_type":"text/plain"},{"artifact_id":"art_96b1126787955daa7a575fd24d4b88cb3228eff01c353ef52a91e6786b86c26d","path":"control/stdout.log","role":"log","object":{"object_id":"art_96b1126787955daa7a575fd24d4b88cb3228eff01c353ef52a91e6786b86c26d","workspace_id":"a","bytes":15,"sha256":"4d3ac42b53378ed2def23b3bf88787f9fffed17b22bd42ce971d7067dc591dab"},"media_type":"text/plain"},{"artifact_id":"art_f81f8d6d569f7939ae26c3401fc4525484a9dd05262a6c4f71479a7887a97269","path":"outputs/answer.json","role":"output","object":{"object_id":"art_f81f8d6d569f7939ae26c3401fc4525484a9dd05262a6c4f71479a7887a97269","workspace_id":"a","bytes":57344,"sha256":"4ddfc63fe431bec0b2e3b39fcf81babbacce5b66bfe1a3f8170659b533e53373"}}]}','2026-10-14T02:18:15.251Z');
INSERT INTO "collection_publications" VALUES('a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','op_db5c6e961c439caba8664b58ae35ba44','95ada61d3460acb2c73988e84e4f3ab25173794afe2dd0ec50e340b0ab852e34','completed',5,'2026-10-14T02:18:15.251Z');
INSERT INTO "artifacts" VALUES('a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','art_672a61f83bd2b744d9aeb47ad7a49545c78ccb8942c0a7c98a643f8c556d8cc6','art_672a61f83bd2b744d9aeb47ad7a49545c78ccb8942c0a7c98a643f8c556d8cc6','control/environment.json','provenance',16,'ffbc2dfc402782325da71132100e74ff511d1585dd80e4ea196ed4bcace3fef2','application/json');
INSERT INTO "artifacts" VALUES('a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','art_6fa2734b20077720a710210b0fe09a210457011caa6a915bcd2232a68b55fee1','art_6fa2734b20077720a710210b0fe09a210457011caa6a915bcd2232a68b55fee1','control/execution-result.json','manifest',742,'0109b0b5ba38e2e52987e258bed2a7a0e5a4775b3657ef85f1f868cef6e26180','application/json');
INSERT INTO "artifacts" VALUES('a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','art_478d1ebeebd3ba9c96e7353dd33ae2206c7775b13fffada7852398318824b3a6','art_478d1ebeebd3ba9c96e7353dd33ae2206c7775b13fffada7852398318824b3a6','control/stderr.log','log',0,'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855','text/plain');
INSERT INTO "artifacts" VALUES('a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','art_96b1126787955daa7a575fd24d4b88cb3228eff01c353ef52a91e6786b86c26d','art_96b1126787955daa7a575fd24d4b88cb3228eff01c353ef52a91e6786b86c26d','control/stdout.log','log',15,'4d3ac42b53378ed2def23b3bf88787f9fffed17b22bd42ce971d7067dc591dab','text/plain');
INSERT INTO "artifacts" VALUES('a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','art_f81f8d6d569f7939ae26c3401fc4525484a9dd05262a6c4f71479a7887a97269','art_f81f8d6d569f7939ae26c3401fc4525484a9dd05262a6c4f71479a7887a97269','outputs/answer.json','output',57344,'4ddfc63fe431bec0b2e3b39fcf81babbacce5b66bfe1a3f8170659b533e53373','');
INSERT INTO "retention_inventory" VALUES(1,'input','a','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338','2026-10-14T00:58:14.267Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(2,'input','a','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040','2026-10-14T00:58:14.283Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(3,'input','b','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040','2026-10-14T00:58:14.300Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(4,'input','b','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338','2026-10-14T00:58:14.316Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(5,'result','a','art_672a61f83bd2b744d9aeb47ad7a49545c78ccb8942c0a7c98a643f8c556d8cc6',16,'ffbc2dfc402782325da71132100e74ff511d1585dd80e4ea196ed4bcace3fef2','2026-10-14T00:58:14.431Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(6,'result','a','art_6fa2734b20077720a710210b0fe09a210457011caa6a915bcd2232a68b55fee1',742,'0109b0b5ba38e2e52987e258bed2a7a0e5a4775b3657ef85f1f868cef6e26180','2026-10-14T00:58:14.431Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(7,'result','a','art_478d1ebeebd3ba9c96e7353dd33ae2206c7775b13fffada7852398318824b3a6',0,'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855','2026-10-14T00:58:14.431Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(8,'result','a','art_96b1126787955daa7a575fd24d4b88cb3228eff01c353ef52a91e6786b86c26d',15,'4d3ac42b53378ed2def23b3bf88787f9fffed17b22bd42ce971d7067dc591dab','2026-10-14T00:58:14.431Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(9,'result','a','art_f81f8d6d569f7939ae26c3401fc4525484a9dd05262a6c4f71479a7887a97269',57344,'4ddfc63fe431bec0b2e3b39fcf81babbacce5b66bfe1a3f8170659b533e53373','2026-10-14T00:58:14.431Z',NULL,NULL);
INSERT INTO "retention_cursor" VALUES(1,0);
INSERT INTO "managed_config" VALUES(1,0);
INSERT INTO "managed_connections" VALUES('con_fixture','a','fixture','fixture',1,'verified','enabled','fixture_account','shared_account','fixture_key','fixture','','2026-10-14T00:58:15.251Z',NULL,X'',0,0);
INSERT INTO "execution_authorizations" VALUES('a','grant_bc272bc3b6953a6622d63c7cbd43a1ef','job_5964d6def72399c0a3bbd0a05717f7de','att_d1909a9cb724c93c76464facf17d3c0a','ef9dc7e361ea257a3397332127f95a31b364be4a4a924c9cc1d60933df80e1ec','8113493e43871c7a6568be03da295ef264242489696f716c7ee9848d2393fa4b','{"binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"account_scope":"shared_account","credential_ref":"vault:con_fixture","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}','shared_account','5da4ce3965f702c51427a1c6cb497e5307e8620c7ffb31946d4c643edf41a497','385a1ea51e20fd46dd83ba540575b4edfc574ac4b83d9c6bad09c2958e4c9cf5',10,'tok_66bb8f351117c1e3e3fbc861ccc1c1f1','2026-10-14T00:58:15.251Z','{"authorization_id":"grant_bc272bc3b6953a6622d63c7cbd43a1ef","workspace_id":"a","job_id":"job_5964d6def72399c0a3bbd0a05717f7de","attempt_id":"att_d1909a9cb724c93c76464facf17d3c0a","status":"granted","max_remote_wall_seconds":10,"created_at":"2026-10-14T00:58:15.251Z","consumed_at":null,"replay":false}','consumed','2026-10-14T01:08:15.251Z');
INSERT INTO "execution_authorizations" VALUES('a','grant_2fb804a2ad0d46cc36b8b6c60857001a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','0a2727f2b07e6f35a37f0168badd6b88c0d28b09e8ceb475d39b7fdc3f0b4e48','9ba0d7d1dbc8b9401a0ca576c41041b35026dfe1b90f6e68127e9b9e330f1848','{"binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"account_scope":"shared_account","credential_ref":"vault:con_fixture","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}','shared_account','5da4ce3965f702c51427a1c6cb497e5307e8620c7ffb31946d4c643edf41a497','4c2c16f5c30a7b93c5f16027d7ede9ad630c8c112697fceaefb9d70557333dd2',10,'tok_66bb8f351117c1e3e3fbc861ccc1c1f1','2026-10-14T01:38:15.251Z','{"authorization_id":"grant_2fb804a2ad0d46cc36b8b6c60857001a","workspace_id":"a","job_id":"job_4ff5df457357d717326105c514dab367","attempt_id":"att_bdd76f7ad52573268d0343c061adfbde","status":"granted","max_remote_wall_seconds":10,"created_at":"2026-10-14T01:38:15.251Z","consumed_at":null,"replay":false}','consumed','2026-10-14T01:48:15.251Z');
INSERT INTO "execution_authorizations" VALUES('a','grant_fd39fb4f3e7d822dfca30d77a7acf116','job_3a2d0ddc123b4299759733212182de7d','att_4bdb352efbe53dd4efb2568bfefab62d','fbeb3905b22132c34fa98644cee6b50540f94d030a5778a01b0dc3674c2e6760','4e05bef826ee5e326a9f877994017b4ca6e72330c813827b3b21eae5fdf2b87e','{"binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"account_scope":"shared_account","credential_ref":"vault:con_fixture","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}','shared_account','91c1b6b4447c801db007e012937ecd8dfa7f3320e52eb5bcf15d19b8e0124a92','dfa89b0d20e64489bb10bd4af02799c1c020a051a6df1e0f1a3e2a8a67e344f0',10,'tok_66bb8f351117c1e3e3fbc861ccc1c1f1','2026-10-14T02:18:15.251Z','{"authorization_id":"grant_fd39fb4f3e7d822dfca30d77a7acf116","workspace_id":"a","job_id":"job_3a2d0ddc123b4299759733212182de7d","attempt_id":"att_4bdb352efbe53dd4efb2568bfefab62d","status":"granted","max_remote_wall_seconds":10,"created_at":"2026-10-14T02:18:15.251Z","consumed_at":null,"replay":false}','consumed','2026-10-14T02:28:15.251Z');
INSERT INTO "execution_authorizations" VALUES('a','grant_a1c94e6ddb957a5001de0e989cff1ecd','job_801a6082d8e1ed37e27307df750267ec','att_e8c0893a294f147b21c11bd9a7836e35','43f6d503f6b5ffd94f4f93ac8a2390a4f2cc549acc7cfb13e255bb786b3ca758','87fe4858f380c77f08af7a1571626239cf9faaa6a03db6a967a10b674ec58b1c','{"binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"account_scope":"shared_account","credential_ref":"vault:con_fixture","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}','shared_account','91c1b6b4447c801db007e012937ecd8dfa7f3320e52eb5bcf15d19b8e0124a92','69e0a7186dbb02acd05adcc364f357bfb4ba59b56224bed2dcd24803bd22915f',10,'tok_66bb8f351117c1e3e3fbc861ccc1c1f1','2026-10-14T02:48:15.251Z','{"authorization_id":"grant_a1c94e6ddb957a5001de0e989cff1ecd","workspace_id":"a","job_id":"job_801a6082d8e1ed37e27307df750267ec","attempt_id":"att_e8c0893a294f147b21c11bd9a7836e35","status":"granted","max_remote_wall_seconds":10,"created_at":"2026-10-14T02:48:15.251Z","consumed_at":null,"replay":false}','granted',NULL);
INSERT INTO "collection_progress" VALUES('a','job_4ff5df457357d717326105c514dab367','att_bdd76f7ad52573268d0343c061adfbde','op_db5c6e961c439caba8664b58ae35ba44',1,'2e9d5591a31451e41d726726b0991abec17d8a64cdf01f2c2c93693efa827b3d','available','{"scope":"selected_output_bytes","generation":1,"bytes_completed":57344,"bytes_received":null,"bytes_total":57344,"observed_at":"2026-10-14T02:18:15.251Z"}');
INSERT INTO "sqlite_sequence" VALUES('scheduler_queue',5);
INSERT INTO "sqlite_sequence" VALUES('retention_inventory',9);
INSERT INTO "sqlite_sequence" VALUES('operations',1);
CREATE INDEX api_token_workspace ON api_token_hashes(workspace_id);
CREATE INDEX job_objects_owner ON job_objects(workspace_id,object_id);
CREATE INDEX attempts_outstanding ON attempts(orchestration,workspace_id);
CREATE INDEX scheduler_workspace_order ON scheduler_queue(workspace_id,queue_seq);
CREATE INDEX dispatch_due ON dispatch_journals(phase,queue_seq);
CREATE UNIQUE INDEX operation_one_cancel ON operations(workspace_id,job_id,attempt_id) WHERE kind='cancel';
CREATE UNIQUE INDEX operation_one_retry ON operations(workspace_id,job_id,attempt_id) WHERE kind='retry_compute';
CREATE UNIQUE INDEX operation_pending ON operations(workspace_id,job_id,attempt_id,kind) WHERE status IN ('accepted','running');
CREATE INDEX operation_work ON operations(kind,status,operation_seq);
CREATE INDEX collection_capacity ON collection_leases(held,until_ms);
CREATE INDEX retention_pending ON retention_inventory(deleted_at,expired_at,inventory_seq);
CREATE INDEX managed_connections_workspace ON managed_connections(workspace_id,connection_id);
CREATE INDEX execution_authorization_account ON execution_authorizations(account_scope,status);
CREATE UNIQUE INDEX cleanup_pending ON cleanup_operations(workspace_id,job_id,attempt_id) WHERE status IN ('accepted','running');
CREATE INDEX cleanup_work ON cleanup_operations(status,cleanup_seq);
CREATE INDEX preparation_account_holds ON preparation_authorizations(account_scope,storage_released);
CREATE INDEX managed_resource_members ON managed_resource_accounts(resource_scope,account_scope);
CREATE TRIGGER immutable_profile_revision BEFORE UPDATE ON profile_revisions BEGIN SELECT RAISE(ABORT,'immutable profile revision'); END;
CREATE TRIGGER immutable_job_request BEFORE UPDATE OF request,canonical_version,request_sha256,profile,profile_revision,created_at,job_id,workspace_id ON jobs BEGIN SELECT RAISE(ABORT,'immutable job request'); END;
CREATE TRIGGER immutable_job_object BEFORE UPDATE ON job_objects BEGIN SELECT RAISE(ABORT,'immutable job object'); END;
CREATE TRIGGER immutable_idempotency BEFORE UPDATE ON idempotency BEGIN SELECT RAISE(ABORT,'immutable admission receipt'); END;
CREATE TRIGGER immutable_event BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT,'immutable event'); END;
CREATE TRIGGER immutable_attempt_identity BEFORE UPDATE OF workspace_id,job_id,attempt_id,number,nonce,created_at ON attempts BEGIN SELECT RAISE(ABORT,'immutable attempt identity'); END;
CREATE TRIGGER immutable_referenced_object BEFORE UPDATE ON objects WHEN EXISTS(SELECT 1 FROM job_objects WHERE workspace_id=OLD.workspace_id AND object_id=OLD.object_id) BEGIN SELECT RAISE(ABORT,'referenced object is immutable'); END;
CREATE TRIGGER scheduler_enqueue AFTER INSERT ON attempts BEGIN
 INSERT INTO scheduler_queue(workspace_id,job_id,attempt_id,dispatch_barrier)
 VALUES(NEW.workspace_id,NEW.job_id,NEW.attempt_id,
 CASE WHEN NEW.orchestration NOT IN ('queued','preparing','blocked') OR
 json_extract(NEW.state,'$.Execution')!='not_submitted' OR
 json_extract(NEW.state,'$.RemoteActivity')!='not_started' THEN 1 ELSE 0 END);
END;
CREATE TRIGGER scheduler_barrier AFTER UPDATE OF state,orchestration ON attempts
 WHEN NEW.orchestration NOT IN ('queued','preparing','blocked') OR
 json_extract(NEW.state,'$.Execution')!='not_submitted' OR
 json_extract(NEW.state,'$.RemoteActivity')!='not_started'
 BEGIN UPDATE scheduler_queue SET dispatch_barrier=1
 WHERE workspace_id=NEW.workspace_id AND job_id=NEW.job_id AND attempt_id=NEW.attempt_id; END;
CREATE TRIGGER scheduler_identity BEFORE UPDATE OF queue_seq,workspace_id,job_id,attempt_id ON scheduler_queue
 BEGIN SELECT RAISE(ABORT,'immutable queue identity'); END;
CREATE TRIGGER scheduler_barrier_monotonic BEFORE UPDATE OF dispatch_barrier ON scheduler_queue
 WHEN OLD.dispatch_barrier=1 AND NEW.dispatch_barrier=0
 BEGIN SELECT RAISE(ABORT,'dispatch barrier cannot be cleared'); END;
CREATE TRIGGER scheduler_generation BEFORE UPDATE OF generation ON scheduler_leases
 WHEN NEW.generation<=OLD.generation
 BEGIN SELECT RAISE(ABORT,'lease generation must increase'); END;
CREATE TRIGGER resource_identity BEFORE UPDATE OF resource_id,workspace_id,job_id,attempt_id,instance_id,account_scope,purpose,operation_id,resource_key,identity,plan_sha256,created_at ON provider_resources
 BEGIN SELECT RAISE(ABORT,'immutable provider resource intent'); END;
CREATE TRIGGER resource_reference BEFORE UPDATE OF resource_ref ON provider_resources
 WHEN OLD.resource_ref!='' AND NEW.resource_ref!=OLD.resource_ref
 BEGIN SELECT RAISE(ABORT,'immutable discovered resource'); END;
CREATE TRIGGER resource_readiness BEFORE UPDATE OF readiness ON provider_resources
 WHEN OLD.readiness='ready' AND NEW.readiness!='ready'
 BEGIN SELECT RAISE(ABORT,'readiness cannot be silently reset'); END;
CREATE TRIGGER submission_identity BEFORE UPDATE OF intent_id,queue_seq,identity,prepared,created_at ON submission_intents
 BEGIN SELECT RAISE(ABORT,'immutable submission intent'); END;
CREATE TRIGGER submission_reference BEFORE UPDATE OF remote_ref ON submission_intents
 WHEN OLD.remote_ref!='' AND NEW.remote_ref!=OLD.remote_ref
 BEGIN SELECT RAISE(ABORT,'immutable execution reference'); END;
CREATE TRIGGER submission_terminal BEFORE UPDATE OF status ON submission_intents
 WHEN (OLD.status IN ('accepted','rejected') AND NEW.status!=OLD.status)
 OR (OLD.status!='started' AND NEW.status='started')
 BEGIN SELECT RAISE(ABORT,'submission outcome cannot be reset'); END;
CREATE TRIGGER dispatch_version BEFORE UPDATE ON dispatch_journals
 WHEN NEW.queue_seq!=OLD.queue_seq OR NEW.version!=OLD.version+1
 BEGIN SELECT RAISE(ABORT,'invalid dispatch journal version'); END;
CREATE TRIGGER operation_identity BEFORE UPDATE OF workspace_id,operation_id,job_id,attempt_id,kind,created_at,request_sha256,request_json ON operations
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
CREATE TRIGGER immutable_operation_receipt BEFORE UPDATE ON operation_idempotency
 BEGIN SELECT RAISE(ABORT,'immutable control receipt'); END;
CREATE TRIGGER event_operation_owner BEFORE INSERT ON events
 WHEN NEW.operation_id!='' AND NOT EXISTS (
 SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id
 AND o.operation_id=NEW.operation_id AND o.job_id=NEW.job_id
 AND (o.attempt_id=NEW.attempt_id OR o.new_attempt_id=NEW.attempt_id))
 BEGIN SELECT RAISE(ABORT,'event control ownership mismatch'); END;
CREATE TRIGGER collection_owner_insert BEFORE INSERT ON collection_leases
 WHEN NOT EXISTS (SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id
 AND o.operation_id=NEW.operation_id AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id AND o.kind='collect')
 BEGIN SELECT RAISE(ABORT,'collection operation owner mismatch'); END;
CREATE TRIGGER collection_owner_update BEFORE UPDATE ON collection_leases
 WHEN NEW.workspace_id!=OLD.workspace_id OR NEW.job_id!=OLD.job_id OR NEW.attempt_id!=OLD.attempt_id
 OR NOT EXISTS (SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id
 AND o.operation_id=NEW.operation_id AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id AND o.kind='collect')
 BEGIN SELECT RAISE(ABORT,'collection operation owner mismatch'); END;
CREATE TRIGGER immutable_collection_snapshot BEFORE UPDATE ON collection_snapshots
 BEGIN SELECT RAISE(ABORT,'immutable collection snapshot'); END;
CREATE TRIGGER immutable_collection_publication BEFORE UPDATE ON collection_publications
 BEGIN SELECT RAISE(ABORT,'immutable collection publication'); END;
CREATE TRIGGER immutable_artifact BEFORE UPDATE ON artifacts
 BEGIN SELECT RAISE(ABORT,'immutable verified artifact'); END;
CREATE TRIGGER collection_fence_monotonic BEFORE UPDATE ON collection_leases
 WHEN NEW.generation<OLD.generation OR NEW.generation>OLD.generation+1
 OR (NEW.generation=OLD.generation AND (NEW.operation_id!=OLD.operation_id
 OR NEW.fence!=OLD.fence OR NEW.until_ms!=OLD.until_ms OR NEW.attempt_revision!=OLD.attempt_revision
 OR NEW.held!=0 OR OLD.held!=1))
 OR (NEW.generation=OLD.generation+1 AND (NEW.fence=OLD.fence OR NEW.held!=1))
 BEGIN SELECT RAISE(ABORT,'invalid collection fence transition'); END;
CREATE TRIGGER collection_publication_owner BEFORE INSERT ON collection_publications
 WHEN NOT EXISTS (SELECT 1 FROM collection_snapshots s WHERE s.workspace_id=NEW.workspace_id
 AND s.job_id=NEW.job_id AND s.attempt_id=NEW.attempt_id AND s.snapshot_sha256=NEW.snapshot_sha256)
 OR NOT EXISTS (SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id
 AND o.operation_id=NEW.operation_id AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id AND o.kind='collect')
 BEGIN SELECT RAISE(ABORT,'collection publication owner mismatch'); END;
CREATE TRIGGER retention_object_insert AFTER INSERT ON objects
 BEGIN INSERT INTO retention_inventory(kind,workspace_id,object_id,bytes,sha256,recorded_at)
 VALUES('input',NEW.workspace_id,NEW.object_id,NEW.bytes,NEW.sha256,strftime('%Y-%m-%dT%H:%M:%fZ','now')); END;
CREATE TRIGGER retention_artifact_insert AFTER INSERT ON artifacts
 BEGIN INSERT INTO retention_inventory(kind,workspace_id,object_id,bytes,sha256,recorded_at)
 VALUES('result',NEW.workspace_id,NEW.object_id,NEW.bytes,NEW.sha256,strftime('%Y-%m-%dT%H:%M:%fZ','now')); END;
CREATE TRIGGER retention_inventory_identity BEFORE UPDATE ON retention_inventory
 WHEN NEW.inventory_seq!=OLD.inventory_seq OR NEW.kind!=OLD.kind OR NEW.workspace_id!=OLD.workspace_id
 OR NEW.object_id!=OLD.object_id OR NEW.bytes!=OLD.bytes OR NEW.sha256!=OLD.sha256 OR NEW.recorded_at!=OLD.recorded_at
 OR (OLD.expired_at IS NOT NULL AND NEW.expired_at IS NOT OLD.expired_at)
 OR (OLD.deleted_at IS NOT NULL AND NEW.deleted_at IS NOT OLD.deleted_at)
 BEGIN SELECT RAISE(ABORT,'immutable retention identity or tombstone'); END;
CREATE TRIGGER retention_inventory_keep BEFORE DELETE ON retention_inventory
 BEGIN SELECT RAISE(ABORT,'retention tombstones cannot be forgotten'); END;
CREATE TRIGGER retention_object_immutable BEFORE UPDATE ON objects
 BEGIN SELECT RAISE(ABORT,'tracked object identity is immutable'); END;
CREATE TRIGGER retention_object_keep BEFORE DELETE ON objects
 BEGIN SELECT RAISE(ABORT,'object metadata survives byte expiry'); END;
CREATE TRIGGER retention_artifact_keep BEFORE DELETE ON artifacts
 BEGIN SELECT RAISE(ABORT,'artifact metadata survives byte expiry'); END;
CREATE TRIGGER retention_reference_guard BEFORE INSERT ON job_objects
 WHEN EXISTS(SELECT 1 FROM retention_inventory r WHERE r.kind='input'
 AND r.workspace_id=NEW.workspace_id AND r.object_id=NEW.object_id AND r.expired_at IS NOT NULL)
 BEGIN SELECT RAISE(ABORT,'input bytes expired'); END;
CREATE TRIGGER retention_retry_guard BEFORE INSERT ON attempts
 WHEN EXISTS(SELECT 1 FROM job_objects j JOIN retention_inventory r
 ON r.kind='input' AND r.workspace_id=j.workspace_id AND r.object_id=j.object_id
 WHERE j.workspace_id=NEW.workspace_id AND j.job_id=NEW.job_id AND r.expired_at IS NOT NULL)
 BEGIN SELECT RAISE(ABORT,'original retry inputs expired'); END;
CREATE TRIGGER retention_active_guard BEFORE UPDATE ON attempts
 WHEN NEW.orchestration NOT IN ('succeeded','failed','cancelled','timed_out')
 AND EXISTS(SELECT 1 FROM job_objects j JOIN retention_inventory r
 ON r.kind='input' AND r.workspace_id=j.workspace_id AND r.object_id=j.object_id
 WHERE j.workspace_id=NEW.workspace_id AND j.job_id=NEW.job_id AND r.expired_at IS NOT NULL)
 BEGIN SELECT RAISE(ABORT,'expired inputs cannot become active'); END;
CREATE TRIGGER retention_expiration_identity BEFORE UPDATE ON retention_expirations
 BEGIN SELECT RAISE(ABORT,'immutable result expiry'); END;
CREATE TRIGGER retention_expiration_keep BEFORE DELETE ON retention_expirations
 BEGIN SELECT RAISE(ABORT,'result expiry cannot be forgotten'); END;
CREATE TRIGGER retention_audit_immutable BEFORE UPDATE ON retention_audit
 BEGIN SELECT RAISE(ABORT,'immutable retention audit'); END;
CREATE TRIGGER retention_audit_keep BEFORE DELETE ON retention_audit
 BEGIN SELECT RAISE(ABORT,'retention audit cannot be forgotten'); END;
CREATE TRIGGER retention_root_immutable BEFORE UPDATE ON retention_roots
 BEGIN SELECT RAISE(ABORT,'retention root identity cannot be remapped'); END;
CREATE TRIGGER retention_root_keep BEFORE DELETE ON retention_roots
 BEGIN SELECT RAISE(ABORT,'retention root binding cannot be forgotten'); END;
CREATE TRIGGER managed_account_identity BEFORE UPDATE OF canonical_account,account_scope ON managed_connections
 WHEN OLD.canonical_account!='' AND (NEW.canonical_account!=OLD.canonical_account OR NEW.account_scope!=OLD.account_scope)
 BEGIN SELECT RAISE(ABORT,'managed account identity is immutable'); END;
CREATE TRIGGER managed_profile_repoint BEFORE UPDATE OF revision ON profiles
 WHEN OLD.revision!=NEW.revision AND EXISTS(SELECT 1 FROM managed_connections c WHERE c.connection_id=json_extract((SELECT snapshot FROM profile_revisions WHERE profile=OLD.profile AND revision=OLD.revision),'$.binding.ProviderInstanceID'))
 BEGIN SELECT RAISE(ABORT,'managed profile alias cannot be repointed'); END;
CREATE TRIGGER immutable_managed_profile_runtime BEFORE UPDATE ON managed_profile_runtime
 BEGIN SELECT RAISE(ABORT,'immutable managed runtime policy'); END;
CREATE TRIGGER immutable_execution_authorization BEFORE UPDATE OF workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_sha256,binding,account_scope,inputs_sha256,attempt_nonce,max_remote_wall_seconds,granting_token_id,created_at,original_receipt ON execution_authorizations BEGIN SELECT RAISE(ABORT,'immutable execution authorization'); END;
CREATE TRIGGER execution_authorization_one_shot BEFORE UPDATE OF status,consumed_at ON execution_authorizations WHEN OLD.status!='granted' OR NEW.status NOT IN ('consumed','revoked') BEGIN SELECT RAISE(ABORT,'execution authorization is one shot'); END;
CREATE TRIGGER collection_progress_owner_insert BEFORE INSERT ON collection_progress
 WHEN NOT EXISTS (SELECT 1 FROM collection_leases c WHERE c.workspace_id=NEW.workspace_id
 AND c.job_id=NEW.job_id AND c.attempt_id=NEW.attempt_id AND c.operation_id=NEW.operation_id
 AND c.generation=NEW.generation AND c.fence=NEW.fence AND c.held=1)
 BEGIN SELECT RAISE(ABORT,'collection progress owner mismatch'); END;
CREATE TRIGGER collection_progress_owner_update BEFORE UPDATE ON collection_progress
 WHEN NEW.workspace_id!=OLD.workspace_id OR NEW.job_id!=OLD.job_id OR NEW.attempt_id!=OLD.attempt_id
 OR NOT EXISTS (SELECT 1 FROM collection_leases c WHERE c.workspace_id=NEW.workspace_id
 AND c.job_id=NEW.job_id AND c.attempt_id=NEW.attempt_id AND c.operation_id=NEW.operation_id
 AND c.generation=NEW.generation AND c.fence=NEW.fence AND c.held=1)
 OR (NEW.generation=OLD.generation AND (NEW.operation_id!=OLD.operation_id OR NEW.fence!=OLD.fence
 OR json_extract(NEW.progress,'$.bytes_completed')<json_extract(OLD.progress,'$.bytes_completed')
 OR json_extract(NEW.progress,'$.bytes_received')<json_extract(OLD.progress,'$.bytes_received')))
 BEGIN SELECT RAISE(ABORT,'invalid collection progress transition'); END;
CREATE TRIGGER cleanup_resource_owner BEFORE INSERT ON cleanup_operations
 WHEN NOT EXISTS (SELECT 1 FROM provider_resources r WHERE r.resource_id=NEW.resource_id
 AND r.workspace_id=NEW.workspace_id AND r.job_id=NEW.job_id AND r.attempt_id=NEW.attempt_id
 AND r.purpose='staging' AND r.resource_ref!='')
 BEGIN SELECT RAISE(ABORT,'cleanup target is not an owned attempt resource'); END;
CREATE TRIGGER cleanup_operation_namespace BEFORE INSERT ON cleanup_operations
 WHEN EXISTS (SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id AND o.operation_id=NEW.operation_id)
 BEGIN SELECT RAISE(ABORT,'operation identifier already used'); END;
CREATE TRIGGER operation_cleanup_namespace BEFORE INSERT ON operations
 WHEN EXISTS (SELECT 1 FROM cleanup_operations c WHERE c.workspace_id=NEW.workspace_id AND c.operation_id=NEW.operation_id)
 BEGIN SELECT RAISE(ABORT,'operation identifier already used'); END;
CREATE TRIGGER cleanup_identity BEFORE UPDATE OF workspace_id,operation_id,job_id,attempt_id,resource_id,created_at,request_sha256,request_json ON cleanup_operations
 BEGIN SELECT RAISE(ABORT,'immutable cleanup identity'); END;
CREATE TRIGGER cleanup_terminal BEFORE UPDATE ON cleanup_operations
 WHEN OLD.status IN ('succeeded','failed')
 BEGIN SELECT RAISE(ABORT,'immutable terminal cleanup'); END;
CREATE TRIGGER cleanup_revision BEFORE UPDATE ON cleanup_operations
 WHEN NEW.revision!=OLD.revision+1 OR NEW.cleanup_seq!=OLD.cleanup_seq
 OR NOT ((OLD.status='accepted' AND NEW.status IN ('running','failed'))
 OR (OLD.status='running' AND NEW.status IN ('succeeded','failed')))
 BEGIN SELECT RAISE(ABORT,'invalid cleanup transition'); END;
CREATE TRIGGER cleanup_keep BEFORE DELETE ON cleanup_operations
 BEGIN SELECT RAISE(ABORT,'cleanup history cannot be forgotten'); END;
CREATE TRIGGER immutable_cleanup_receipt BEFORE UPDATE ON cleanup_idempotency
 BEGIN SELECT RAISE(ABORT,'immutable cleanup receipt'); END;
CREATE TRIGGER cleanup_receipt_keep BEFORE DELETE ON cleanup_idempotency
 BEGIN SELECT RAISE(ABORT,'cleanup receipts cannot be forgotten'); END;
CREATE TRIGGER cleanup_lease_insert BEFORE INSERT ON cleanup_leases
 WHEN NEW.generation!=1 OR NOT EXISTS (SELECT 1 FROM cleanup_operations c
 WHERE c.workspace_id=NEW.workspace_id AND c.operation_id=NEW.operation_id AND c.status='running')
 BEGIN SELECT RAISE(ABORT,'cleanup lease owner mismatch'); END;
CREATE TRIGGER cleanup_lease_update BEFORE UPDATE ON cleanup_leases
 WHEN NEW.workspace_id!=OLD.workspace_id OR NEW.operation_id!=OLD.operation_id
 OR NEW.generation!=OLD.generation+1 OR NEW.fence=OLD.fence
 OR NOT EXISTS (SELECT 1 FROM cleanup_operations c
 WHERE c.workspace_id=NEW.workspace_id AND c.operation_id=NEW.operation_id AND c.status='running')
 BEGIN SELECT RAISE(ABORT,'invalid cleanup fence transition'); END;
CREATE TRIGGER cleanup_lease_keep BEFORE DELETE ON cleanup_leases
 BEGIN SELECT RAISE(ABORT,'cleanup lease history cannot be forgotten'); END;
CREATE TRIGGER managed_operation_identity BEFORE UPDATE OF operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,fingerprint,credential_key,receipt,created_at ON managed_connection_operations
 BEGIN SELECT RAISE(ABORT,'immutable management operation identity'); END;
CREATE TRIGGER managed_installation_identity BEFORE UPDATE OF installation ON managed_connection_operations
 BEGIN SELECT RAISE(ABORT,'immutable image installation plan'); END;
CREATE TRIGGER immutable_preparation_authorization BEFORE UPDATE OF workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_sha256,request,original_receipt,binding,account_scope,inputs_sha256,attempt_nonce,reserved_bytes,ready_until_ms ON preparation_authorizations BEGIN SELECT RAISE(ABORT,'immutable preparation authorization'); END;
CREATE TRIGGER preparation_consume_once BEFORE UPDATE OF consumed_at ON preparation_authorizations WHEN OLD.consumed_at IS NOT NULL OR NEW.consumed_at IS NULL BEGIN SELECT RAISE(ABORT,'preparation authorization is one shot'); END;
CREATE TRIGGER preparation_ready_once BEFORE UPDATE OF ready_revision,ready_at ON preparation_authorizations WHEN OLD.ready_revision IS NOT NULL OR NEW.ready_revision IS NULL OR NEW.ready_at IS NULL BEGIN SELECT RAISE(ABORT,'readiness is immutable'); END;
CREATE TRIGGER preparation_release_once BEFORE UPDATE OF storage_released ON preparation_authorizations WHEN OLD.storage_released!=0 OR NEW.storage_released!=1 BEGIN SELECT RAISE(ABORT,'storage release is final'); END;
CREATE TRIGGER immutable_start_authorization BEFORE UPDATE OF workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_sha256,request,original_receipt ON start_authorizations BEGIN SELECT RAISE(ABORT,'immutable start authorization'); END;
CREATE TRIGGER start_consume_once BEFORE UPDATE OF consumed_at ON start_authorizations WHEN OLD.consumed_at IS NOT NULL OR NEW.consumed_at IS NULL BEGIN SELECT RAISE(ABORT,'start authorization is one shot'); END;
CREATE TRIGGER immutable_workload_secret_authorization BEFORE UPDATE OF workspace_id,authorization_id,job_id,attempt_id,key_sha256,request_hmac,metadata,original_receipt,expires_at_ms ON workload_secret_authorizations BEGIN SELECT RAISE(ABORT,'immutable workload secret authorization'); END;
CREATE TRIGGER workload_secret_consume_once BEFORE UPDATE OF consumed_at ON workload_secret_authorizations WHEN OLD.consumed_at IS NOT NULL OR NEW.consumed_at IS NULL OR OLD.revoked_at IS NOT NULL BEGIN SELECT RAISE(ABORT,'workload secret consumption is one shot'); END;
CREATE TRIGGER workload_secret_revoke_once BEFORE UPDATE OF revoked_at,revocation_key_sha256,revocation_receipt ON workload_secret_authorizations WHEN OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL OR OLD.consumed_at IS NOT NULL BEGIN SELECT RAISE(ABORT,'workload secret revocation is final'); END;
CREATE TRIGGER cleanup_resource_snapshot_owner BEFORE INSERT ON cleanup_resources
 WHEN NEW.state!='pending' OR NEW.reason!='awaiting_worker' OR NOT EXISTS (SELECT 1 FROM cleanup_operations c JOIN provider_resources r
 ON r.workspace_id=c.workspace_id AND r.job_id=c.job_id AND r.attempt_id=c.attempt_id
 WHERE c.workspace_id=NEW.workspace_id AND c.operation_id=NEW.operation_id AND r.resource_id=NEW.resource_id
 AND c.status IN ('accepted','running') AND r.resource_ref!='')
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
CREATE TRIGGER managed_resource_identity BEFORE UPDATE ON managed_resource_accounts
 WHEN NEW.account_scope!=OLD.account_scope OR NEW.resource_kind!=OLD.resource_kind OR
      (OLD.resource_scope!='' AND NEW.resource_scope!=OLD.resource_scope)
 BEGIN SELECT RAISE(ABORT,'verified scheduling resource identity is immutable'); END;
CREATE TRIGGER managed_resource_retained BEFORE DELETE ON managed_resource_accounts
 BEGIN SELECT RAISE(ABORT,'scheduling resource association is retained'); END;
CREATE TRIGGER scheduler_resource_reservation_identity BEFORE UPDATE ON scheduler_resource_reservations
 BEGIN SELECT RAISE(ABORT,'immutable attempt scheduling resource'); END;
