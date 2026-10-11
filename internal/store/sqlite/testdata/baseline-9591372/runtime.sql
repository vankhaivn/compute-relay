PRAGMA application_id=1129466969;
PRAGMA user_version=14;
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
 CHECK(max_remote_wall_seconds IS NULL OR max_remote_wall_seconds BETWEEN 1 AND 86400)) STRICT;
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
CREATE TABLE "managed_connection_operations" (
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
CREATE TABLE preparation_progress (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0),
 progress TEXT NOT NULL CHECK(length(progress)<=1024 AND json_valid(progress)),
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
INSERT INTO "schema_migrations" VALUES(1,'0001_identity.sql','20d73957c03ded0f24ac743e23ed6975246f2a2aa50de8f2cd5706e999c56a1d','2026-10-11T10:18:02.256685Z');
INSERT INTO "schema_migrations" VALUES(2,'0002_workspace_objects.sql','23fa96b850befd5ca1fb8db6d9e8697a0322624476d66e65a13c29fbec7d44ae','2026-10-11T10:18:02.256857Z');
INSERT INTO "schema_migrations" VALUES(3,'0003_admission.sql','e20173c79349c7ced7b79c357e9cf3abedc78c295f0e340c48a44b719a59031d','2026-10-11T10:18:02.257308Z');
INSERT INTO "schema_migrations" VALUES(4,'0004_scheduler.sql','0f700afbb305c80df33c425651ffc5189651af7738233129cb3aa4637d4b1575','2026-10-11T10:18:02.257719Z');
INSERT INTO "schema_migrations" VALUES(5,'0005_dispatch.sql','6151f59b2b362e1b73543fffc0a8a1653d41284177cdf0f9b01b8093dfd37d54','2026-10-11T10:18:02.25806Z');
INSERT INTO "schema_migrations" VALUES(6,'0006_operations.sql','d2f23ab35dd7dde59e49aa2e60b42d524ac35f0f9fc4d68db9107c801a5876b7','2026-10-11T10:18:02.259487Z');
INSERT INTO "schema_migrations" VALUES(7,'0007_collection.sql','7035b159fe7947a6ee55056e48a2fb1c80b2e0bba54c97515d48bae261a533b1','2026-10-11T10:18:02.259928Z');
INSERT INTO "schema_migrations" VALUES(8,'0008_retention.sql','3d2c2a6678dca327a87d6779d35efd30db1275f68c060cbbb79fdcf8c995b6ad','2026-10-11T10:18:02.260576Z');
INSERT INTO "schema_migrations" VALUES(9,'0009_retention_roots.sql','4ea9f136b568646613467716ca0f67662e2f4f4bfc891ed3154c49c4b4b81e78','2026-10-11T10:18:02.260685Z');
INSERT INTO "schema_migrations" VALUES(10,'0010_managed_connections.sql','ebc2183edb541d214823ad2caff163c2fdfebbac6c931e5664cefd181f13dbc5','2026-10-11T10:18:02.261223Z');
INSERT INTO "schema_migrations" VALUES(11,'0011_execution_authorization.sql','56c7114ae536dd74a58fcf4f9238183c90ee8725c5aa70603327feb6133deed6','2026-10-11T10:18:02.261445Z');
INSERT INTO "schema_migrations" VALUES(12,'0012_collection_progress.sql','1856d9995c682f374c040307df13fbe2b911cda27d6dde40c5349b27772400df','2026-10-11T10:18:02.261645Z');
INSERT INTO "schema_migrations" VALUES(13,'0013_connection_configuration.sql','521a2da75f70fd51e47d3c17640c70261a35dcc81c2dc098a69cde618dd123e0','2026-10-11T10:18:02.264921Z');
INSERT INTO "schema_migrations" VALUES(14,'0014_preparation_progress.sql','cefc2b8c6f80a12c1e0104b90b0f1dd7d2b50b38d209ec0df1a81db16e415fe6','2026-10-11T10:18:02.265016Z');
INSERT INTO "runtime_installation" VALUES(1,'rt_74e8dd4f62aaf91926cb72939e758106','2026-10-11T10:18:02.265034Z');
INSERT INTO "workspaces" VALUES('a',1,'["fixture"]');
INSERT INTO "workspaces" VALUES('b',1,'["fixture"]');
INSERT INTO "api_token_hashes" VALUES('tok_2de43b132405822abbe7a8a666275151','a',X'8B42FF9BF5034ACD09FA873652BC53AF73F1B92B47E0688D83139397B3198A70','["execute","operate","read","write"]','2026-10-11T10:18:02.353496Z',NULL,0);
INSERT INTO "objects" VALUES('a','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "objects" VALUES('a','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "objects" VALUES('b','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "objects" VALUES('b','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "profile_revisions" VALUES('fixture','one','{"binding":{"Profile":"fixture","ProviderInstanceID":"fake_1","ConfigurationRevision":"one"},"account_scope":"shared_account","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}');
INSERT INTO "profile_revisions" VALUES('fixture','managed','{"binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"account_scope":"shared_account","credential_ref":"vault:con_fixture","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}');
INSERT INTO "profiles" VALUES('fixture','managed',1);
INSERT INTO "jobs" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"result_collection":"manual","timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}}','compute-relay/job-request/v1','51248eb2350c459fa1417d9b154fac7d06e227f1e006d6daf1f4d432525c732b','fixture','managed','att_585b0865d17faefd9cbfe595fa48d92c','2026-10-11T10:18:02.356305Z');
INSERT INTO "jobs" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"result_collection":"manual","timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}}','compute-relay/job-request/v1','51248eb2350c459fa1417d9b154fac7d06e227f1e006d6daf1f4d432525c732b','fixture','managed','att_5678e00c6c79e0e6532b2d637070e517','2026-10-11T10:18:02.370125Z');
INSERT INTO "jobs" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}}','compute-relay/job-request/v1','c516e672576037a05a7e5b2c18b6bd441803a549363fb75b6635514e07d9c245','fixture','managed','att_d7fe2350489b2e53495917c979b67000','2026-10-11T10:18:02.46838Z');
INSERT INTO "jobs" VALUES('a','job_d2af39fe387717f3f98bc53ce4c9b894','{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}}','compute-relay/job-request/v1','c516e672576037a05a7e5b2c18b6bd441803a549363fb75b6635514e07d9c245','fixture','managed','att_a7f95c8c3e5758857732d9f018cf86f1','2026-10-11T10:18:02.478176Z');
INSERT INTO "jobs" VALUES('a','job_5163a75c8a92e190527bcff3a82afd9d','{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}}','compute-relay/job-request/v1','c516e672576037a05a7e5b2c18b6bd441803a549363fb75b6635514e07d9c245','fixture','managed','att_9adb393ddb6bd27735629a75b301a242','2026-10-11T10:18:02.479089Z');
INSERT INTO "attempts" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','att_585b0865d17faefd9cbfe595fa48d92c',1,'2db4df6d073ac87cf775e19299359f5376fb86d2ec40ca61170aabc62030a3bb','{"Orchestration":"collecting","Execution":"succeeded","Result":"not_available","Cancellation":"not_requested","RemoteActivity":"inactive","ReleaseEvidence":"not_observable","DeadlineExceeded":false}','collecting',6,'2026-10-11T10:18:02.356305Z','2026-10-11T10:58:03.281Z');
INSERT INTO "attempts" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517',1,'a03ef318f0a7cca5132fe5ad4166e4cf0076ae6419eb4520b6616a26904e5ee7','{"Orchestration":"succeeded","Execution":"succeeded","Result":"available","Cancellation":"not_requested","RemoteActivity":"inactive","ReleaseEvidence":"not_observable","DeadlineExceeded":false}','succeeded',8,'2026-10-11T10:18:02.370125Z','2026-10-11T11:38:03.281Z');
INSERT INTO "attempts" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','att_d7fe2350489b2e53495917c979b67000',1,'91b68009e6e319b255af4319d493f3ab96222147afca5f3c792d38ae2c951a80','{"Orchestration":"running","Execution":"running","Result":"not_available","Cancellation":"not_requested","RemoteActivity":"active","ReleaseEvidence":"not_observable","DeadlineExceeded":false}','running',5,'2026-10-11T10:18:02.46838Z','2026-10-11T12:08:03.281Z');
INSERT INTO "attempts" VALUES('a','job_d2af39fe387717f3f98bc53ce4c9b894','att_a7f95c8c3e5758857732d9f018cf86f1',1,'5d5118ae612b82f277b37a0f1e76f9a47c47b3c315601670fbfc7411f1e76166','{"Orchestration":"queued","Execution":"not_submitted","Result":"not_available","Cancellation":"not_requested","RemoteActivity":"not_started","ReleaseEvidence":"unknown","DeadlineExceeded":false}','queued',1,'2026-10-11T10:18:02.478176Z','2026-10-11T10:18:02.478176Z');
INSERT INTO "attempts" VALUES('a','job_5163a75c8a92e190527bcff3a82afd9d','att_9adb393ddb6bd27735629a75b301a242',1,'11ae5d8c57fa3a44645f5459159a60e5311784f2999d45b5b24fc1267c1f0957','{"Orchestration":"queued","Execution":"not_submitted","Result":"not_available","Cancellation":"not_requested","RemoteActivity":"not_started","ReleaseEvidence":"unknown","DeadlineExceeded":false}','queued',1,'2026-10-11T10:18:02.479089Z','2026-10-11T10:18:02.479089Z');
INSERT INTO "job_objects" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','bundle','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "job_objects" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','input:0','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "job_objects" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','bundle','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "job_objects" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','input:0','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "job_objects" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','bundle','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "job_objects" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','input:0','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "job_objects" VALUES('a','job_d2af39fe387717f3f98bc53ce4c9b894','bundle','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "job_objects" VALUES('a','job_d2af39fe387717f3f98bc53ce4c9b894','input:0','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "job_objects" VALUES('a','job_5163a75c8a92e190527bcff3a82afd9d','bundle','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338');
INSERT INTO "job_objects" VALUES('a','job_5163a75c8a92e190527bcff3a82afd9d','input:0','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_be00bfb16e2259c8aa210af5006f44f5',1,'att_585b0865d17faefd9cbfe595fa48d92c','','job.accepted','2026-10-11T10:18:02.356305Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_1a1eb75b6f41546e18880e1bd4954f33',2,'att_585b0865d17faefd9cbfe595fa48d92c','','scheduler.claimed','2026-10-11T10:28:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_b2907a3eafde82ce07abc7485600c61a',3,'att_585b0865d17faefd9cbfe595fa48d92c','','inputs.ready','2026-10-11T10:28:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_c1ac486ee357d4eeb602b476365dabce',4,'att_585b0865d17faefd9cbfe595fa48d92c','','preparation.intent_recorded','2026-10-11T10:28:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_1a818a8eca7729fadb1f533d42a1dd8a',5,'att_585b0865d17faefd9cbfe595fa48d92c','','provider.staging.ready','2026-10-11T10:28:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_224fea3d048d1c6706427fac7295a12c',6,'att_585b0865d17faefd9cbfe595fa48d92c','','scheduler.released','2026-10-11T10:28:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_fe665f67f087a725d699851e0c2cb7f1',7,'att_585b0865d17faefd9cbfe595fa48d92c','','scheduler.claimed','2026-10-11T10:38:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_420e37eeba33cd278963c0bdefe7094c',8,'att_585b0865d17faefd9cbfe595fa48d92c','','submission.intent_recorded','2026-10-11T10:38:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_d1d4a96175cd9a4b624bb6199f77b4ba',9,'att_585b0865d17faefd9cbfe595fa48d92c','','submission.accepted','2026-10-11T10:38:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_e3ea70ce2041e3a5cdb3a58ed9e079ae',10,'att_585b0865d17faefd9cbfe595fa48d92c','','scheduler.released','2026-10-11T10:38:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_37f6b46cebc7eb2e24053b0b4c9faab8',11,'att_585b0865d17faefd9cbfe595fa48d92c','','scheduler.claimed','2026-10-11T10:48:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_8817aa709efda711775f7d5546c9df62',12,'att_585b0865d17faefd9cbfe595fa48d92c','','execution.observed','2026-10-11T10:48:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_57f1da82567e90088ca61cd14a558059',13,'att_585b0865d17faefd9cbfe595fa48d92c','','scheduler.released','2026-10-11T10:48:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_1cf44c86e859c4b23dec1563834c7870',14,'att_585b0865d17faefd9cbfe595fa48d92c','','scheduler.claimed','2026-10-11T10:58:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_7d548e6c502015cbf76adacaa9f95bf4',15,'att_585b0865d17faefd9cbfe595fa48d92c','','execution.observed','2026-10-11T10:58:03.281Z');
INSERT INTO "events" VALUES('a','job_132e9dcca9bd47ceb565c6b75ccd69a8','evt_992012f799efd1a4020e62a26f6d5183',16,'att_585b0865d17faefd9cbfe595fa48d92c','','scheduler.released','2026-10-11T10:58:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_d53cc368dab2b97abd294cdacfe5b1ab',1,'att_5678e00c6c79e0e6532b2d637070e517','','job.accepted','2026-10-11T10:18:02.370125Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_f8f9ee18a0bd8b25c20652a079dbc193',2,'att_5678e00c6c79e0e6532b2d637070e517','','scheduler.claimed','2026-10-11T11:08:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_93e67d9fdb2b64e25dcc21cb2107adf5',3,'att_5678e00c6c79e0e6532b2d637070e517','','inputs.ready','2026-10-11T11:08:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_d2bd970815d2247695f0abb467d115da',4,'att_5678e00c6c79e0e6532b2d637070e517','','preparation.intent_recorded','2026-10-11T11:08:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_4502a2cd93a42fca5c23ef7c6e0d61aa',5,'att_5678e00c6c79e0e6532b2d637070e517','','provider.staging.ready','2026-10-11T11:08:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_cd1dc52d516a37d349cbe23f8efd134f',6,'att_5678e00c6c79e0e6532b2d637070e517','','scheduler.released','2026-10-11T11:08:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_d2626336db0e24ec177a3aafee3382a4',7,'att_5678e00c6c79e0e6532b2d637070e517','','scheduler.claimed','2026-10-11T11:18:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_8739da46f5a76eb776965f9fbde440e0',8,'att_5678e00c6c79e0e6532b2d637070e517','','submission.intent_recorded','2026-10-11T11:18:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_4bb38d1181359e0210f62bfbf6cf4d8f',9,'att_5678e00c6c79e0e6532b2d637070e517','','submission.accepted','2026-10-11T11:18:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_ab6e073233eba730a7168ac42e84fccc',10,'att_5678e00c6c79e0e6532b2d637070e517','','scheduler.released','2026-10-11T11:18:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_81f8cf4b828314e8510376a86ff0d197',11,'att_5678e00c6c79e0e6532b2d637070e517','','scheduler.claimed','2026-10-11T11:28:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_3531fa99d5df9db71aef42eab7d7c69a',12,'att_5678e00c6c79e0e6532b2d637070e517','','execution.observed','2026-10-11T11:28:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_57b812d5f09857e2c59f295972f129c9',13,'att_5678e00c6c79e0e6532b2d637070e517','','scheduler.released','2026-10-11T11:28:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_618eba8aa2b803881c045a99d1f3d5c2',14,'att_5678e00c6c79e0e6532b2d637070e517','','scheduler.claimed','2026-10-11T11:38:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_686f339c61b962314e7135b0fb1f2007',15,'att_5678e00c6c79e0e6532b2d637070e517','','execution.observed','2026-10-11T11:38:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_1f366af9fea7ee33c05159e444a88284',16,'att_5678e00c6c79e0e6532b2d637070e517','','scheduler.released','2026-10-11T11:38:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_db4f2d55213b806de9726cc124a4a203',17,'att_5678e00c6c79e0e6532b2d637070e517','op_e1ee6a4403c68db69453c3531dd973b6','operation.accepted','2026-10-11T11:38:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_ac77867cadd7109de66f9c8fedafd412',18,'att_5678e00c6c79e0e6532b2d637070e517','op_e1ee6a4403c68db69453c3531dd973b6','collection.requested','2026-10-11T11:38:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_ff3ff8d37cee3760e08d152c2faab7d9',19,'att_5678e00c6c79e0e6532b2d637070e517','op_e1ee6a4403c68db69453c3531dd973b6','collection.requested','2026-10-11T11:38:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_590338c64540979fe3df9178b3883763',20,'att_5678e00c6c79e0e6532b2d637070e517','op_e1ee6a4403c68db69453c3531dd973b6','artifact.verified','2026-10-11T11:38:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_82fe608c969d532fa0c9a888809c21f1',21,'att_5678e00c6c79e0e6532b2d637070e517','op_e1ee6a4403c68db69453c3531dd973b6','job.completed','2026-10-11T11:38:03.281Z');
INSERT INTO "events" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','evt_aa7d6f80ea71222d20511ef65b64b98f',22,'att_5678e00c6c79e0e6532b2d637070e517','op_e1ee6a4403c68db69453c3531dd973b6','operation.completed','2026-10-11T11:38:03.281Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_816b956fd944b81fb3e3c2a32c5975f8',1,'att_d7fe2350489b2e53495917c979b67000','','job.accepted','2026-10-11T10:18:02.46838Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_d2ee5e79223679aa5e1dd63f371252e6',2,'att_d7fe2350489b2e53495917c979b67000','','scheduler.claimed','2026-10-11T11:48:03.281Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_dc4cd86e8c5593bd8c24ca3784b90158',3,'att_d7fe2350489b2e53495917c979b67000','','inputs.ready','2026-10-11T11:48:03.281Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_9b83c0a98b058a63075556c2fbe9d5ea',4,'att_d7fe2350489b2e53495917c979b67000','','preparation.intent_recorded','2026-10-11T11:48:03.281Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_87f28a1543345396e68949c777cd128c',5,'att_d7fe2350489b2e53495917c979b67000','','provider.staging.ready','2026-10-11T11:48:03.281Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_00027b3f01d0e47776bb8a438c52ebc7',6,'att_d7fe2350489b2e53495917c979b67000','','scheduler.released','2026-10-11T11:48:03.281Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_43072febaf1ee02e28d1217ecbd4f214',7,'att_d7fe2350489b2e53495917c979b67000','','scheduler.claimed','2026-10-11T11:58:03.281Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_0fc40e71460e4b9856dfc756708a3bfb',8,'att_d7fe2350489b2e53495917c979b67000','','submission.intent_recorded','2026-10-11T11:58:03.281Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_aaa2e0483a42fc03b0d2a828b3385695',9,'att_d7fe2350489b2e53495917c979b67000','','submission.accepted','2026-10-11T11:58:03.281Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_b1dbcfbabfccff8efb6b87891591e2df',10,'att_d7fe2350489b2e53495917c979b67000','','scheduler.released','2026-10-11T11:58:03.281Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_f2afd827e13f4efeae016d70c776f664',11,'att_d7fe2350489b2e53495917c979b67000','','scheduler.claimed','2026-10-11T12:08:03.281Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_ad04977fc0823a870811a433c3e78e20',12,'att_d7fe2350489b2e53495917c979b67000','','execution.observed','2026-10-11T12:08:03.281Z');
INSERT INTO "events" VALUES('a','job_7884ae4ec28b505245fa2b45708828ac','evt_fbd4eee244896e7a8e7bf5bdfa75a65a',13,'att_d7fe2350489b2e53495917c979b67000','','scheduler.released','2026-10-11T12:08:03.281Z');
INSERT INTO "events" VALUES('a','job_d2af39fe387717f3f98bc53ce4c9b894','evt_8e4bbc9294663b081806f169ead42d62',1,'att_a7f95c8c3e5758857732d9f018cf86f1','','job.accepted','2026-10-11T10:18:02.478176Z');
INSERT INTO "events" VALUES('a','job_5163a75c8a92e190527bcff3a82afd9d','evt_fbaef270822798e01b8d2e0daab64eb8',1,'att_9adb393ddb6bd27735629a75b301a242','','job.accepted','2026-10-11T10:18:02.479089Z');
INSERT INTO "idempotency" VALUES('a','job.create','1fef4edf1b2475e5a29f279a5dbe86c2d535d4c7be31414cee1bc57cc62f9ff9','51248eb2350c459fa1417d9b154fac7d06e227f1e006d6daf1f4d432525c732b','compute-relay/job-request/v1','job_132e9dcca9bd47ceb565c6b75ccd69a8','att_585b0865d17faefd9cbfe595fa48d92c','{"job_id":"job_132e9dcca9bd47ceb565c6b75ccd69a8","attempt_id":"att_585b0865d17faefd9cbfe595fa48d92c","status":"queued","created_at":"2026-10-11T10:18:02.356305Z","idempotency_replay":false,"links":{"self":"/v1/workspaces/a/jobs/job_132e9dcca9bd47ceb565c6b75ccd69a8","events":"/v1/workspaces/a/jobs/job_132e9dcca9bd47ceb565c6b75ccd69a8/events","attempts":"/v1/workspaces/a/jobs/job_132e9dcca9bd47ceb565c6b75ccd69a8/attempts","artifacts":"/v1/workspaces/a/jobs/job_132e9dcca9bd47ceb565c6b75ccd69a8/artifacts"}}');
INSERT INTO "idempotency" VALUES('a','job.create','782c401a4476ff2d60ce99b82e856067c191363c382f5dd47f7a80e142b4e420','51248eb2350c459fa1417d9b154fac7d06e227f1e006d6daf1f4d432525c732b','compute-relay/job-request/v1','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','{"job_id":"job_03c160d230d6f5d739e5bcde0246ed72","attempt_id":"att_5678e00c6c79e0e6532b2d637070e517","status":"queued","created_at":"2026-10-11T10:18:02.370125Z","idempotency_replay":false,"links":{"self":"/v1/workspaces/a/jobs/job_03c160d230d6f5d739e5bcde0246ed72","events":"/v1/workspaces/a/jobs/job_03c160d230d6f5d739e5bcde0246ed72/events","attempts":"/v1/workspaces/a/jobs/job_03c160d230d6f5d739e5bcde0246ed72/attempts","artifacts":"/v1/workspaces/a/jobs/job_03c160d230d6f5d739e5bcde0246ed72/artifacts"}}');
INSERT INTO "idempotency" VALUES('a','job.create','29d1667047ed802d85e84f9c813d355c41d61fd0a442da5eb736dba4ed4311dc','c516e672576037a05a7e5b2c18b6bd441803a549363fb75b6635514e07d9c245','compute-relay/job-request/v1','job_7884ae4ec28b505245fa2b45708828ac','att_d7fe2350489b2e53495917c979b67000','{"job_id":"job_7884ae4ec28b505245fa2b45708828ac","attempt_id":"att_d7fe2350489b2e53495917c979b67000","status":"queued","created_at":"2026-10-11T10:18:02.46838Z","idempotency_replay":false,"links":{"self":"/v1/workspaces/a/jobs/job_7884ae4ec28b505245fa2b45708828ac","events":"/v1/workspaces/a/jobs/job_7884ae4ec28b505245fa2b45708828ac/events","attempts":"/v1/workspaces/a/jobs/job_7884ae4ec28b505245fa2b45708828ac/attempts","artifacts":"/v1/workspaces/a/jobs/job_7884ae4ec28b505245fa2b45708828ac/artifacts"}}');
INSERT INTO "idempotency" VALUES('a','job.create','b21194ebbf3067cde6fe28afa6cf0730b7b025c4215aacb3c25417d73554c93e','c516e672576037a05a7e5b2c18b6bd441803a549363fb75b6635514e07d9c245','compute-relay/job-request/v1','job_d2af39fe387717f3f98bc53ce4c9b894','att_a7f95c8c3e5758857732d9f018cf86f1','{"job_id":"job_d2af39fe387717f3f98bc53ce4c9b894","attempt_id":"att_a7f95c8c3e5758857732d9f018cf86f1","status":"queued","created_at":"2026-10-11T10:18:02.478176Z","idempotency_replay":false,"links":{"self":"/v1/workspaces/a/jobs/job_d2af39fe387717f3f98bc53ce4c9b894","events":"/v1/workspaces/a/jobs/job_d2af39fe387717f3f98bc53ce4c9b894/events","attempts":"/v1/workspaces/a/jobs/job_d2af39fe387717f3f98bc53ce4c9b894/attempts","artifacts":"/v1/workspaces/a/jobs/job_d2af39fe387717f3f98bc53ce4c9b894/artifacts"}}');
INSERT INTO "idempotency" VALUES('a','job.create','8a3961f2163bbd092703531e37b2ed534439e2aff7ab09653d3d27e5b216f0e5','c516e672576037a05a7e5b2c18b6bd441803a549363fb75b6635514e07d9c245','compute-relay/job-request/v1','job_5163a75c8a92e190527bcff3a82afd9d','att_9adb393ddb6bd27735629a75b301a242','{"job_id":"job_5163a75c8a92e190527bcff3a82afd9d","attempt_id":"att_9adb393ddb6bd27735629a75b301a242","status":"queued","created_at":"2026-10-11T10:18:02.479089Z","idempotency_replay":false,"links":{"self":"/v1/workspaces/a/jobs/job_5163a75c8a92e190527bcff3a82afd9d","events":"/v1/workspaces/a/jobs/job_5163a75c8a92e190527bcff3a82afd9d/events","attempts":"/v1/workspaces/a/jobs/job_5163a75c8a92e190527bcff3a82afd9d/attempts","artifacts":"/v1/workspaces/a/jobs/job_5163a75c8a92e190527bcff3a82afd9d/artifacts"}}');
INSERT INTO "scheduler_control" VALUES(1,'{"paused":false,"max_workers":4,"max_active_per_account":1,"lease_duration_ns":30000000000,"quota_fresh_for_ns":300000000000,"blocked_recheck_ns":300000000000}','a',1791720483281);
INSERT INTO "scheduler_queue" VALUES(1,'a','job_132e9dcca9bd47ceb565c6b75ccd69a8','att_585b0865d17faefd9cbfe595fa48d92c',1,1791716302781);
INSERT INTO "scheduler_queue" VALUES(2,'a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517',1,1791718699781);
INSERT INTO "scheduler_queue" VALUES(3,'a','job_7884ae4ec28b505245fa2b45708828ac','att_d7fe2350489b2e53495917c979b67000',1,1791720498281);
INSERT INTO "scheduler_queue" VALUES(4,'a','job_d2af39fe387717f3f98bc53ce4c9b894','att_a7f95c8c3e5758857732d9f018cf86f1',0,0);
INSERT INTO "scheduler_queue" VALUES(5,'a','job_5163a75c8a92e190527bcff3a82afd9d','att_9adb393ddb6bd27735629a75b301a242',0,0);
INSERT INTO "scheduler_leases" VALUES(1,4,'','',0,0);
INSERT INTO "scheduler_leases" VALUES(2,4,'','',0,0);
INSERT INTO "scheduler_leases" VALUES(3,3,'','',0,0);
INSERT INTO "provider_resources" VALUES('res_staging_si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4','a','job_132e9dcca9bd47ceb565c6b75ccd69a8','att_585b0865d17faefd9cbfe595fa48d92c','con_fixture','shared_account','staging','prep_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4','cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e','{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_132e9dcca9bd47ceb565c6b75ccd69a8","AttemptID":"att_585b0865d17faefd9cbfe595fa48d92c","InstanceID":"con_fixture","IntentID":"si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4","ResourceKey":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Nonce":"2db4df6d073ac87cf775e19299359f5376fb86d2ec40ca61170aabc62030a3bb","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','26ebea7ca8a1165dda1c600c9159deeb0105ea6e478bba461c731b09658f5c17','cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e','ready','pinned','2026-10-11T10:28:03.281Z');
INSERT INTO "provider_resources" VALUES('res_execution_si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4','a','job_132e9dcca9bd47ceb565c6b75ccd69a8','att_585b0865d17faefd9cbfe595fa48d92c','con_fixture','shared_account','execution','si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4','cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e','{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_132e9dcca9bd47ceb565c6b75ccd69a8","AttemptID":"att_585b0865d17faefd9cbfe595fa48d92c","InstanceID":"con_fixture","IntentID":"si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4","ResourceKey":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Nonce":"2db4df6d073ac87cf775e19299359f5376fb86d2ec40ca61170aabc62030a3bb","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','26ebea7ca8a1165dda1c600c9159deeb0105ea6e478bba461c731b09658f5c17','{"resource":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","version":"fixture-1"}','ready','pinned','2026-10-11T10:38:03.281Z');
INSERT INTO "provider_resources" VALUES('res_staging_si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f','a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','con_fixture','shared_account','staging','prep_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f','cr-ddcded35614eb5fed84d884178bcf1809c689d98','{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_03c160d230d6f5d739e5bcde0246ed72","AttemptID":"att_5678e00c6c79e0e6532b2d637070e517","InstanceID":"con_fixture","IntentID":"si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f","ResourceKey":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Nonce":"a03ef318f0a7cca5132fe5ad4166e4cf0076ae6419eb4520b6616a26904e5ee7","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','21ffa444b8faeae58b4747ee07458af26e93c7152f9d136272bed96ec353334e','cr-ddcded35614eb5fed84d884178bcf1809c689d98','ready','pinned','2026-10-11T11:08:03.281Z');
INSERT INTO "provider_resources" VALUES('res_execution_si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f','a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','con_fixture','shared_account','execution','si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f','cr-ddcded35614eb5fed84d884178bcf1809c689d98','{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_03c160d230d6f5d739e5bcde0246ed72","AttemptID":"att_5678e00c6c79e0e6532b2d637070e517","InstanceID":"con_fixture","IntentID":"si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f","ResourceKey":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Nonce":"a03ef318f0a7cca5132fe5ad4166e4cf0076ae6419eb4520b6616a26904e5ee7","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','21ffa444b8faeae58b4747ee07458af26e93c7152f9d136272bed96ec353334e','{"resource":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","version":"fixture-1"}','ready','pinned','2026-10-11T11:18:03.281Z');
INSERT INTO "provider_resources" VALUES('res_staging_si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca','a','job_7884ae4ec28b505245fa2b45708828ac','att_d7fe2350489b2e53495917c979b67000','con_fixture','shared_account','staging','prep_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca','cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4','{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_7884ae4ec28b505245fa2b45708828ac","AttemptID":"att_d7fe2350489b2e53495917c979b67000","InstanceID":"con_fixture","IntentID":"si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca","ResourceKey":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Nonce":"91b68009e6e319b255af4319d493f3ab96222147afca5f3c792d38ae2c951a80","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','33c633b9b8a27460cac4740a3911f70c9d190e4394873fc0090261e6f0474534','cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4','ready','pinned','2026-10-11T11:48:03.281Z');
INSERT INTO "provider_resources" VALUES('res_execution_si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca','a','job_7884ae4ec28b505245fa2b45708828ac','att_d7fe2350489b2e53495917c979b67000','con_fixture','shared_account','execution','si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca','cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4','{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_7884ae4ec28b505245fa2b45708828ac","AttemptID":"att_d7fe2350489b2e53495917c979b67000","InstanceID":"con_fixture","IntentID":"si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca","ResourceKey":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Nonce":"91b68009e6e319b255af4319d493f3ab96222147afca5f3c792d38ae2c951a80","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','33c633b9b8a27460cac4740a3911f70c9d190e4394873fc0090261e6f0474534','{"resource":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","version":"fixture-1"}','ready','pinned','2026-10-11T11:58:03.281Z');
INSERT INTO "submission_intents" VALUES('si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4',1,'{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_132e9dcca9bd47ceb565c6b75ccd69a8","AttemptID":"att_585b0865d17faefd9cbfe595fa48d92c","InstanceID":"con_fixture","IntentID":"si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4","ResourceKey":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Nonce":"2db4df6d073ac87cf775e19299359f5376fb86d2ec40ca61170aabc62030a3bb","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_132e9dcca9bd47ceb565c6b75ccd69a8","AttemptID":"att_585b0865d17faefd9cbfe595fa48d92c","InstanceID":"con_fixture","IntentID":"si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4","ResourceKey":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Nonce":"2db4df6d073ac87cf775e19299359f5376fb86d2ec40ca61170aabc62030a3bb","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"PreparationID":"prep_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4","PlanSHA256":"26ebea7ca8a1165dda1c600c9159deeb0105ea6e478bba461c731b09658f5c17","Resource":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Ready":true,"Private":true}','accepted','{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_132e9dcca9bd47ceb565c6b75ccd69a8","AttemptID":"att_585b0865d17faefd9cbfe595fa48d92c","InstanceID":"con_fixture","IntentID":"si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4","ResourceKey":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Nonce":"2db4df6d073ac87cf775e19299359f5376fb86d2ec40ca61170aabc62030a3bb","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Version":"fixture-1"}','2026-10-11T10:38:03.281Z');
INSERT INTO "submission_intents" VALUES('si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f',2,'{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_03c160d230d6f5d739e5bcde0246ed72","AttemptID":"att_5678e00c6c79e0e6532b2d637070e517","InstanceID":"con_fixture","IntentID":"si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f","ResourceKey":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Nonce":"a03ef318f0a7cca5132fe5ad4166e4cf0076ae6419eb4520b6616a26904e5ee7","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_03c160d230d6f5d739e5bcde0246ed72","AttemptID":"att_5678e00c6c79e0e6532b2d637070e517","InstanceID":"con_fixture","IntentID":"si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f","ResourceKey":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Nonce":"a03ef318f0a7cca5132fe5ad4166e4cf0076ae6419eb4520b6616a26904e5ee7","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"PreparationID":"prep_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f","PlanSHA256":"21ffa444b8faeae58b4747ee07458af26e93c7152f9d136272bed96ec353334e","Resource":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Ready":true,"Private":true}','accepted','{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_03c160d230d6f5d739e5bcde0246ed72","AttemptID":"att_5678e00c6c79e0e6532b2d637070e517","InstanceID":"con_fixture","IntentID":"si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f","ResourceKey":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Nonce":"a03ef318f0a7cca5132fe5ad4166e4cf0076ae6419eb4520b6616a26904e5ee7","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Version":"fixture-1"}','2026-10-11T11:18:03.281Z');
INSERT INTO "submission_intents" VALUES('si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca',3,'{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_7884ae4ec28b505245fa2b45708828ac","AttemptID":"att_d7fe2350489b2e53495917c979b67000","InstanceID":"con_fixture","IntentID":"si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca","ResourceKey":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Nonce":"91b68009e6e319b255af4319d493f3ab96222147afca5f3c792d38ae2c951a80","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"}','{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_7884ae4ec28b505245fa2b45708828ac","AttemptID":"att_d7fe2350489b2e53495917c979b67000","InstanceID":"con_fixture","IntentID":"si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca","ResourceKey":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Nonce":"91b68009e6e319b255af4319d493f3ab96222147afca5f3c792d38ae2c951a80","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"PreparationID":"prep_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca","PlanSHA256":"33c633b9b8a27460cac4740a3911f70c9d190e4394873fc0090261e6f0474534","Resource":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Ready":true,"Private":true}','accepted','{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_7884ae4ec28b505245fa2b45708828ac","AttemptID":"att_d7fe2350489b2e53495917c979b67000","InstanceID":"con_fixture","IntentID":"si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca","ResourceKey":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Nonce":"91b68009e6e319b255af4319d493f3ab96222147afca5f3c792d38ae2c951a80","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Version":"fixture-1"}','2026-10-11T11:58:03.281Z');
INSERT INTO "dispatch_journals" VALUES(1,6,'collectible','{"version":6,"phase":"collectible","plan":{"Job":{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_132e9dcca9bd47ceb565c6b75ccd69a8","AttemptID":"att_585b0865d17faefd9cbfe595fa48d92c","InstanceID":"con_fixture","IntentID":"si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4","ResourceKey":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Nonce":"2db4df6d073ac87cf775e19299359f5376fb86d2ec40ca61170aabc62030a3bb","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"Specification":{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"result_collection":"manual","timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}},"SpecificationSHA256":"f8582f7adf3c1ef026959a65cd6a3c83640306318914d75cb924331af1931b9b","Required":["batch_execution","execution_timeout","private_input_staging","python","remote_network_control","strong_execution_identity"],"WallSeconds":10,"Inputs":{"bundle":{"object_id":"code","workspace_id":"a","bytes":327,"sha256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338"},"inputs":[{"name":"data","target":"data.txt","object":{"object_id":"data","workspace_id":"a","bytes":12,"sha256":"23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040"}}]}},"VerifyAfterStart":null},"preparation_id":"prep_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4","prepared":{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_132e9dcca9bd47ceb565c6b75ccd69a8","AttemptID":"att_585b0865d17faefd9cbfe595fa48d92c","InstanceID":"con_fixture","IntentID":"si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4","ResourceKey":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Nonce":"2db4df6d073ac87cf775e19299359f5376fb86d2ec40ca61170aabc62030a3bb","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"PreparationID":"prep_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4","PlanSHA256":"26ebea7ca8a1165dda1c600c9159deeb0105ea6e478bba461c731b09658f5c17","Resource":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Ready":true,"Private":true},"submit_started":true,"remote":{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_132e9dcca9bd47ceb565c6b75ccd69a8","AttemptID":"att_585b0865d17faefd9cbfe595fa48d92c","InstanceID":"con_fixture","IntentID":"si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4","ResourceKey":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Nonce":"2db4df6d073ac87cf775e19299359f5376fb86d2ec40ca61170aabc62030a3bb","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Version":"fixture-1"},"observation":{"Remote":{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_132e9dcca9bd47ceb565c6b75ccd69a8","AttemptID":"att_585b0865d17faefd9cbfe595fa48d92c","InstanceID":"con_fixture","IntentID":"si_8e80e51ae97ee99c75dc13eed2d557fef05c253eb1e37513b2f8845e27cd51c4","ResourceKey":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Nonce":"2db4df6d073ac87cf775e19299359f5376fb86d2ec40ca61170aabc62030a3bb","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-8e80e51ae97ee99c75dc13eed2d557fef05c253e","Version":"fixture-1"},"Execution":"succeeded","RawState":"succeeded","RemoteActivity":"inactive","ReleaseEvidence":"not_observable","ObservedAt":"2026-10-11T10:58:03.281Z"},"failures":0}');
INSERT INTO "dispatch_journals" VALUES(2,7,'collectible','{"version":7,"phase":"collectible","plan":{"Job":{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_03c160d230d6f5d739e5bcde0246ed72","AttemptID":"att_5678e00c6c79e0e6532b2d637070e517","InstanceID":"con_fixture","IntentID":"si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f","ResourceKey":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Nonce":"a03ef318f0a7cca5132fe5ad4166e4cf0076ae6419eb4520b6616a26904e5ee7","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"Specification":{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"result_collection":"manual","timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}},"SpecificationSHA256":"f8582f7adf3c1ef026959a65cd6a3c83640306318914d75cb924331af1931b9b","Required":["batch_execution","execution_timeout","private_input_staging","python","remote_network_control","strong_execution_identity"],"WallSeconds":10,"Inputs":{"bundle":{"object_id":"code","workspace_id":"a","bytes":327,"sha256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338"},"inputs":[{"name":"data","target":"data.txt","object":{"object_id":"data","workspace_id":"a","bytes":12,"sha256":"23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040"}}]}},"VerifyAfterStart":null},"preparation_id":"prep_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f","prepared":{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_03c160d230d6f5d739e5bcde0246ed72","AttemptID":"att_5678e00c6c79e0e6532b2d637070e517","InstanceID":"con_fixture","IntentID":"si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f","ResourceKey":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Nonce":"a03ef318f0a7cca5132fe5ad4166e4cf0076ae6419eb4520b6616a26904e5ee7","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"PreparationID":"prep_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f","PlanSHA256":"21ffa444b8faeae58b4747ee07458af26e93c7152f9d136272bed96ec353334e","Resource":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Ready":true,"Private":true},"submit_started":true,"remote":{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_03c160d230d6f5d739e5bcde0246ed72","AttemptID":"att_5678e00c6c79e0e6532b2d637070e517","InstanceID":"con_fixture","IntentID":"si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f","ResourceKey":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Nonce":"a03ef318f0a7cca5132fe5ad4166e4cf0076ae6419eb4520b6616a26904e5ee7","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Version":"fixture-1"},"observation":{"Remote":{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_03c160d230d6f5d739e5bcde0246ed72","AttemptID":"att_5678e00c6c79e0e6532b2d637070e517","InstanceID":"con_fixture","IntentID":"si_ddcded35614eb5fed84d884178bcf1809c689d98b9b3c7b10d07fbcdb7b3577f","ResourceKey":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Nonce":"a03ef318f0a7cca5132fe5ad4166e4cf0076ae6419eb4520b6616a26904e5ee7","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-ddcded35614eb5fed84d884178bcf1809c689d98","Version":"fixture-1"},"Execution":"succeeded","RawState":"succeeded","RemoteActivity":"inactive","ReleaseEvidence":"not_observable","ObservedAt":"2026-10-11T11:38:03.281Z"},"failures":0}');
INSERT INTO "dispatch_journals" VALUES(3,5,'submitted','{"version":5,"phase":"submitted","plan":{"Job":{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_7884ae4ec28b505245fa2b45708828ac","AttemptID":"att_d7fe2350489b2e53495917c979b67000","InstanceID":"con_fixture","IntentID":"si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca","ResourceKey":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Nonce":"91b68009e6e319b255af4319d493f3ab96222147afca5f3c792d38ae2c951a80","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"Specification":{"api_version":"compute-connector/v1alpha1","bundle":{"object_id":"code"},"execution":{"command":["python","never-executed.py"],"kind":"python"},"inputs":[{"name":"data","source":{"kind":"object","object_id":"data"},"target":"data.txt"}],"name":"dispatch-fixture","network":{"remote_internet":"disabled"},"outputs":[{"path":"answer.json","required":true}],"profile":"fixture","resources":{"accelerator":"cpu"},"timeouts":{"finalization_grace_seconds":2,"remote_wall_seconds":10,"setup_seconds":2}},"SpecificationSHA256":"1ccf6ccab4061e2d40bca9f611c1281f37592b7ec822c1b7e1256f89cb4c421c","Required":["batch_execution","execution_timeout","private_input_staging","python","remote_network_control","strong_execution_identity"],"WallSeconds":10,"Inputs":{"bundle":{"object_id":"code","workspace_id":"a","bytes":327,"sha256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338"},"inputs":[{"name":"data","target":"data.txt","object":{"object_id":"data","workspace_id":"a","bytes":12,"sha256":"23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040"}}]}},"VerifyAfterStart":null},"preparation_id":"prep_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca","prepared":{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_7884ae4ec28b505245fa2b45708828ac","AttemptID":"att_d7fe2350489b2e53495917c979b67000","InstanceID":"con_fixture","IntentID":"si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca","ResourceKey":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Nonce":"91b68009e6e319b255af4319d493f3ab96222147afca5f3c792d38ae2c951a80","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"PreparationID":"prep_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca","PlanSHA256":"33c633b9b8a27460cac4740a3911f70c9d190e4394873fc0090261e6f0474534","Resource":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Ready":true,"Private":true},"submit_started":true,"remote":{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_7884ae4ec28b505245fa2b45708828ac","AttemptID":"att_d7fe2350489b2e53495917c979b67000","InstanceID":"con_fixture","IntentID":"si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca","ResourceKey":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Nonce":"91b68009e6e319b255af4319d493f3ab96222147afca5f3c792d38ae2c951a80","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Version":"fixture-1"},"observation":{"Remote":{"Identity":{"InstallationID":"rt_74e8dd4f62aaf91926cb72939e758106","WorkspaceID":"a","JobID":"job_7884ae4ec28b505245fa2b45708828ac","AttemptID":"att_d7fe2350489b2e53495917c979b67000","InstanceID":"con_fixture","IntentID":"si_5225c4189a1a96aa23b23c1c7425c9c1b0245ad44ce3de34e50f7993b36704ca","ResourceKey":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Nonce":"91b68009e6e319b255af4319d493f3ab96222147afca5f3c792d38ae2c951a80","BundleSHA256":"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338","InputManifestSHA256":"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592"},"Resource":"cr-5225c4189a1a96aa23b23c1c7425c9c1b0245ad4","Version":"fixture-1"},"Execution":"running","RawState":"running","RemoteActivity":"active","ReleaseEvidence":"not_observable","ObservedAt":"2026-10-11T12:08:03.281Z"},"failures":0}');
INSERT INTO "operations" VALUES(1,'a','op_e1ee6a4403c68db69453c3531dd973b6','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','collect','succeeded',2,'2026-10-11T11:38:03.281Z','a7ef4b9010cd1032dca8c7b9a720222c63de5aceb635fb1a827076ecad1f240d','{"attempt_id":"att_5678e00c6c79e0e6532b2d637070e517","reason":"explicit operator request"}','{"Operation":{"ID":"op_e1ee6a4403c68db69453c3531dd973b6","WorkspaceID":"a","JobID":"job_03c160d230d6f5d739e5bcde0246ed72","AttemptID":"att_5678e00c6c79e0e6532b2d637070e517","Kind":"collect","Status":"succeeded","Revision":2,"Failure":null,"CreatedAt":"2026-10-11T11:38:03.281Z","UpdatedAt":"2026-10-11T11:38:03.281Z"},"Effect":"results_available","NewAttemptID":"","TerminationConfirmed":false,"Replay":false}',NULL,0);
INSERT INTO "operation_idempotency" VALUES('a','collect','cecddcbb0af060ef17ed3c720c7487313ca84c2661d6eec7c9abe7646e036ffc','a7ef4b9010cd1032dca8c7b9a720222c63de5aceb635fb1a827076ecad1f240d','control/v1','op_e1ee6a4403c68db69453c3531dd973b6','{"Operation":{"ID":"op_e1ee6a4403c68db69453c3531dd973b6","WorkspaceID":"a","JobID":"job_03c160d230d6f5d739e5bcde0246ed72","AttemptID":"att_5678e00c6c79e0e6532b2d637070e517","Kind":"collect","Status":"accepted","Revision":1,"Failure":null,"CreatedAt":"2026-10-11T11:38:03.281Z","UpdatedAt":"2026-10-11T11:38:03.281Z"},"Effect":"collection_requested","NewAttemptID":"","TerminationConfirmed":false,"Replay":false}');
INSERT INTO "collection_leases" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','op_e1ee6a4403c68db69453c3531dd973b6',1,'5a6a3a893d330026743246671cacfdb4d442b8d7b8b8a776853fc514e889b01b',1791719313281,7,0);
INSERT INTO "collection_snapshots" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','c2309c54414f65446ec01bbd9cde7f8554172627f54fa70451f147c85e0f97de','{"manifest":"{\"artifacts\":[{\"bytes\":57344,\"path\":\"answer.json\",\"sha256\":\"4ddfc63fe431bec0b2e3b39fcf81babbacce5b66bfe1a3f8170659b533e53373\"}],\"attempt_id\":\"att_5678e00c6c79e0e6532b2d637070e517\",\"attempt_nonce\":\"a03ef318f0a7cca5132fe5ad4166e4cf0076ae6419eb4520b6616a26904e5ee7\",\"bundle_sha256\":\"59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338\",\"error\":null,\"exit_code\":0,\"finished_at\":\"2026-10-11T11:38:03Z\",\"input_manifest_sha256\":\"5aee84ceeadc833755132cfc380c36a8540c80b95a0757b568f65c60bc57a592\",\"job_id\":\"job_03c160d230d6f5d739e5bcde0246ed72\",\"manifest_version\":\"1\",\"phase\":\"completed\",\"resource_check\":{\"gpu_required\":false,\"gpu_verified\":false},\"runner_version\":\"offline-fixture\",\"started_at\":\"2026-10-11T11:38:02Z\",\"timed_out\":false}","files":[{"artifact_id":"art_70922307a5ac0057949415b032c002897e40598a3d615f01964a8fada476c1f9","path":"control/environment.json","role":"provenance","object":{"object_id":"art_70922307a5ac0057949415b032c002897e40598a3d615f01964a8fada476c1f9","workspace_id":"a","bytes":16,"sha256":"ffbc2dfc402782325da71132100e74ff511d1585dd80e4ea196ed4bcace3fef2"},"media_type":"application/json"},{"artifact_id":"art_328d34b42d8918a3e59abdc139605df7f1497111e39714d496e53f91b5b1754d","path":"control/execution-result.json","role":"manifest","object":{"object_id":"art_328d34b42d8918a3e59abdc139605df7f1497111e39714d496e53f91b5b1754d","workspace_id":"a","bytes":742,"sha256":"8b292d154d0eb139b336c4138a4838b23aa5f7c410d9110a536341ada6e27a5b"},"media_type":"application/json"},{"artifact_id":"art_ad1ee8ce74bd6dc77f58ab26053e14f9f44f183d5144222b3cb21a12bfba4738","path":"control/stderr.log","role":"log","object":{"object_id":"art_ad1ee8ce74bd6dc77f58ab26053e14f9f44f183d5144222b3cb21a12bfba4738","workspace_id":"a","bytes":0,"sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},"media_type":"text/plain"},{"artifact_id":"art_d16e936d2229cf35193a44ce5de81ca493fa9e2b28599342b9e166e86223afa1","path":"control/stdout.log","role":"log","object":{"object_id":"art_d16e936d2229cf35193a44ce5de81ca493fa9e2b28599342b9e166e86223afa1","workspace_id":"a","bytes":15,"sha256":"4d3ac42b53378ed2def23b3bf88787f9fffed17b22bd42ce971d7067dc591dab"},"media_type":"text/plain"},{"artifact_id":"art_9af08d079705940bc32e4ae48076561f2ae85db3f23e1bee8186d5688c6eafaf","path":"outputs/answer.json","role":"output","object":{"object_id":"art_9af08d079705940bc32e4ae48076561f2ae85db3f23e1bee8186d5688c6eafaf","workspace_id":"a","bytes":57344,"sha256":"4ddfc63fe431bec0b2e3b39fcf81babbacce5b66bfe1a3f8170659b533e53373"}}]}','2026-10-11T11:38:03.281Z');
INSERT INTO "collection_publications" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','op_e1ee6a4403c68db69453c3531dd973b6','c2309c54414f65446ec01bbd9cde7f8554172627f54fa70451f147c85e0f97de','completed',5,'2026-10-11T11:38:03.281Z');
INSERT INTO "artifacts" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','art_70922307a5ac0057949415b032c002897e40598a3d615f01964a8fada476c1f9','art_70922307a5ac0057949415b032c002897e40598a3d615f01964a8fada476c1f9','control/environment.json','provenance',16,'ffbc2dfc402782325da71132100e74ff511d1585dd80e4ea196ed4bcace3fef2','application/json');
INSERT INTO "artifacts" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','art_328d34b42d8918a3e59abdc139605df7f1497111e39714d496e53f91b5b1754d','art_328d34b42d8918a3e59abdc139605df7f1497111e39714d496e53f91b5b1754d','control/execution-result.json','manifest',742,'8b292d154d0eb139b336c4138a4838b23aa5f7c410d9110a536341ada6e27a5b','application/json');
INSERT INTO "artifacts" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','art_ad1ee8ce74bd6dc77f58ab26053e14f9f44f183d5144222b3cb21a12bfba4738','art_ad1ee8ce74bd6dc77f58ab26053e14f9f44f183d5144222b3cb21a12bfba4738','control/stderr.log','log',0,'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855','text/plain');
INSERT INTO "artifacts" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','art_d16e936d2229cf35193a44ce5de81ca493fa9e2b28599342b9e166e86223afa1','art_d16e936d2229cf35193a44ce5de81ca493fa9e2b28599342b9e166e86223afa1','control/stdout.log','log',15,'4d3ac42b53378ed2def23b3bf88787f9fffed17b22bd42ce971d7067dc591dab','text/plain');
INSERT INTO "artifacts" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','art_9af08d079705940bc32e4ae48076561f2ae85db3f23e1bee8186d5688c6eafaf','art_9af08d079705940bc32e4ae48076561f2ae85db3f23e1bee8186d5688c6eafaf','outputs/answer.json','output',57344,'4ddfc63fe431bec0b2e3b39fcf81babbacce5b66bfe1a3f8170659b533e53373','');
INSERT INTO "retention_inventory" VALUES(1,'input','a','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338','2026-10-11T10:18:02.305Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(2,'input','a','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040','2026-10-11T10:18:02.320Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(3,'input','b','code',327,'59cb9b7bf8c1a6457df2830f479c82c8161f338ebad2c56b4e0e61d853428338','2026-10-11T10:18:02.335Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(4,'input','b','data',12,'23b57499cad5e8dd735d7260115bacbd89e8e0fcdb5d548c966f098c707e9040','2026-10-11T10:18:02.352Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(5,'result','a','art_70922307a5ac0057949415b032c002897e40598a3d615f01964a8fada476c1f9',16,'ffbc2dfc402782325da71132100e74ff511d1585dd80e4ea196ed4bcace3fef2','2026-10-11T10:18:02.467Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(6,'result','a','art_328d34b42d8918a3e59abdc139605df7f1497111e39714d496e53f91b5b1754d',742,'8b292d154d0eb139b336c4138a4838b23aa5f7c410d9110a536341ada6e27a5b','2026-10-11T10:18:02.467Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(7,'result','a','art_ad1ee8ce74bd6dc77f58ab26053e14f9f44f183d5144222b3cb21a12bfba4738',0,'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855','2026-10-11T10:18:02.467Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(8,'result','a','art_d16e936d2229cf35193a44ce5de81ca493fa9e2b28599342b9e166e86223afa1',15,'4d3ac42b53378ed2def23b3bf88787f9fffed17b22bd42ce971d7067dc591dab','2026-10-11T10:18:02.467Z',NULL,NULL);
INSERT INTO "retention_inventory" VALUES(9,'result','a','art_9af08d079705940bc32e4ae48076561f2ae85db3f23e1bee8186d5688c6eafaf',57344,'4ddfc63fe431bec0b2e3b39fcf81babbacce5b66bfe1a3f8170659b533e53373','2026-10-11T10:18:02.467Z',NULL,NULL);
INSERT INTO "retention_cursor" VALUES(1,0);
INSERT INTO "managed_config" VALUES(1,0);
INSERT INTO "managed_connections" VALUES('con_fixture','a','fixture','fixture',1,'verified','enabled','fixture_account','shared_account','fixture_key','fixture','','2026-10-11T10:18:03.281Z',NULL);
INSERT INTO "execution_authorizations" VALUES('a','grant_e90d7d53ea0d26bee27daafd50bb0e67','job_132e9dcca9bd47ceb565c6b75ccd69a8','att_585b0865d17faefd9cbfe595fa48d92c','ef9dc7e361ea257a3397332127f95a31b364be4a4a924c9cc1d60933df80e1ec','8c60f2bf1bc76a9259aea5846b5f2c9578d759541493d0d2d86c2a3a11b128e8','{"binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"account_scope":"shared_account","credential_ref":"vault:con_fixture","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}','shared_account','5da4ce3965f702c51427a1c6cb497e5307e8620c7ffb31946d4c643edf41a497','2db4df6d073ac87cf775e19299359f5376fb86d2ec40ca61170aabc62030a3bb',10,'tok_2de43b132405822abbe7a8a666275151','2026-10-11T10:18:03.281Z','{"authorization_id":"grant_e90d7d53ea0d26bee27daafd50bb0e67","workspace_id":"a","job_id":"job_132e9dcca9bd47ceb565c6b75ccd69a8","attempt_id":"att_585b0865d17faefd9cbfe595fa48d92c","status":"granted","max_remote_wall_seconds":10,"created_at":"2026-10-11T10:18:03.281Z","consumed_at":null,"replay":false}','consumed','2026-10-11T10:28:03.281Z');
INSERT INTO "execution_authorizations" VALUES('a','grant_f974e549ac2cf966c075cce40251f23c','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','0a2727f2b07e6f35a37f0168badd6b88c0d28b09e8ceb475d39b7fdc3f0b4e48','6966bc832cf88c2c0322784e17b424a27102a303b2b5fe3bb33f1cc251fb5ab0','{"binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"account_scope":"shared_account","credential_ref":"vault:con_fixture","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}','shared_account','5da4ce3965f702c51427a1c6cb497e5307e8620c7ffb31946d4c643edf41a497','a03ef318f0a7cca5132fe5ad4166e4cf0076ae6419eb4520b6616a26904e5ee7',10,'tok_2de43b132405822abbe7a8a666275151','2026-10-11T10:58:03.281Z','{"authorization_id":"grant_f974e549ac2cf966c075cce40251f23c","workspace_id":"a","job_id":"job_03c160d230d6f5d739e5bcde0246ed72","attempt_id":"att_5678e00c6c79e0e6532b2d637070e517","status":"granted","max_remote_wall_seconds":10,"created_at":"2026-10-11T10:58:03.281Z","consumed_at":null,"replay":false}','consumed','2026-10-11T11:08:03.281Z');
INSERT INTO "execution_authorizations" VALUES('a','grant_7b6cfa8bc6a0b45207db50809e537d87','job_7884ae4ec28b505245fa2b45708828ac','att_d7fe2350489b2e53495917c979b67000','fbeb3905b22132c34fa98644cee6b50540f94d030a5778a01b0dc3674c2e6760','d56867bce2f66eff5384ca36b12e8e4246d9b7d6e5bf0e28e39af1656c2041f4','{"binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"account_scope":"shared_account","credential_ref":"vault:con_fixture","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}','shared_account','91c1b6b4447c801db007e012937ecd8dfa7f3320e52eb5bcf15d19b8e0124a92','91b68009e6e319b255af4319d493f3ab96222147afca5f3c792d38ae2c951a80',10,'tok_2de43b132405822abbe7a8a666275151','2026-10-11T11:38:03.281Z','{"authorization_id":"grant_7b6cfa8bc6a0b45207db50809e537d87","workspace_id":"a","job_id":"job_7884ae4ec28b505245fa2b45708828ac","attempt_id":"att_d7fe2350489b2e53495917c979b67000","status":"granted","max_remote_wall_seconds":10,"created_at":"2026-10-11T11:38:03.281Z","consumed_at":null,"replay":false}','consumed','2026-10-11T11:48:03.281Z');
INSERT INTO "execution_authorizations" VALUES('a','grant_ba146ad33a44a762d361f1b61be6a3e3','job_d2af39fe387717f3f98bc53ce4c9b894','att_a7f95c8c3e5758857732d9f018cf86f1','43f6d503f6b5ffd94f4f93ac8a2390a4f2cc549acc7cfb13e255bb786b3ca758','8d87d60b5e8b390be05e02f5835d033997789ea08607f7896362c51e86cb2530','{"binding":{"Profile":"fixture","ProviderInstanceID":"con_fixture","ConfigurationRevision":"managed"},"account_scope":"shared_account","credential_ref":"vault:con_fixture","cost_class":"free_allowance","allow_remote_internet":false,"max_remote_wall_seconds":1800,"max_bundle_bytes":104857600,"max_input_bytes":4294967296}','shared_account','91c1b6b4447c801db007e012937ecd8dfa7f3320e52eb5bcf15d19b8e0124a92','5d5118ae612b82f277b37a0f1e76f9a47c47b3c315601670fbfc7411f1e76166',10,'tok_2de43b132405822abbe7a8a666275151','2026-10-11T12:08:03.281Z','{"authorization_id":"grant_ba146ad33a44a762d361f1b61be6a3e3","workspace_id":"a","job_id":"job_d2af39fe387717f3f98bc53ce4c9b894","attempt_id":"att_a7f95c8c3e5758857732d9f018cf86f1","status":"granted","max_remote_wall_seconds":10,"created_at":"2026-10-11T12:08:03.281Z","consumed_at":null,"replay":false}','granted',NULL);
INSERT INTO "collection_progress" VALUES('a','job_03c160d230d6f5d739e5bcde0246ed72','att_5678e00c6c79e0e6532b2d637070e517','op_e1ee6a4403c68db69453c3531dd973b6',1,'5a6a3a893d330026743246671cacfdb4d442b8d7b8b8a776853fc514e889b01b','available','{"scope":"selected_output_bytes","generation":1,"bytes_completed":57344,"bytes_received":null,"bytes_total":57344,"observed_at":"2026-10-11T11:38:03.281Z"}');
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
CREATE TRIGGER managed_operation_identity BEFORE UPDATE OF operation_id,workspace_id,connection_id,action,connection_revision,token_id,key_sha256,fingerprint,credential_key,receipt,created_at ON managed_connection_operations
 BEGIN SELECT RAISE(ABORT,'immutable management operation identity'); END;
