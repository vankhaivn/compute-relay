-- Explicit remote cleanup is a separate control family. Existing control, ledger and
-- receipt tables are untouched; cleanup rows only reference them.
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
-- One pending cleanup per attempt; distinct HTTP keys coalesce onto it.
CREATE UNIQUE INDEX cleanup_pending ON cleanup_operations(workspace_id,job_id,attempt_id) WHERE status IN ('accepted','running');
CREATE INDEX cleanup_work ON cleanup_operations(status,cleanup_seq);
-- The target is the exact staging ledger entry of the same attempt with a recorded reference.
CREATE TRIGGER cleanup_resource_owner BEFORE INSERT ON cleanup_operations
 WHEN NOT EXISTS (SELECT 1 FROM provider_resources r WHERE r.resource_id=NEW.resource_id
 AND r.workspace_id=NEW.workspace_id AND r.job_id=NEW.job_id AND r.attempt_id=NEW.attempt_id
 AND r.purpose='staging' AND r.resource_ref!='')
 BEGIN SELECT RAISE(ABORT,'cleanup target is not an owned attempt resource'); END;
-- Control and cleanup receipts share one operation read namespace.
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
CREATE TRIGGER immutable_cleanup_receipt BEFORE UPDATE ON cleanup_idempotency
 BEGIN SELECT RAISE(ABORT,'immutable cleanup receipt'); END;
CREATE TRIGGER cleanup_receipt_keep BEFORE DELETE ON cleanup_idempotency
 BEGIN SELECT RAISE(ABORT,'cleanup receipts cannot be forgotten'); END;

-- A worker owns one running cleanup through a fenced lease. Every claim advances the
-- generation and fence; expiry permits recovery, never a second concurrent owner.
CREATE TABLE cleanup_leases (
 workspace_id TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0),
 fence TEXT NOT NULL CHECK(length(fence)=64),
 until_ms INTEGER NOT NULL CHECK(until_ms>0),
 PRIMARY KEY(workspace_id,operation_id),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES cleanup_operations(workspace_id,operation_id)
) STRICT;
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
