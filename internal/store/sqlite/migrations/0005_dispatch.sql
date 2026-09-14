-- Each attempt gets one bounded journal. Its one-shot gates are never reset by recovery.
CREATE TABLE dispatch_journals (
 queue_seq INTEGER PRIMARY KEY REFERENCES scheduler_queue(queue_seq),
 version INTEGER NOT NULL CHECK(version>=0),
 phase TEXT NOT NULL CHECK(phase IN ('local','staging','ready','submitting','submitted','rejected','collectible','attention','failed')),
 journal TEXT NOT NULL CHECK(length(journal)<=2097152),
 CHECK(json_extract(journal,'$.version')=version),
 CHECK(json_extract(journal,'$.phase')=phase)
) STRICT;
CREATE TRIGGER dispatch_version BEFORE UPDATE ON dispatch_journals
 WHEN NEW.queue_seq!=OLD.queue_seq OR NEW.version!=OLD.version+1
 BEGIN SELECT RAISE(ABORT,'invalid dispatch journal version'); END;

-- Exact ownership and creation operation are written BEFORE provider mutation. Resource
-- references may be populated once by evidence; absence is never a cleanup authorization.
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
CREATE TRIGGER resource_identity BEFORE UPDATE OF resource_id,workspace_id,job_id,attempt_id,instance_id,account_scope,purpose,operation_id,resource_key,identity,plan_sha256,created_at ON provider_resources
 BEGIN SELECT RAISE(ABORT,'immutable provider resource intent'); END;
CREATE TRIGGER resource_reference BEFORE UPDATE OF resource_ref ON provider_resources
 WHEN OLD.resource_ref!='' AND NEW.resource_ref!=OLD.resource_ref
 BEGIN SELECT RAISE(ABORT,'immutable discovered resource'); END;
CREATE TRIGGER resource_readiness BEFORE UPDATE OF readiness ON provider_resources
 WHEN OLD.readiness='ready' AND NEW.readiness!='ready'
 BEGIN SELECT RAISE(ABORT,'readiness cannot be silently reset'); END;

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
CREATE TRIGGER submission_identity BEFORE UPDATE OF intent_id,queue_seq,identity,prepared,created_at ON submission_intents
 BEGIN SELECT RAISE(ABORT,'immutable submission intent'); END;
CREATE TRIGGER submission_reference BEFORE UPDATE OF remote_ref ON submission_intents
 WHEN OLD.remote_ref!='' AND NEW.remote_ref!=OLD.remote_ref
 BEGIN SELECT RAISE(ABORT,'immutable execution reference'); END;
CREATE TRIGGER submission_terminal BEFORE UPDATE OF status ON submission_intents
 WHEN (OLD.status IN ('accepted','rejected') AND NEW.status!=OLD.status)
 OR (OLD.status!='started' AND NEW.status='started')
 BEGIN SELECT RAISE(ABORT,'submission outcome cannot be reset'); END;
CREATE INDEX dispatch_due ON dispatch_journals(phase,queue_seq);
