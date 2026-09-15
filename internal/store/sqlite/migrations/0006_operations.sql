-- Preserve every existing journal byte/version and widen only its phase vocabulary.
-- No submission intent, dispatch barrier or resource ownership record is reset.
CREATE TABLE dispatch_journals_v6 (
 queue_seq INTEGER PRIMARY KEY REFERENCES scheduler_queue(queue_seq),
 version INTEGER NOT NULL CHECK(version>=0),
 phase TEXT NOT NULL CHECK(phase IN ('local','staging','ready','submitting','submitted','rejected','collectible','attention','failed','prevented')),
 journal TEXT NOT NULL CHECK(length(journal)<=2097152),
 CHECK(json_extract(journal,'$.version')=version),
 CHECK(json_extract(journal,'$.phase')=phase)
) STRICT;
INSERT INTO dispatch_journals_v6 SELECT * FROM dispatch_journals;
DROP TABLE dispatch_journals;
ALTER TABLE dispatch_journals_v6 RENAME TO dispatch_journals;
CREATE TRIGGER dispatch_version BEFORE UPDATE ON dispatch_journals
 WHEN NEW.queue_seq!=OLD.queue_seq OR NEW.version!=OLD.version+1
 BEGIN SELECT RAISE(ABORT,'invalid dispatch journal version'); END;
CREATE INDEX dispatch_due ON dispatch_journals(phase,queue_seq);

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
-- Cancellation is one durable intent per attempt. Another HTTP key cannot repeat an
-- uncertain remote cancel. A new explicit retry can only replace its parent once.
CREATE UNIQUE INDEX operation_one_cancel ON operations(workspace_id,job_id,attempt_id) WHERE kind='cancel';
CREATE UNIQUE INDEX operation_one_retry ON operations(workspace_id,job_id,attempt_id) WHERE kind='retry_compute';
CREATE UNIQUE INDEX operation_pending ON operations(workspace_id,job_id,attempt_id,kind) WHERE status IN ('accepted','running');
CREATE INDEX operation_work ON operations(kind,status,operation_seq);
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
CREATE TRIGGER immutable_operation_receipt BEFORE UPDATE ON operation_idempotency
 BEGIN SELECT RAISE(ABORT,'immutable control receipt'); END;
-- Existing events used an empty operation ID. New links must belong to this exact
-- workspace/job and either the target attempt or the explicitly created retry attempt.
CREATE TRIGGER event_operation_owner BEFORE INSERT ON events
 WHEN NEW.operation_id!='' AND NOT EXISTS (
 SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id
 AND o.operation_id=NEW.operation_id AND o.job_id=NEW.job_id
 AND (o.attempt_id=NEW.attempt_id OR o.new_attempt_id=NEW.attempt_id))
 BEGIN SELECT RAISE(ABORT,'event control ownership mismatch'); END;
