-- Retention deadlines belong to the original attempt and are never recomputed
-- from later observations or a restarted process clock. A deferred scan only
-- changes when the scheduler may inspect the immutable deadline again.
CREATE TABLE remote_retention_deadlines (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('collected','uncollected')),
 anchor_ms INTEGER NOT NULL CHECK(anchor_ms>0),
 due_ms INTEGER NOT NULL CHECK(due_ms>=anchor_ms),
 retry_after_ms INTEGER NOT NULL DEFAULT 0 CHECK(retry_after_ms>=0),
 operation_id TEXT,
 PRIMARY KEY(workspace_id,job_id,attempt_id,kind),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES operations(workspace_id,operation_id)
) STRICT;

CREATE INDEX remote_retention_due ON remote_retention_deadlines(retry_after_ms,due_ms);

CREATE TRIGGER remote_retention_identity BEFORE UPDATE OF workspace_id,job_id,attempt_id,kind,anchor_ms,due_ms ON remote_retention_deadlines
 BEGIN SELECT RAISE(ABORT,'immutable remote retention deadline'); END;

CREATE TRIGGER remote_retention_operation_once BEFORE UPDATE OF operation_id ON remote_retention_deadlines
 WHEN OLD.operation_id IS NOT NULL OR NEW.operation_id IS NULL OR NOT EXISTS(
 SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id AND o.job_id=NEW.job_id
 AND o.attempt_id=NEW.attempt_id AND o.operation_id=NEW.operation_id AND o.kind='cleanup')
 BEGIN SELECT RAISE(ABORT,'retention cleanup operation is one shot'); END;

CREATE TRIGGER remote_retention_keep BEFORE DELETE ON remote_retention_deadlines
 BEGIN SELECT RAISE(ABORT,'remote retention deadline cannot be forgotten'); END;

-- Jobs admitted with the policy before this migration may have settled already.
-- Round their old timestamps upward to avoid an early deletion. Attempts updated
-- after their first terminal fact get a conservatively late uncollected deadline.
INSERT INTO remote_retention_deadlines(workspace_id,job_id,attempt_id,kind,anchor_ms,due_ms)
 SELECT p.workspace_id,p.job_id,p.attempt_id,'collected',
 CAST(strftime('%s',p.verified_at) AS INTEGER)*1000+999,
 CAST(strftime('%s',p.verified_at) AS INTEGER)*1000+999+CAST(json_extract(j.request,'$.retention.after_collection_seconds') AS INTEGER)*1000
 FROM collection_publications p JOIN jobs j ON j.workspace_id=p.workspace_id AND j.job_id=p.job_id
 WHERE json_type(j.request,'$.retention.after_collection_seconds')='integer';

INSERT INTO remote_retention_deadlines(workspace_id,job_id,attempt_id,kind,anchor_ms,due_ms)
 SELECT a.workspace_id,a.job_id,a.attempt_id,'uncollected',
 CAST(strftime('%s',a.updated_at) AS INTEGER)*1000+999,
 CAST(strftime('%s',a.updated_at) AS INTEGER)*1000+999+CAST(json_extract(j.request,'$.retention.uncollected_max_seconds') AS INTEGER)*1000
 FROM attempts a JOIN jobs j ON j.workspace_id=a.workspace_id AND j.job_id=a.job_id
 WHERE json_type(j.request,'$.retention.uncollected_max_seconds')='integer'
 AND (a.orchestration IN ('succeeded','failed','cancelled','timed_out') OR
      (a.orchestration='needs_attention' AND json_extract(a.state,'$.Execution') IN ('succeeded','failed','cancelled','timed_out')))
 AND json_extract(a.state,'$.RemoteActivity') IN ('not_started','inactive');
