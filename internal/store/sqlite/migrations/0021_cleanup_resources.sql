-- One immutable target snapshot and durable outcome per owned ledger resource.
-- Existing attempt-scoped operation receipts and old terminal records are unchanged.
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
