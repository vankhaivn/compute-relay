-- Collection samples do not change frozen requests, receipts or attempt identity.
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
