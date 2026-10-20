-- Exact consumer releases preserve consent and deletion receipts across restarts.
CREATE TABLE input_releases (
 workspace_id TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 request_json TEXT NOT NULL,
 released_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,key_sha256)
) STRICT;
CREATE TABLE input_released_jobs (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 artifact_id TEXT NOT NULL,
 sha256 TEXT NOT NULL,
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES collection_publications(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,artifact_id) REFERENCES artifacts(workspace_id,artifact_id)
) STRICT;
CREATE TABLE input_release_objects (
 workspace_id TEXT NOT NULL,
 key_sha256 TEXT NOT NULL,
 inventory_seq INTEGER NOT NULL REFERENCES retention_inventory(inventory_seq),
 PRIMARY KEY(workspace_id,key_sha256,inventory_seq),
 FOREIGN KEY(workspace_id,key_sha256) REFERENCES input_releases(workspace_id,key_sha256)
) STRICT;
CREATE TRIGGER input_released_job_identity BEFORE INSERT ON input_released_jobs
 WHEN NOT EXISTS(SELECT 1 FROM artifacts a WHERE a.workspace_id=NEW.workspace_id AND a.job_id=NEW.job_id
 AND a.attempt_id=NEW.attempt_id AND a.artifact_id=NEW.artifact_id AND a.sha256=NEW.sha256 AND a.role='output')
 BEGIN SELECT RAISE(ABORT,'input release output identity mismatch'); END;
CREATE TRIGGER input_release_immutable BEFORE UPDATE ON input_releases BEGIN SELECT RAISE(ABORT,'immutable input release'); END;
CREATE TRIGGER input_release_keep BEFORE DELETE ON input_releases BEGIN SELECT RAISE(ABORT,'input release cannot be forgotten'); END;
CREATE TRIGGER input_released_job_immutable BEFORE UPDATE ON input_released_jobs BEGIN SELECT RAISE(ABORT,'immutable input release job'); END;
CREATE TRIGGER input_released_job_keep BEFORE DELETE ON input_released_jobs BEGIN SELECT RAISE(ABORT,'input release job cannot be forgotten'); END;
CREATE TRIGGER input_release_object_immutable BEFORE UPDATE ON input_release_objects BEGIN SELECT RAISE(ABORT,'immutable input release object'); END;
CREATE TRIGGER input_release_object_keep BEFORE DELETE ON input_release_objects BEGIN SELECT RAISE(ABORT,'input release object cannot be forgotten'); END;
