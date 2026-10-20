-- Explicit consumer releases reuse result expiry and its durable deletion tickets.
-- Keep the original request identity after expiry and physical reclamation.
CREATE TABLE result_releases (
 workspace_id TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 artifact_id TEXT NOT NULL,
 sha256 TEXT NOT NULL CHECK(length(sha256)=64),
 PRIMARY KEY(workspace_id,key_sha256),
 FOREIGN KEY(workspace_id,job_id,attempt_id)
  REFERENCES retention_expirations(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,artifact_id) REFERENCES artifacts(workspace_id,artifact_id)
) STRICT;
CREATE TRIGGER result_release_identity BEFORE INSERT ON result_releases
 WHEN NOT EXISTS(SELECT 1 FROM artifacts a WHERE a.workspace_id=NEW.workspace_id
 AND a.job_id=NEW.job_id AND a.attempt_id=NEW.attempt_id
 AND a.artifact_id=NEW.artifact_id AND a.sha256=NEW.sha256 AND a.role='output')
 BEGIN SELECT RAISE(ABORT,'result release identity mismatch'); END;
CREATE TRIGGER result_release_immutable BEFORE UPDATE ON result_releases
 BEGIN SELECT RAISE(ABORT,'immutable result release'); END;
CREATE TRIGGER result_release_keep BEFORE DELETE ON result_releases
 BEGIN SELECT RAISE(ABORT,'result release cannot be forgotten'); END;
