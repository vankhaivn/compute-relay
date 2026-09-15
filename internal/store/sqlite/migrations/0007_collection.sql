-- Add transfer-only ownership and publication. Earlier migrations, dispatch
-- identities, possible remote activity and original operation receipts are untouched.
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
CREATE INDEX collection_capacity ON collection_leases(held,until_ms);
CREATE TRIGGER collection_owner_insert BEFORE INSERT ON collection_leases
 WHEN NOT EXISTS (SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id
 AND o.operation_id=NEW.operation_id AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id AND o.kind='collect')
 BEGIN SELECT RAISE(ABORT,'collection operation owner mismatch'); END;
CREATE TRIGGER collection_owner_update BEFORE UPDATE ON collection_leases
 WHEN NEW.workspace_id!=OLD.workspace_id OR NEW.job_id!=OLD.job_id OR NEW.attempt_id!=OLD.attempt_id
 OR NOT EXISTS (SELECT 1 FROM operations o WHERE o.workspace_id=NEW.workspace_id
 AND o.operation_id=NEW.operation_id AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id AND o.kind='collect')
 BEGIN SELECT RAISE(ABORT,'collection operation owner mismatch'); END;

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
CREATE TRIGGER immutable_collection_snapshot BEFORE UPDATE ON collection_snapshots
 BEGIN SELECT RAISE(ABORT,'immutable collection snapshot'); END;

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
CREATE TRIGGER immutable_collection_publication BEFORE UPDATE ON collection_publications
 BEGIN SELECT RAISE(ABORT,'immutable collection publication'); END;

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
