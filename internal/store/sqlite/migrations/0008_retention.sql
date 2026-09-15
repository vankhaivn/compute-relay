-- Record byte lifecycle separately from immutable job, receipt and provider history.
-- Legacy objects receive a fresh conservative observation time, never guessed age.
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
INSERT INTO retention_inventory(kind,workspace_id,object_id,bytes,sha256,recorded_at)
 SELECT 'input',workspace_id,object_id,bytes,sha256,strftime('%Y-%m-%dT%H:%M:%fZ','now') FROM objects;
INSERT INTO retention_inventory(kind,workspace_id,object_id,bytes,sha256,recorded_at)
 SELECT 'result',workspace_id,object_id,bytes,sha256,strftime('%Y-%m-%dT%H:%M:%fZ','now') FROM artifacts;
CREATE INDEX retention_pending ON retention_inventory(deleted_at,expired_at,inventory_seq);
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
-- SQL guards close admission/retry/freeze races after an eligibility read. Even a
-- stale caller that verified bytes before expiry cannot acquire a new reference.
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
CREATE TRIGGER retention_expiration_identity BEFORE UPDATE ON retention_expirations
 BEGIN SELECT RAISE(ABORT,'immutable result expiry'); END;
CREATE TRIGGER retention_expiration_keep BEFORE DELETE ON retention_expirations
 BEGIN SELECT RAISE(ABORT,'result expiry cannot be forgotten'); END;
CREATE TABLE retention_audit (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 workspace_id TEXT NOT NULL,
 target_kind TEXT NOT NULL,
 target_id TEXT NOT NULL,
 action TEXT NOT NULL CHECK(action IN ('hold','release','expire','delete')),
 occurred_at TEXT NOT NULL
) STRICT;
CREATE TRIGGER retention_audit_immutable BEFORE UPDATE ON retention_audit
 BEGIN SELECT RAISE(ABORT,'immutable retention audit'); END;
CREATE TRIGGER retention_audit_keep BEFORE DELETE ON retention_audit
 BEGIN SELECT RAISE(ABORT,'retention audit cannot be forgotten'); END;
CREATE TABLE retention_cursor (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 inventory_seq INTEGER NOT NULL CHECK(inventory_seq>=0)
) STRICT;
INSERT INTO retention_cursor VALUES(1,0);

-- Dry-run observations do not mutate the original ownership ledger, authorize
-- deletion or claim that this runtime deleted anything. Repeated absence is safe.
CREATE TABLE cleanup_previews (
 resource_id TEXT PRIMARY KEY REFERENCES provider_resources(resource_id),
 plan_sha256 TEXT NOT NULL CHECK(length(plan_sha256)=64),
 outcome TEXT NOT NULL CHECK(outcome IN ('would_delete','already_absent','retained')),
 observed_at TEXT NOT NULL
) STRICT;
