-- Preserve released and expired generations as well as live ownership. Keys are
-- canonical JSON tuples, so opaque IDs containing ':' cannot alias another tuple.
CREATE TABLE work_leases (
 kind TEXT NOT NULL CHECK(kind IN ('dispatch','collection','cleanup')),
 key TEXT NOT NULL CHECK(COALESCE(json_valid(key) AND json_type(key)='array' AND
 CASE kind
 WHEN 'dispatch' THEN json_array_length(key)=1 AND json_type(key,'$[0]')='integer'
  AND json_extract(key,'$[0]')>0 AND key=json_array(json_extract(key,'$[0]'))
 WHEN 'collection' THEN json_array_length(key)=3 AND json_type(key,'$[0]')='text'
  AND json_type(key,'$[1]')='text' AND json_type(key,'$[2]')='text'
  AND key=json_array(json_extract(key,'$[0]'),json_extract(key,'$[1]'),json_extract(key,'$[2]'))
 WHEN 'cleanup' THEN json_array_length(key)=2 AND json_type(key,'$[0]')='text'
  AND json_type(key,'$[1]')='text' AND key=json_array(json_extract(key,'$[0]'),json_extract(key,'$[1]')) END,0)),
 generation INTEGER NOT NULL CHECK(generation>0),
 owner TEXT NOT NULL,
 fence TEXT NOT NULL,
 until_ms INTEGER NOT NULL CHECK(until_ms>=0),
 held INTEGER NOT NULL CHECK(held IN (0,1)),
 operation_id TEXT,
 attempt_revision INTEGER,
 queue_seq INTEGER GENERATED ALWAYS AS (CASE WHEN kind='dispatch' THEN json_extract(key,'$[0]') END) STORED,
 workspace_id TEXT GENERATED ALWAYS AS (CASE WHEN kind!='dispatch' THEN json_extract(key,'$[0]') END) STORED,
 job_id TEXT GENERATED ALWAYS AS (CASE WHEN kind='collection' THEN json_extract(key,'$[1]') END) STORED,
 attempt_id TEXT GENERATED ALWAYS AS (CASE WHEN kind='collection' THEN json_extract(key,'$[2]') END) STORED,
 cleanup_operation_id TEXT GENERATED ALWAYS AS (CASE WHEN kind='cleanup' THEN json_extract(key,'$[1]') END) STORED,
 PRIMARY KEY(kind,key),
 FOREIGN KEY(queue_seq) REFERENCES scheduler_queue(queue_seq),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES operations(workspace_id,operation_id),
 FOREIGN KEY(workspace_id,cleanup_operation_id) REFERENCES cleanup_operations(workspace_id,operation_id),
 CHECK((kind='collection' AND operation_id IS NOT NULL AND attempt_revision IS NOT NULL AND attempt_revision>0)
  OR (kind!='collection' AND operation_id IS NULL AND attempt_revision IS NULL)),
 CHECK((kind='dispatch' AND ((held=1 AND length(owner) BETWEEN 1 AND 128 AND length(fence)=64 AND until_ms>0)
  OR (held=0 AND ((owner='' AND fence='' AND until_ms=0) OR (length(owner) BETWEEN 1 AND 128 AND length(fence)=64 AND until_ms>0)))))
  OR (kind='collection' AND owner='' AND length(fence)>=32)
  OR (kind='cleanup' AND owner='' AND length(fence)=64 AND until_ms>0))
) STRICT;
CREATE INDEX work_lease_capacity ON work_leases(kind,held,until_ms);
INSERT INTO work_leases(kind,key,generation,owner,fence,until_ms,held)
 SELECT 'dispatch',json_array(queue_seq),generation,owner,fence,until_ms,held FROM scheduler_leases;
INSERT INTO work_leases(kind,key,generation,owner,fence,until_ms,held,operation_id,attempt_revision)
 SELECT 'collection',json_array(workspace_id,job_id,attempt_id),generation,'',fence,until_ms,held,operation_id,attempt_revision FROM collection_leases;
INSERT INTO work_leases(kind,key,generation,owner,fence,until_ms,held)
 SELECT 'cleanup',json_array(l.workspace_id,l.operation_id),l.generation,'',l.fence,l.until_ms,c.status='running'
 FROM cleanup_leases l JOIN cleanup_operations c ON c.workspace_id=l.workspace_id AND c.operation_id=l.operation_id;

-- Retain the progress child's foreign key to its lease, including historical
-- released progress. New checkpoints still require the exact current held fence.
CREATE TABLE collection_progress_v26 (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0),
 fence TEXT NOT NULL CHECK(length(fence)>=32),
 stage TEXT NOT NULL CHECK(stage IN ('discovering','transferring','verifying','available','failed')),
 progress TEXT NOT NULL CHECK(length(progress)<=2048 AND json_valid(progress)),
 lease_kind TEXT GENERATED ALWAYS AS ('collection') STORED,
 lease_key TEXT GENERATED ALWAYS AS (json_array(workspace_id,job_id,attempt_id)) STORED,
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(lease_kind,lease_key) REFERENCES work_leases(kind,key),
 FOREIGN KEY(workspace_id,operation_id) REFERENCES operations(workspace_id,operation_id)
) STRICT;
INSERT INTO collection_progress_v26(workspace_id,job_id,attempt_id,operation_id,generation,fence,stage,progress)
 SELECT workspace_id,job_id,attempt_id,operation_id,generation,fence,stage,progress FROM collection_progress;
DROP TABLE collection_progress;
ALTER TABLE collection_progress_v26 RENAME TO collection_progress;
DROP TABLE scheduler_leases;
DROP TABLE collection_leases;
DROP TABLE cleanup_leases;

CREATE TRIGGER work_lease_insert BEFORE INSERT ON work_leases
 WHEN NEW.generation!=1
 OR (NEW.kind='collection' AND NOT EXISTS (SELECT 1 FROM operations o
  WHERE o.workspace_id=NEW.workspace_id AND o.operation_id=NEW.operation_id
  AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id AND o.kind='collect'))
 OR (NEW.kind='cleanup' AND NOT EXISTS (SELECT 1 FROM cleanup_operations c
  WHERE c.workspace_id=NEW.workspace_id AND c.operation_id=NEW.cleanup_operation_id AND c.status='running'))
 BEGIN SELECT RAISE(ABORT,'work lease owner mismatch'); END;
CREATE TRIGGER work_lease_update BEFORE UPDATE ON work_leases
 WHEN NEW.kind!=OLD.kind OR NEW.key!=OLD.key
 OR NEW.generation<OLD.generation OR NEW.generation>OLD.generation+1
 OR (NEW.generation=OLD.generation AND (NEW.owner!=OLD.owner OR NEW.fence!=OLD.fence
  OR NEW.operation_id IS NOT OLD.operation_id OR NEW.attempt_revision IS NOT OLD.attempt_revision
  OR OLD.held!=1 OR NEW.until_ms<OLD.until_ms OR (NEW.held=0 AND NEW.until_ms!=OLD.until_ms)))
 OR (NEW.generation=OLD.generation+1 AND (NEW.fence=OLD.fence OR NEW.held!=1))
 OR (NEW.kind='collection' AND NOT EXISTS (SELECT 1 FROM operations o
  WHERE o.workspace_id=NEW.workspace_id AND o.operation_id=NEW.operation_id
  AND o.job_id=NEW.job_id AND o.attempt_id=NEW.attempt_id AND o.kind='collect'))
 OR (NEW.kind='cleanup' AND NOT EXISTS (SELECT 1 FROM cleanup_operations c
  WHERE c.workspace_id=NEW.workspace_id AND c.operation_id=NEW.cleanup_operation_id AND c.status='running'))
 BEGIN SELECT RAISE(ABORT,'invalid work lease transition'); END;
CREATE TRIGGER work_lease_keep BEFORE DELETE ON work_leases
 BEGIN SELECT RAISE(ABORT,'work lease history cannot be forgotten'); END;
CREATE TRIGGER collection_progress_owner_insert BEFORE INSERT ON collection_progress
 WHEN NOT EXISTS (SELECT 1 FROM work_leases c WHERE c.kind='collection' AND c.key=NEW.lease_key
 AND c.operation_id=NEW.operation_id AND c.generation=NEW.generation AND c.fence=NEW.fence AND c.held=1)
 BEGIN SELECT RAISE(ABORT,'collection progress owner mismatch'); END;
CREATE TRIGGER collection_progress_owner_update BEFORE UPDATE ON collection_progress
 WHEN NEW.workspace_id!=OLD.workspace_id OR NEW.job_id!=OLD.job_id OR NEW.attempt_id!=OLD.attempt_id
 OR NOT EXISTS (SELECT 1 FROM work_leases c WHERE c.kind='collection' AND c.key=NEW.lease_key
 AND c.operation_id=NEW.operation_id AND c.generation=NEW.generation AND c.fence=NEW.fence AND c.held=1)
 OR (NEW.generation=OLD.generation AND (NEW.operation_id!=OLD.operation_id OR NEW.fence!=OLD.fence
 OR json_extract(NEW.progress,'$.bytes_completed')<json_extract(OLD.progress,'$.bytes_completed')
 OR json_extract(NEW.progress,'$.bytes_received')<json_extract(OLD.progress,'$.bytes_received')))
 BEGIN SELECT RAISE(ABORT,'invalid collection progress transition'); END;
