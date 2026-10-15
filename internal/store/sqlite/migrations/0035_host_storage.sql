-- P4 machine storage is separate from attempt cleanup. Original attempt and
-- operation receipts retain their foreign keys and exact frozen identities.
ALTER TABLE managed_connections ADD COLUMN storage_soft_bytes INTEGER
 CHECK(storage_soft_bytes IS NULL OR storage_soft_bytes BETWEEN 1 AND 1125899906842624);
ALTER TABLE managed_connections ADD COLUMN storage_hard_bytes INTEGER
 CHECK(storage_hard_bytes IS NULL OR storage_hard_bytes BETWEEN 2 AND 1125899906842624);
CREATE TRIGGER managed_storage_budget_insert BEFORE INSERT ON managed_connections
 WHEN (NEW.storage_soft_bytes IS NULL)!=(NEW.storage_hard_bytes IS NULL)
 OR (NEW.storage_soft_bytes IS NOT NULL AND NEW.storage_soft_bytes>=NEW.storage_hard_bytes)
 BEGIN SELECT RAISE(ABORT,'invalid managed storage budget'); END;
CREATE TRIGGER managed_storage_budget_update BEFORE UPDATE OF storage_soft_bytes,storage_hard_bytes ON managed_connections
 WHEN (NEW.storage_soft_bytes IS NULL)!=(NEW.storage_hard_bytes IS NULL)
 OR (NEW.storage_soft_bytes IS NOT NULL AND NEW.storage_soft_bytes>=NEW.storage_hard_bytes)
 BEGIN SELECT RAISE(ABORT,'invalid managed storage budget'); END;

CREATE TABLE host_storage_scans (
 scan_seq INTEGER PRIMARY KEY AUTOINCREMENT,
 installation_id TEXT NOT NULL,
 machine_id TEXT NOT NULL CHECK(length(machine_id)=64),
 engine_id TEXT NOT NULL CHECK(length(engine_id)=64),
 observed_ms INTEGER NOT NULL CHECK(observed_ms>0),
 status TEXT NOT NULL CHECK(status IN ('complete','incomplete')),
 reason TEXT NOT NULL DEFAULT '' CHECK(length(reason)<=64),
 foreign_containers INTEGER NOT NULL DEFAULT 0 CHECK(foreign_containers>=0),
 foreign_volumes INTEGER NOT NULL DEFAULT 0 CHECK(foreign_volumes>=0),
 foreign_images INTEGER NOT NULL DEFAULT 0 CHECK(foreign_images>=0),
 CHECK((status='complete')=(reason=''))
) STRICT;
CREATE INDEX host_storage_scan_scope ON host_storage_scans(installation_id,machine_id,engine_id,scan_seq DESC);
CREATE TRIGGER host_storage_scan_immutable BEFORE UPDATE ON host_storage_scans
 BEGIN SELECT RAISE(ABORT,'immutable host storage scan'); END;

-- These are private exact observations. Public routes project opaque orphan IDs,
-- class counts and bytes only; no Docker ID or labels escape the store.
CREATE TABLE host_storage_objects (
 scan_seq INTEGER NOT NULL REFERENCES host_storage_scans(scan_seq),
 class TEXT NOT NULL CHECK(class IN ('container','volume','image')),
 object_id TEXT NOT NULL CHECK(length(object_id) BETWEEN 1 AND 255),
 size_bytes INTEGER,
 labels_json TEXT NOT NULL CHECK(length(labels_json)<=4096 AND json_valid(labels_json)),
 running INTEGER NOT NULL DEFAULT 0 CHECK(running IN (0,1)),
 references_count INTEGER NOT NULL DEFAULT 0 CHECK(references_count>=0),
 PRIMARY KEY(scan_seq,class,object_id),
 CHECK(size_bytes IS NULL OR size_bytes>=0)
) STRICT;
CREATE TRIGGER host_storage_object_immutable BEFORE UPDATE ON host_storage_objects
 BEGIN SELECT RAISE(ABORT,'immutable host storage observation'); END;

-- An observed absence is not a Relay deletion receipt. Keep the original
-- generation/attempt records and their operation histories intact.
CREATE TABLE host_storage_absences (
 installation_id TEXT NOT NULL,
 machine_id TEXT NOT NULL CHECK(length(machine_id)=64),
 engine_id TEXT NOT NULL CHECK(length(engine_id)=64),
 class TEXT NOT NULL CHECK(class IN ('container','volume','image')),
 object_id TEXT NOT NULL CHECK(length(object_id) BETWEEN 1 AND 255),
 first_scan_seq INTEGER NOT NULL REFERENCES host_storage_scans(scan_seq),
 last_scan_seq INTEGER NOT NULL REFERENCES host_storage_scans(scan_seq),
 PRIMARY KEY(installation_id,machine_id,engine_id,class,object_id)
) STRICT;
CREATE TRIGGER host_storage_absence_identity BEFORE UPDATE OF installation_id,machine_id,engine_id,class,object_id,first_scan_seq ON host_storage_absences
 BEGIN SELECT RAISE(ABORT,'immutable storage absence identity'); END;

-- A provisional reservation is accepted with its attempt. A later unique
-- writer claim transfers it to the immutable remote mutation operation.
CREATE TABLE host_storage_reservations (
 reservation_id TEXT PRIMARY KEY,
 installation_id TEXT NOT NULL,
 machine_id TEXT NOT NULL CHECK(length(machine_id)=64),
 engine_id TEXT NOT NULL CHECK(length(engine_id)=64),
 class TEXT NOT NULL CHECK(class IN ('attempt','cache','image')),
 resource_key TEXT NOT NULL CHECK(length(resource_key) BETWEEN 1 AND 255),
 bytes INTEGER NOT NULL CHECK(bytes>=0),
 owner_workspace_id TEXT NOT NULL,
 owner_job_id TEXT NOT NULL,
 owner_attempt_id TEXT NOT NULL,
 mutation_operation_id TEXT,
 state TEXT NOT NULL CHECK(state IN ('provisional','committed','released')),
 created_at TEXT NOT NULL,
 released_at TEXT NOT NULL DEFAULT '',
 FOREIGN KEY(owner_workspace_id,owner_job_id,owner_attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 CHECK((state='released')=(released_at!=''))
) STRICT;
CREATE INDEX host_storage_live_reservations ON host_storage_reservations(installation_id,machine_id,engine_id,state) WHERE state!='released';
CREATE UNIQUE INDEX host_storage_active_reservation_key ON host_storage_reservations(installation_id,machine_id,engine_id,class,resource_key)
 WHERE state='provisional' OR (class='attempt' AND state!='released');
CREATE UNIQUE INDEX host_storage_committed_mutation ON host_storage_reservations(installation_id,machine_id,engine_id,class,mutation_operation_id)
 WHERE state='committed';
CREATE TRIGGER host_storage_reservation_identity BEFORE UPDATE OF reservation_id,installation_id,machine_id,engine_id,class,resource_key,bytes,owner_workspace_id,owner_job_id,owner_attempt_id,created_at ON host_storage_reservations
 BEGIN SELECT RAISE(ABORT,'immutable storage reservation identity'); END;
CREATE TRIGGER host_storage_reservation_state BEFORE UPDATE OF state ON host_storage_reservations
 WHEN NOT (OLD.state='provisional' AND NEW.state IN ('committed','released') OR OLD.state='committed' AND NEW.state='released')
 BEGIN SELECT RAISE(ABORT,'invalid storage reservation transition'); END;
CREATE TRIGGER host_storage_reservation_keep BEFORE DELETE ON host_storage_reservations
 BEGIN SELECT RAISE(ABORT,'storage reservation cannot be forgotten'); END;

CREATE TABLE host_storage_previews (
 preview_id TEXT PRIMARY KEY,
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 connection_id TEXT NOT NULL REFERENCES managed_connections(connection_id),
 scan_seq INTEGER NOT NULL REFERENCES host_storage_scans(scan_seq),
 class TEXT NOT NULL CHECK(class IN ('container','volume','image')),
 object_id TEXT NOT NULL CHECK(length(object_id) BETWEEN 1 AND 255),
 fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64),
 eligible INTEGER NOT NULL CHECK(eligible IN (0,1)),
 reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 64),
 expires_ms INTEGER NOT NULL CHECK(expires_ms>0),
 created_at TEXT NOT NULL
) STRICT;
CREATE INDEX host_storage_preview_expiry ON host_storage_previews(expires_ms);
CREATE TRIGGER host_storage_preview_immutable BEFORE UPDATE ON host_storage_previews
 BEGIN SELECT RAISE(ABORT,'immutable storage preview'); END;

-- This sibling journal has no fake job/attempt FK. Its worker shares the exact
-- cleanup engine algorithm while its target is a frozen Docker object.
CREATE TABLE host_storage_cleanup_operations (
 operation_id TEXT PRIMARY KEY,
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 connection_id TEXT NOT NULL REFERENCES managed_connections(connection_id),
 preview_id TEXT REFERENCES host_storage_previews(preview_id),
 installation_id TEXT NOT NULL,
 machine_id TEXT NOT NULL CHECK(length(machine_id)=64),
 engine_id TEXT NOT NULL CHECK(length(engine_id)=64),
 action TEXT NOT NULL CHECK(action IN ('orphan_container','orphan_volume','orphan_image','evict_cache','evict_image')),
 class TEXT NOT NULL CHECK(class IN ('container','volume','image')),
 object_id TEXT NOT NULL CHECK(length(object_id) BETWEEN 1 AND 255),
 labels_json TEXT NOT NULL CHECK(length(labels_json)<=4096 AND json_valid(labels_json)),
 binding_json TEXT NOT NULL CHECK(length(binding_json)<=8192 AND json_valid(binding_json)),
 fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64),
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 actor_token_id TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('accepted','running','succeeded','failed')),
 result TEXT NOT NULL CHECK(result IN ('pending','running','deleted','absent','retained','failed','unknown')),
 reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 64),
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 confirmed_scan_seq INTEGER NOT NULL DEFAULT 0 CHECK(confirmed_scan_seq>=0),
 UNIQUE(workspace_id,key_sha256)
) STRICT;
CREATE UNIQUE INDEX host_storage_cleanup_active_target ON host_storage_cleanup_operations(installation_id,machine_id,engine_id,class,object_id)
 WHERE status IN ('accepted','running');
CREATE INDEX host_storage_cleanup_work ON host_storage_cleanup_operations(status,created_at,operation_id);
CREATE TRIGGER host_storage_cleanup_identity BEFORE UPDATE OF operation_id,workspace_id,connection_id,preview_id,installation_id,machine_id,engine_id,action,class,object_id,labels_json,binding_json,fingerprint,key_sha256,actor_token_id,created_at ON host_storage_cleanup_operations
 BEGIN SELECT RAISE(ABORT,'immutable storage cleanup identity'); END;
CREATE TRIGGER host_storage_cleanup_terminal BEFORE UPDATE ON host_storage_cleanup_operations
 WHEN OLD.status IN ('succeeded','failed') BEGIN SELECT RAISE(ABORT,'terminal storage cleanup'); END;
CREATE TRIGGER host_storage_cleanup_keep BEFORE DELETE ON host_storage_cleanup_operations
 BEGIN SELECT RAISE(ABORT,'storage cleanup receipt cannot be forgotten'); END;

CREATE TABLE host_storage_cleanup_leases (
 operation_id TEXT PRIMARY KEY REFERENCES host_storage_cleanup_operations(operation_id),
 generation INTEGER NOT NULL CHECK(generation>0),
 fence TEXT NOT NULL CHECK(length(fence)=64),
 until_ms INTEGER NOT NULL CHECK(until_ms>0)
) STRICT;

-- An accepted exact deletion owns the target until it settles. An unknown
-- result keeps that fence because a timed-out rm may still complete remotely.
-- The check runs inside the same SQLite writer transaction as admission pins,
-- closing the window between the worker's eligibility read and Docker rm.
CREATE TRIGGER host_storage_cache_pin_delete_fence BEFORE INSERT ON host_cache_pins
 WHEN EXISTS(SELECT 1 FROM host_cache_generations g JOIN host_storage_cleanup_operations o
 ON o.installation_id=g.installation_id AND o.machine_id=g.machine_id AND o.engine_id=g.engine_id
 AND o.class='volume' AND o.object_id=g.volume_name
 WHERE g.installation_id=NEW.installation_id AND g.machine_id=NEW.machine_id AND g.engine_id=NEW.engine_id
 AND g.sha256=NEW.sha256 AND g.bytes=NEW.bytes
 AND (o.status IN ('accepted','running') OR o.result='unknown'))
 BEGIN SELECT RAISE(ABORT,'storage target deletion fenced'); END;
CREATE TRIGGER host_storage_image_pin_delete_fence BEFORE INSERT ON host_application_image_pins
 WHEN EXISTS(SELECT 1 FROM host_application_images g JOIN host_storage_cleanup_operations o
 ON o.installation_id=g.installation_id AND o.machine_id=g.machine_id AND o.engine_id=g.engine_id
 AND o.class='image' AND o.object_id=g.expected_image_id
 WHERE g.installation_id=NEW.installation_id AND g.machine_id=NEW.machine_id AND g.engine_id=NEW.engine_id
 AND g.archive_sha256=NEW.archive_sha256 AND g.archive_bytes=NEW.archive_bytes
 AND (o.status IN ('accepted','running') OR o.result='unknown'))
 BEGIN SELECT RAISE(ABORT,'storage target deletion fenced'); END;
