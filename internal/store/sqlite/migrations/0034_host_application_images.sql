-- A content key has one active image generation across workspaces and SSH
-- aliases of the same verified engine. Unknown load outcomes stay active so a
-- timed-out caller cannot send the archive twice.
CREATE TABLE host_application_images (
 generation_id TEXT PRIMARY KEY,
 installation_id TEXT NOT NULL,
 machine_id TEXT NOT NULL CHECK(length(machine_id)=64),
 engine_id TEXT NOT NULL CHECK(length(engine_id)=64),
 archive_sha256 TEXT NOT NULL CHECK(length(archive_sha256)=64),
 archive_bytes INTEGER NOT NULL CHECK(archive_bytes>0 AND archive_bytes<=2147483648),
 source_workspace_id TEXT NOT NULL,
 source_object_id TEXT NOT NULL,
 owner_job_id TEXT NOT NULL,
 owner_attempt_id TEXT NOT NULL,
 operation_id TEXT NOT NULL UNIQUE,
 expected_image_id TEXT NOT NULL DEFAULT '',
 rootfs_diff_ids TEXT NOT NULL DEFAULT '',
 state TEXT NOT NULL CHECK(state IN ('preparing','loading','ready','unknown','retired')),
 created_at TEXT NOT NULL,
 prepare_lease_until TEXT NOT NULL,
 loading_at TEXT NOT NULL DEFAULT '',
 ready_at TEXT NOT NULL DEFAULT '',
 last_used_at TEXT NOT NULL DEFAULT '',
 retired_at TEXT NOT NULL DEFAULT '',
 retired_reason TEXT NOT NULL DEFAULT '' CHECK(retired_reason IN ('','preload_expired','observed_missing')),
 FOREIGN KEY(source_workspace_id,owner_job_id,owner_attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 CHECK(state IN ('preparing','retired') OR expected_image_id!='' AND rootfs_diff_ids!=''),
 CHECK(state!='ready' OR ready_at!=''),
 CHECK(state!='retired' OR retired_at!='' AND retired_reason!='')
) STRICT;
CREATE UNIQUE INDEX host_application_image_active_key ON host_application_images
 (installation_id,machine_id,engine_id,archive_sha256,archive_bytes)
 WHERE state IN ('preparing','loading','ready','unknown');
CREATE INDEX host_application_image_history ON host_application_images
 (installation_id,machine_id,engine_id,archive_sha256,archive_bytes,created_at);

-- Insertion is the durable mutation permit. This table is append only.
CREATE TABLE host_application_image_loads (
 operation_id TEXT PRIMARY KEY,
 generation_id TEXT NOT NULL UNIQUE REFERENCES host_application_images(generation_id),
 expected_image_id TEXT NOT NULL CHECK(length(expected_image_id)=71 AND substr(expected_image_id,1,7)='sha256:'),
 begun_at TEXT NOT NULL
) STRICT;
CREATE TRIGGER host_application_image_load_keep BEFORE DELETE ON host_application_image_loads
 BEGIN SELECT RAISE(ABORT,'image load record cannot be forgotten'); END;
CREATE TRIGGER host_application_image_load_immutable BEFORE UPDATE ON host_application_image_loads
 BEGIN SELECT RAISE(ABORT,'image load record is immutable'); END;
CREATE TRIGGER host_application_image_keep BEFORE DELETE ON host_application_images
 BEGIN SELECT RAISE(ABORT,'image generation cannot be forgotten'); END;
CREATE TRIGGER host_application_image_identity BEFORE UPDATE OF generation_id,installation_id,machine_id,engine_id,archive_sha256,archive_bytes,source_workspace_id,source_object_id,owner_job_id,owner_attempt_id,operation_id,created_at ON host_application_images
 BEGIN SELECT RAISE(ABORT,'image generation identity is immutable'); END;

CREATE TABLE host_application_image_pins (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 installation_id TEXT NOT NULL,
 machine_id TEXT NOT NULL CHECK(length(machine_id)=64),
 engine_id TEXT NOT NULL CHECK(length(engine_id)=64),
 archive_sha256 TEXT NOT NULL CHECK(length(archive_sha256)=64),
 archive_bytes INTEGER NOT NULL CHECK(archive_bytes>0 AND archive_bytes<=2147483648),
 object_id TEXT NOT NULL,
 created_at TEXT NOT NULL,
 released_at TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
CREATE INDEX host_application_image_live_pins ON host_application_image_pins
 (installation_id,machine_id,engine_id,archive_sha256,archive_bytes) WHERE released_at='';
CREATE TRIGGER host_application_image_pin_keep BEFORE DELETE ON host_application_image_pins
 BEGIN SELECT RAISE(ABORT,'image pin cannot be forgotten'); END;
CREATE TRIGGER host_application_image_pin_identity BEFORE UPDATE OF workspace_id,job_id,attempt_id,installation_id,machine_id,engine_id,archive_sha256,archive_bytes,object_id,created_at ON host_application_image_pins
 BEGIN SELECT RAISE(ABORT,'image pin identity is immutable'); END;
CREATE TRIGGER host_application_image_pin_release_once BEFORE UPDATE OF released_at ON host_application_image_pins
 WHEN OLD.released_at!='' OR NEW.released_at=''
 BEGIN SELECT RAISE(ABORT,'image pin release is one shot'); END;
