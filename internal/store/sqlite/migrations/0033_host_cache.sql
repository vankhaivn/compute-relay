-- Each content key has at most one active immutable volume generation. A
-- claimed writer is never replaced because of lease expiry or an unknown SSH
-- outcome; only a conclusive remote observation can retire it.
CREATE TABLE host_cache_generations (
 generation_id TEXT PRIMARY KEY,
 installation_id TEXT NOT NULL,
 machine_id TEXT NOT NULL CHECK(length(machine_id)=64),
 engine_id TEXT NOT NULL CHECK(length(engine_id)=64),
 sha256 TEXT NOT NULL CHECK(length(sha256)=64),
 bytes INTEGER NOT NULL CHECK(bytes>=0),
 volume_name TEXT NOT NULL UNIQUE,
 operation_id TEXT NOT NULL UNIQUE,
 owner_workspace_id TEXT NOT NULL,
 owner_job_id TEXT NOT NULL,
 owner_attempt_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('claimed','verified','retired')),
 writer_phase TEXT NOT NULL CHECK(writer_phase IN ('intent','begun','finished')),
 created_at TEXT NOT NULL,
 verified_at TEXT NOT NULL DEFAULT '',
 retired_at TEXT NOT NULL DEFAULT '',
 retired_reason TEXT NOT NULL DEFAULT '' CHECK(retired_reason IN ('','missing','corrupt')),
 last_used_at TEXT NOT NULL DEFAULT '',
 UNIQUE(generation_id,installation_id,machine_id,engine_id,sha256,bytes),
 FOREIGN KEY(owner_workspace_id,owner_job_id,owner_attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id),
 CHECK(state!='verified' OR verified_at!=''),
 CHECK(state!='retired' OR retired_at!='' AND retired_reason!=''),
 CHECK(state='claimed' OR writer_phase='finished')
) STRICT;

CREATE UNIQUE INDEX host_cache_active_key ON host_cache_generations(installation_id,machine_id,engine_id,sha256,bytes)
 WHERE state IN ('claimed','verified');
CREATE INDEX host_cache_generation_key ON host_cache_generations(installation_id,machine_id,engine_id,sha256,bytes,created_at);

CREATE TABLE host_cache_pins (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 installation_id TEXT NOT NULL,
 machine_id TEXT NOT NULL CHECK(length(machine_id)=64),
 engine_id TEXT NOT NULL CHECK(length(engine_id)=64),
 sha256 TEXT NOT NULL CHECK(length(sha256)=64),
 bytes INTEGER NOT NULL CHECK(bytes>=0),
 created_at TEXT NOT NULL,
 released_at TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(workspace_id,job_id,attempt_id,installation_id,machine_id,engine_id,sha256,bytes),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
CREATE INDEX host_cache_live_pins ON host_cache_pins(installation_id,machine_id,engine_id,sha256,bytes)
 WHERE released_at='';

-- A mount receipt is written only after inspection of the exact Docker
-- container and read-only volume mount. It is immutable and keeps the key
-- pinned until cleanup proves that container absent.
CREATE TABLE host_cache_mount_receipts (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 installation_id TEXT NOT NULL,
 machine_id TEXT NOT NULL,
 engine_id TEXT NOT NULL,
 sha256 TEXT NOT NULL,
 bytes INTEGER NOT NULL,
 generation_id TEXT NOT NULL,
 container_id TEXT NOT NULL CHECK(length(container_id)=64),
 volume_name TEXT NOT NULL,
 observed_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,job_id,attempt_id,installation_id,machine_id,engine_id,sha256,bytes),
 FOREIGN KEY(workspace_id,job_id,attempt_id,installation_id,machine_id,engine_id,sha256,bytes)
   REFERENCES host_cache_pins(workspace_id,job_id,attempt_id,installation_id,machine_id,engine_id,sha256,bytes),
 FOREIGN KEY(generation_id,installation_id,machine_id,engine_id,sha256,bytes)
   REFERENCES host_cache_generations(generation_id,installation_id,machine_id,engine_id,sha256,bytes)
) STRICT;
CREATE INDEX host_cache_mount_generation ON host_cache_mount_receipts(generation_id);

CREATE TRIGGER host_cache_generation_identity BEFORE UPDATE OF generation_id,installation_id,machine_id,engine_id,sha256,bytes,volume_name,operation_id,owner_workspace_id,owner_job_id,owner_attempt_id,created_at ON host_cache_generations
 BEGIN SELECT RAISE(ABORT,'immutable cache generation identity'); END;
CREATE TRIGGER host_cache_generation_transition BEFORE UPDATE OF state ON host_cache_generations
 WHEN NOT (OLD.state='claimed' AND OLD.writer_phase IN ('begun','finished') AND NEW.writer_phase='finished' AND NEW.state='verified' OR
           OLD.state='claimed' AND OLD.writer_phase='finished' AND NEW.state='retired' OR
           OLD.state='verified' AND NEW.state='retired')
 BEGIN SELECT RAISE(ABORT,'invalid cache generation transition'); END;
CREATE TRIGGER host_cache_writer_transition BEFORE UPDATE OF writer_phase ON host_cache_generations
 WHEN NOT (OLD.state='claimed' AND OLD.writer_phase='intent' AND NEW.writer_phase='begun' OR OLD.state='claimed' AND OLD.writer_phase='begun' AND NEW.writer_phase='finished')
 BEGIN SELECT RAISE(ABORT,'invalid cache writer transition'); END;
CREATE TRIGGER host_cache_verification_once BEFORE UPDATE OF verified_at ON host_cache_generations
 WHEN NOT (OLD.state='claimed' AND NEW.state='verified' AND OLD.verified_at='' AND NEW.verified_at!='')
 BEGIN SELECT RAISE(ABORT,'cache verification is one shot'); END;
CREATE TRIGGER host_cache_retirement_once BEFORE UPDATE OF retired_at,retired_reason ON host_cache_generations
 WHEN NOT (OLD.state IN ('claimed','verified') AND NEW.state='retired' AND OLD.retired_at='' AND OLD.retired_reason='' AND NEW.retired_at!='' AND NEW.retired_reason IN ('missing','corrupt'))
 BEGIN SELECT RAISE(ABORT,'cache retirement is one shot'); END;
CREATE TRIGGER host_cache_generation_keep BEFORE DELETE ON host_cache_generations
 BEGIN SELECT RAISE(ABORT,'cache generation cannot be forgotten'); END;
CREATE TRIGGER host_cache_pin_identity BEFORE UPDATE OF workspace_id,job_id,attempt_id,installation_id,machine_id,engine_id,sha256,bytes,created_at ON host_cache_pins
 BEGIN SELECT RAISE(ABORT,'immutable cache pin identity'); END;
CREATE TRIGGER host_cache_pin_release_once BEFORE UPDATE OF released_at ON host_cache_pins
 WHEN OLD.released_at!='' OR NEW.released_at=''
 BEGIN SELECT RAISE(ABORT,'cache pin release is one shot'); END;
CREATE TRIGGER host_cache_pin_keep BEFORE DELETE ON host_cache_pins
 BEGIN SELECT RAISE(ABORT,'cache pin cannot be forgotten'); END;
CREATE TRIGGER host_cache_mount_immutable BEFORE UPDATE ON host_cache_mount_receipts
 BEGIN SELECT RAISE(ABORT,'immutable cache mount receipt'); END;
CREATE TRIGGER host_cache_mount_keep BEFORE DELETE ON host_cache_mount_receipts
 BEGIN SELECT RAISE(ABORT,'cache mount receipt cannot be forgotten'); END;
