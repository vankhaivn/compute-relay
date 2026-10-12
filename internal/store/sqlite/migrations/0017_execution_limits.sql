-- Zero means no explicit override; verified adapters freeze their own defaults.
ALTER TABLE managed_connections ADD COLUMN max_active_attempts INTEGER NOT NULL DEFAULT 0
 CHECK(max_active_attempts BETWEEN 0 AND 64);
ALTER TABLE managed_connections ADD COLUMN max_processes INTEGER NOT NULL DEFAULT 0
 CHECK(max_processes BETWEEN 0 AND 65536);
