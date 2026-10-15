-- Non-secret effective thresholds for future SSH connection revisions. Existing
-- rows retain NULL and explicit legacy-v1 interpretation; old runtime JSON is immutable.
ALTER TABLE managed_connections ADD COLUMN host_health BLOB
 CHECK(host_health IS NULL OR length(host_health) BETWEEN 1 AND 2048);
