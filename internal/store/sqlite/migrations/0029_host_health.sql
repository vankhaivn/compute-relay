-- Typed, sanitized observations are shared by every verified alias of a machine.
-- History is pruned by age and per-machine row count on reads and writes.
CREATE TABLE host_health_history (
 sequence INTEGER PRIMARY KEY,
 resource_id TEXT NOT NULL CHECK(length(resource_id)=73 AND substr(resource_id,1,9)='resource_'),
 observed_ms INTEGER NOT NULL CHECK(observed_ms>0),
 snapshot BLOB NOT NULL CHECK(length(snapshot) BETWEEN 1 AND 16384)
) STRICT;
CREATE INDEX host_health_machine_history ON host_health_history(resource_id,observed_ms DESC,sequence DESC);
CREATE INDEX host_health_age ON host_health_history(observed_ms);
CREATE TRIGGER host_health_snapshot_immutable BEFORE UPDATE ON host_health_history
 BEGIN SELECT RAISE(ABORT,'immutable host health observation'); END;
