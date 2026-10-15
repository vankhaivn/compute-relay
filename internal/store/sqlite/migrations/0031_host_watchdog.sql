CREATE TABLE host_watchdog_operations (
 operation_id TEXT PRIMARY KEY,
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 token_id TEXT NOT NULL,
 connection_id TEXT NOT NULL,
 resource_id TEXT NOT NULL,
 action TEXT NOT NULL CHECK(action IN ('install_host_watchdog','remove_host_watchdog')),
 status TEXT NOT NULL CHECK(status IN ('queued','unknown','succeeded','failed')),
 problem TEXT CHECK(problem IS NULL OR problem IN ('stale_revision','authority_revoked')),
 stage TEXT NOT NULL CHECK(stage IN ('queued','applying','done')),
 observed_ms INTEGER NOT NULL DEFAULT 0,
 lease_until INTEGER NOT NULL DEFAULT 0,
 target BLOB NOT NULL CHECK(length(target)<=16384),
 plan BLOB NOT NULL CHECK(length(plan)<=8192),
 receipt BLOB NOT NULL CHECK(length(receipt)<=4096),
 created_at TEXT NOT NULL,
 UNIQUE(workspace_id,key_sha256),
 CHECK((status='failed')=(problem IS NOT NULL))
) STRICT;
CREATE INDEX host_watchdog_resource ON host_watchdog_operations(resource_id,created_at);
