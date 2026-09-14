CREATE TABLE runtime_installation (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    installation_id TEXT NOT NULL UNIQUE CHECK (length(installation_id) BETWEEN 1 AND 128),
    created_at TEXT NOT NULL
) STRICT;
