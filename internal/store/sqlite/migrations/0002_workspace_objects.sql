CREATE TABLE workspaces (
    workspace_id TEXT PRIMARY KEY NOT NULL CHECK (length(workspace_id) BETWEEN 1 AND 128),
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    allowed_profiles TEXT NOT NULL CHECK (length(allowed_profiles) BETWEEN 2 AND 20000)
) STRICT;

CREATE TABLE api_token_hashes (
    token_id TEXT PRIMARY KEY NOT NULL CHECK (length(token_id) BETWEEN 1 AND 128),
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    digest BLOB NOT NULL UNIQUE CHECK (length(digest) = 32),
    scopes TEXT NOT NULL CHECK (length(scopes) BETWEEN 2 AND 100),
    created_at TEXT NOT NULL,
    expires_at TEXT,
    revoked INTEGER NOT NULL DEFAULT 0 CHECK (revoked IN (0, 1))
) STRICT;
CREATE INDEX api_token_workspace ON api_token_hashes(workspace_id);

CREATE TABLE objects (
    workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id) ON DELETE RESTRICT,
    object_id TEXT NOT NULL CHECK (length(object_id) BETWEEN 1 AND 128),
    bytes INTEGER NOT NULL CHECK (bytes >= 0),
    sha256 TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
    PRIMARY KEY (workspace_id, object_id)
) STRICT;
