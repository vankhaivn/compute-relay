-- No table here represents remote acceptance or authorization to dispatch.
CREATE TABLE profile_revisions (
 profile TEXT NOT NULL,
 revision TEXT NOT NULL,
 snapshot TEXT NOT NULL CHECK(length(snapshot)<=8192),
 PRIMARY KEY(profile,revision)
) STRICT;
CREATE TABLE profiles (
 profile TEXT PRIMARY KEY,
 revision TEXT NOT NULL,
 enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
 FOREIGN KEY(profile,revision) REFERENCES profile_revisions(profile,revision)
) STRICT;
CREATE TABLE jobs (
 workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
 job_id TEXT NOT NULL,
 request TEXT NOT NULL CHECK(length(request)<=1048576),
 canonical_version TEXT NOT NULL,
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 profile TEXT NOT NULL,
 profile_revision TEXT NOT NULL,
 active_attempt_id TEXT NOT NULL,
 created_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,job_id),
 FOREIGN KEY(profile,profile_revision) REFERENCES profile_revisions(profile,revision),
 FOREIGN KEY(workspace_id,job_id,active_attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE attempts (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 number INTEGER NOT NULL CHECK(number>0),
 nonce TEXT NOT NULL CHECK(length(nonce)=64),
 state TEXT NOT NULL CHECK(length(state)<=4096),
 orchestration TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision>0),
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 UNIQUE(workspace_id,attempt_id),
 UNIQUE(workspace_id,job_id,number),
 FOREIGN KEY(workspace_id,job_id) REFERENCES jobs(workspace_id,job_id)
) STRICT;
CREATE TABLE job_objects (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 role TEXT NOT NULL,
 object_id TEXT NOT NULL,
 bytes INTEGER NOT NULL CHECK(bytes>=0),
 sha256 TEXT NOT NULL CHECK(length(sha256)=64),
 PRIMARY KEY(workspace_id,job_id,role),
 FOREIGN KEY(workspace_id,job_id) REFERENCES jobs(workspace_id,job_id),
 FOREIGN KEY(workspace_id,object_id) REFERENCES objects(workspace_id,object_id)
) STRICT;
CREATE INDEX job_objects_owner ON job_objects(workspace_id,object_id);
CREATE TABLE events (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 event_id TEXT NOT NULL,
 sequence INTEGER NOT NULL CHECK(sequence>0),
 attempt_id TEXT NOT NULL,
 operation_id TEXT NOT NULL DEFAULT '',
 type TEXT NOT NULL,
 occurred_at TEXT NOT NULL,
 PRIMARY KEY(workspace_id,job_id,sequence),
 UNIQUE(workspace_id,event_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
CREATE TABLE idempotency (
 workspace_id TEXT NOT NULL,
 operation TEXT NOT NULL,
 key_sha256 TEXT NOT NULL CHECK(length(key_sha256)=64),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64),
 canonical_version TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 receipt TEXT NOT NULL CHECK(length(receipt)<=4096),
 PRIMARY KEY(workspace_id,operation,key_sha256),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
CREATE INDEX attempts_outstanding ON attempts(orchestration,workspace_id);
-- Protect frozen identity/content even from accidental updates by future store methods.
CREATE TRIGGER immutable_profile_revision BEFORE UPDATE ON profile_revisions BEGIN SELECT RAISE(ABORT,'immutable profile revision'); END;
CREATE TRIGGER immutable_job_request BEFORE UPDATE OF request,canonical_version,request_sha256,profile,profile_revision,created_at,job_id,workspace_id ON jobs BEGIN SELECT RAISE(ABORT,'immutable job request'); END;
CREATE TRIGGER immutable_job_object BEFORE UPDATE ON job_objects BEGIN SELECT RAISE(ABORT,'immutable job object'); END;
CREATE TRIGGER immutable_idempotency BEFORE UPDATE ON idempotency BEGIN SELECT RAISE(ABORT,'immutable admission receipt'); END;
CREATE TRIGGER immutable_event BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT,'immutable event'); END;
CREATE TRIGGER immutable_attempt_identity BEFORE UPDATE OF workspace_id,job_id,attempt_id,number,nonce,created_at ON attempts BEGIN SELECT RAISE(ABORT,'immutable attempt identity'); END;
CREATE TRIGGER immutable_referenced_object BEFORE UPDATE ON objects WHEN EXISTS(SELECT 1 FROM job_objects WHERE workspace_id=OLD.workspace_id AND object_id=OLD.object_id) BEGIN SELECT RAISE(ABORT,'referenced object is immutable'); END;
