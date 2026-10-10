-- Preparation samples observe provider staging uploads; they never change journals,
-- receipts or attempt identity and are not readiness evidence.
CREATE TABLE preparation_progress (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0),
 progress TEXT NOT NULL CHECK(length(progress)<=1024 AND json_valid(progress)),
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
