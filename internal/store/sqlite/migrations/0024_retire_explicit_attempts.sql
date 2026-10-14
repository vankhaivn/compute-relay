-- Explicit provider mode has no runtime composition after this upgrade. Preserve
-- every frozen receipt and remote fact while preventing all automatic worker I/O.
CREATE TABLE retired_explicit_attempts (
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 reason TEXT NOT NULL CHECK(reason='explicit_mode_retired'),
 PRIMARY KEY(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
INSERT INTO retired_explicit_attempts
 SELECT a.workspace_id,a.job_id,a.attempt_id,'explicit_mode_retired'
 FROM attempts a JOIN jobs j ON j.workspace_id=a.workspace_id AND j.job_id=a.job_id
 JOIN profile_revisions p ON p.profile=j.profile AND p.revision=j.profile_revision
 WHERE COALESCE(json_extract(p.snapshot,'$.credential_ref'),'') NOT LIKE 'vault:%';

-- Work with no mutation barrier fails locally; other unfinished attempts need
-- attention. Neither outcome is evidence that execution stopped, results
-- vanished, or capacity was released. All execution/result/cancellation/release
-- fields, timestamps, plans, intents, nonces and provider resources are retained.
UPDATE attempts SET state=json_set(state,'$.Orchestration',CASE
 WHEN json_extract(state,'$.Execution')='not_submitted' AND json_extract(state,'$.RemoteActivity')='not_started'
 AND NOT EXISTS(SELECT 1 FROM scheduler_queue q WHERE q.workspace_id=attempts.workspace_id
 AND q.job_id=attempts.job_id AND q.attempt_id=attempts.attempt_id AND q.dispatch_barrier=1)
 THEN 'failed' ELSE 'needs_attention' END),
 orchestration=CASE
 WHEN json_extract(state,'$.Execution')='not_submitted' AND json_extract(state,'$.RemoteActivity')='not_started'
 AND NOT EXISTS(SELECT 1 FROM scheduler_queue q WHERE q.workspace_id=attempts.workspace_id
 AND q.job_id=attempts.job_id AND q.attempt_id=attempts.attempt_id AND q.dispatch_barrier=1)
 THEN 'failed' ELSE 'needs_attention' END,revision=revision+1
 WHERE orchestration NOT IN ('succeeded','failed','cancelled','timed_out')
 AND EXISTS(SELECT 1 FROM retired_explicit_attempts r WHERE r.workspace_id=attempts.workspace_id
 AND r.job_id=attempts.job_id AND r.attempt_id=attempts.attempt_id);
UPDATE scheduler_leases SET held=0
 WHERE held=1 AND queue_seq IN (SELECT q.queue_seq FROM scheduler_queue q JOIN retired_explicit_attempts r
 ON r.workspace_id=q.workspace_id AND r.job_id=q.job_id AND r.attempt_id=q.attempt_id);
UPDATE collection_leases SET held=0
 WHERE held=1 AND EXISTS(SELECT 1 FROM retired_explicit_attempts r WHERE r.workspace_id=collection_leases.workspace_id
 AND r.job_id=collection_leases.job_id AND r.attempt_id=collection_leases.attempt_id);
UPDATE profiles SET enabled=0
 WHERE EXISTS(SELECT 1 FROM profile_revisions r WHERE r.profile=profiles.profile AND r.revision=profiles.revision
 AND COALESCE(json_extract(r.snapshot,'$.credential_ref'),'') NOT LIKE 'vault:%');

-- Pending local operations now have an explicit terminal outcome. Their original
-- idempotency receipts, one-shot intent flags and frozen targets remain intact.
CREATE TEMP VIEW retiring_explicit_problems AS
 SELECT r.workspace_id,r.job_id,r.attempt_id,
 json_object('Code','CONFIGURATION_INVALID',
 'Message','Explicit provider mode was retired; inspect the retained original provider evidence.',
 'Stage','local_runtime','SafeOperationRetry',json('false'),
 'ComputeMayHaveStarted',json(CASE WHEN json_extract(a.state,'$.Execution')!='not_submitted'
 OR json_extract(a.state,'$.RemoteActivity') NOT IN ('not_started','inactive') THEN 'true' ELSE 'false' END),
 'RecommendedAction','inspect_provider','Details',json_object('reason','explicit_mode_retired')) AS problem
 FROM retired_explicit_attempts r JOIN attempts a
 ON a.workspace_id=r.workspace_id AND a.job_id=r.job_id AND a.attempt_id=r.attempt_id;
UPDATE operations SET status='failed',revision=revision+1,
 record=json_set(record,'$.Operation.Status','failed','$.Operation.Revision',revision+1,
 '$.Operation.Failure',json((SELECT problem FROM retiring_explicit_problems r
 WHERE r.workspace_id=operations.workspace_id AND r.job_id=operations.job_id AND r.attempt_id=operations.attempt_id)),
 '$.Effect','failed')
 WHERE status IN ('accepted','running') AND EXISTS(SELECT 1 FROM retired_explicit_attempts r
 WHERE r.workspace_id=operations.workspace_id AND r.job_id=operations.job_id AND r.attempt_id=operations.attempt_id);
UPDATE cleanup_resources SET state=CASE state WHEN 'running' THEN 'unknown' ELSE 'retained' END,
 reason=CASE state WHEN 'running' THEN 'prior_delete_outcome_unknown' ELSE 'explicit_mode_retired' END
 WHERE state IN ('pending','running') AND EXISTS(SELECT 1 FROM cleanup_operations c JOIN retired_explicit_attempts r
 ON r.workspace_id=c.workspace_id AND r.job_id=c.job_id AND r.attempt_id=c.attempt_id
 WHERE c.workspace_id=cleanup_resources.workspace_id AND c.operation_id=cleanup_resources.operation_id);
UPDATE cleanup_operations SET status='failed',revision=revision+1,
 record=json_set(record,'$.Operation.Status','failed','$.Operation.Revision',revision+1,
 '$.Operation.Failure',json((SELECT problem FROM retiring_explicit_problems r
 WHERE r.workspace_id=cleanup_operations.workspace_id AND r.job_id=cleanup_operations.job_id AND r.attempt_id=cleanup_operations.attempt_id)),
 '$.Effect','failed')
 WHERE status IN ('accepted','running') AND EXISTS(SELECT 1 FROM retired_explicit_attempts r
 WHERE r.workspace_id=cleanup_operations.workspace_id AND r.job_id=cleanup_operations.job_id AND r.attempt_id=cleanup_operations.attempt_id);
DROP VIEW retiring_explicit_problems;
