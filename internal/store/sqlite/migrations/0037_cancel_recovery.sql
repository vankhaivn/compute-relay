-- Explicit requests may recover terminal failures before the provider stop began.
-- Preserve every operation and receipt, and enforce one stop invocation per attempt
-- across the entire history, independently of request count or worker restarts.
DROP INDEX operation_one_cancel;
CREATE UNIQUE INDEX operation_cancel_invoked ON operations(workspace_id,job_id,attempt_id)
 WHERE kind='cancel' AND cancel_started=1;
