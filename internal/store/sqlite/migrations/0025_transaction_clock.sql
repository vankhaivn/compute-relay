-- Historical callers wrote full-precision UTC timestamps while clock_ms truncated
-- them. Raise only the logical clock, preserving every original timestamp, receipt,
-- deadline and provider observation. Future policy/evidence times are excluded.
WITH local_stamps(stamp) AS (
 SELECT created_at FROM api_token_hashes
 UNION ALL SELECT created_at FROM jobs
 UNION ALL SELECT created_at FROM attempts
 UNION ALL SELECT updated_at FROM attempts
 UNION ALL SELECT occurred_at FROM events
 UNION ALL SELECT created_at FROM provider_resources
 UNION ALL SELECT created_at FROM submission_intents
 UNION ALL SELECT created_at FROM operations
 UNION ALL SELECT json_extract(record,'$.Operation.UpdatedAt') FROM operations
 UNION ALL SELECT created_at FROM cleanup_operations
 UNION ALL SELECT json_extract(record,'$.Operation.UpdatedAt') FROM cleanup_operations
 UNION ALL SELECT pinned_at FROM collection_snapshots
 UNION ALL SELECT verified_at FROM collection_publications
 UNION ALL SELECT recorded_at FROM retention_inventory
 UNION ALL SELECT expired_at FROM retention_inventory
 UNION ALL SELECT deleted_at FROM retention_inventory
 UNION ALL SELECT expired_at FROM retention_expirations
 UNION ALL SELECT updated_at FROM managed_connections
 UNION ALL SELECT created_at FROM managed_connection_operations
 UNION ALL SELECT updated_at FROM managed_connection_operations
 UNION ALL SELECT created_at FROM managed_credential_generations
 UNION ALL SELECT created_at FROM execution_authorizations
 UNION ALL SELECT consumed_at FROM execution_authorizations
 UNION ALL SELECT json_extract(original_receipt,'$.created_at') FROM preparation_authorizations
 UNION ALL SELECT consumed_at FROM preparation_authorizations
 UNION ALL SELECT ready_at FROM preparation_authorizations
 UNION ALL SELECT json_extract(original_receipt,'$.created_at') FROM start_authorizations
 UNION ALL SELECT consumed_at FROM start_authorizations
 UNION ALL SELECT json_extract(original_receipt,'$.created_at') FROM workload_secret_authorizations
 UNION ALL SELECT consumed_at FROM workload_secret_authorizations
 UNION ALL SELECT revoked_at FROM workload_secret_authorizations
), ceilings(value) AS (
 -- Split the fraction before strftime: SQLite's date parser rounds fractions.
 -- Stored local stamps use RFC3339Nano UTC with zero to nine fractional digits.
 SELECT CAST(strftime('%s',substr(stamp,1,19)||'Z') AS INTEGER)*1000
  + CAST(substr(substr(stamp,21,max(length(stamp)-21,0))||'000',1,3) AS INTEGER)
  + CASE WHEN length(stamp)>24 AND CAST(substr(stamp,24,length(stamp)-24) AS INTEGER)>0 THEN 1 ELSE 0 END
 FROM local_stamps WHERE stamp IS NOT NULL
)
-- The prior clock may itself have had an unrecorded fractional millisecond, for
-- example a quota-only write. Bound that fraction without adopting observation time.
UPDATE scheduler_control SET clock_ms=max(clock_ms+CASE WHEN clock_ms>0 THEN 1 ELSE 0 END,COALESCE((SELECT max(value) FROM ceilings),0))
 WHERE singleton=1;

-- Repositories register retained bytes using their one sampled transaction time.
-- These guards preserve tracking even if an insertion bypasses repository code.
DROP TRIGGER retention_object_insert;
DROP TRIGGER retention_artifact_insert;
CREATE TRIGGER retention_object_insert BEFORE INSERT ON objects
 WHEN NOT EXISTS(SELECT 1 FROM retention_inventory r WHERE r.kind='input'
 AND r.workspace_id=NEW.workspace_id AND r.object_id=NEW.object_id
 AND r.bytes=NEW.bytes AND r.sha256=NEW.sha256 AND r.expired_at IS NULL)
 BEGIN SELECT RAISE(ABORT,'object retention registration required'); END;
CREATE TRIGGER retention_artifact_insert BEFORE INSERT ON artifacts
 WHEN NOT EXISTS(SELECT 1 FROM retention_inventory r WHERE r.kind='result'
 AND r.workspace_id=NEW.workspace_id AND r.object_id=NEW.object_id
 AND r.bytes=NEW.bytes AND r.sha256=NEW.sha256 AND r.expired_at IS NULL)
 BEGIN SELECT RAISE(ABORT,'artifact retention registration required'); END;
