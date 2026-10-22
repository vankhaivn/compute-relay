-- A shared object remains charged even when its creation outcome is unknown.
-- This slice never deletes shared objects or releases their reservations.
CREATE TABLE shared_resource_claims (
 installation_id TEXT NOT NULL,
 account_scope TEXT NOT NULL,
 resource_key TEXT NOT NULL CHECK(length(resource_key) BETWEEN 66 AND 85),
 content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64),
 bytes INTEGER NOT NULL CHECK(bytes>=0),
 preparation_id TEXT NOT NULL REFERENCES provider_resources(operation_id),
 identity TEXT NOT NULL CHECK(length(identity)<=4096 AND json_valid(identity)),
 plan_sha256 TEXT NOT NULL CHECK(length(plan_sha256)=64),
 creation_started INTEGER NOT NULL DEFAULT 0 CHECK(creation_started IN (0,1)),
 legacy INTEGER NOT NULL DEFAULT 0 CHECK(legacy IN (0,1)),
 created_at TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_scope,resource_key),
 CHECK(resource_key=content_sha256||'_'||CAST(bytes AS TEXT))
) STRICT;

CREATE TRIGGER shared_resource_identity BEFORE UPDATE OF installation_id,account_scope,resource_key,content_sha256,bytes,preparation_id,identity,plan_sha256,legacy,created_at ON shared_resource_claims
 BEGIN SELECT RAISE(ABORT,'immutable shared resource reservation'); END;
CREATE TRIGGER shared_resource_creation_once BEFORE UPDATE OF creation_started ON shared_resource_claims
 WHEN OLD.creation_started!=0 OR NEW.creation_started!=1
 BEGIN SELECT RAISE(ABORT,'shared creation is one shot'); END;
CREATE TRIGGER shared_resource_keep BEFORE DELETE ON shared_resource_claims
 BEGIN SELECT RAISE(ABORT,'shared resource reservation cannot be forgotten'); END;

-- Shipped managed Kaggle staging shares inputs >=256 MiB. Older inline layouts
-- may contain the same candidates: charge them conservatively rather than infer
-- that an unacknowledged shared creation never happened. Plans stay unchanged.
INSERT INTO shared_resource_claims
 (installation_id,account_scope,resource_key,content_sha256,bytes,preparation_id,identity,plan_sha256,creation_started,legacy,created_at)
 SELECT installation_id,account_scope,sha||'_'||CAST(bytes AS TEXT),sha,bytes,operation_id,identity,plan_sha256,1,1,created_at
 FROM (
 SELECT json_extract(r.identity,'$.InstallationID') AS installation_id,r.account_scope,
 json_extract(i.value,'$.object.sha256') AS sha,json_extract(i.value,'$.object.bytes') AS bytes,
 r.operation_id,r.identity,r.plan_sha256,r.created_at,
 row_number() OVER (PARTITION BY json_extract(r.identity,'$.InstallationID'),r.account_scope,
 json_extract(i.value,'$.object.sha256'),json_extract(i.value,'$.object.bytes') ORDER BY r.created_at,r.operation_id) AS ordinal
 FROM provider_resources r
 JOIN managed_connections c ON c.connection_id=r.instance_id AND c.provider_type='kaggle'
 JOIN scheduler_queue q ON q.workspace_id=r.workspace_id AND q.job_id=r.job_id AND q.attempt_id=r.attempt_id
 JOIN dispatch_journals j ON j.queue_seq=q.queue_seq
 JOIN json_each(j.journal,'$.plan.Job.Inputs.inputs') i
 WHERE r.purpose='staging' AND json_extract(i.value,'$.object.bytes')>=268435456
 ) WHERE ordinal=1;

CREATE INDEX provider_resource_account_count ON provider_resources(account_scope,purpose,created_at);
CREATE INDEX confirmed_resource_cleanup ON cleanup_resources(resource_id,state);
