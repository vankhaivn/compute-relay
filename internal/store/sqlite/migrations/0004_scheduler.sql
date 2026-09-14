-- Queue order is allocated inside admission, not reconstructed from mutable clocks.
CREATE TABLE scheduler_control (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 settings TEXT NOT NULL CHECK(length(settings)<=4096),
 last_workspace TEXT NOT NULL DEFAULT '',
 clock_ms INTEGER NOT NULL DEFAULT 0 CHECK(clock_ms>=0)
) STRICT;
-- Opening/migrating a database never starts dispatch. Local composition must opt in.
INSERT INTO scheduler_control(singleton,settings) VALUES(1,
 '{"paused":true,"max_workers":4,"max_active_per_account":1,"lease_duration_ns":30000000000,"quota_fresh_for_ns":300000000000,"blocked_recheck_ns":300000000000}');
CREATE TABLE scheduler_queue (
 queue_seq INTEGER PRIMARY KEY AUTOINCREMENT,
 workspace_id TEXT NOT NULL,
 job_id TEXT NOT NULL,
 attempt_id TEXT NOT NULL,
 dispatch_barrier INTEGER NOT NULL DEFAULT 0 CHECK(dispatch_barrier IN (0,1)),
 not_before_ms INTEGER NOT NULL DEFAULT 0 CHECK(not_before_ms>=0),
 UNIQUE(workspace_id,job_id,attempt_id),
 FOREIGN KEY(workspace_id,job_id,attempt_id) REFERENCES attempts(workspace_id,job_id,attempt_id)
) STRICT;
-- M3-02 jobs were inserted serially. Preserve insertion order on upgrade, with a
-- stable attempt-number tie-breaker; never change existing request/attempt identities.
INSERT INTO scheduler_queue(workspace_id,job_id,attempt_id,dispatch_barrier)
 SELECT a.workspace_id,a.job_id,a.attempt_id,
 CASE WHEN a.orchestration NOT IN ('queued','preparing','blocked') OR
 json_extract(a.state,'$.Execution')!='not_submitted' OR
 json_extract(a.state,'$.RemoteActivity')!='not_started' THEN 1 ELSE 0 END
 FROM attempts a JOIN jobs j ON j.workspace_id=a.workspace_id AND j.job_id=a.job_id
 ORDER BY j.rowid,a.number;
CREATE TRIGGER scheduler_enqueue AFTER INSERT ON attempts BEGIN
 INSERT INTO scheduler_queue(workspace_id,job_id,attempt_id,dispatch_barrier)
 VALUES(NEW.workspace_id,NEW.job_id,NEW.attempt_id,
 CASE WHEN NEW.orchestration NOT IN ('queued','preparing','blocked') OR
 json_extract(NEW.state,'$.Execution')!='not_submitted' OR
 json_extract(NEW.state,'$.RemoteActivity')!='not_started' THEN 1 ELSE 0 END);
END;
-- Once an attempt leaves local preparation it can never become automatic new work.
-- This conservative latch is NOT the M3-04 submission-intent ledger.
CREATE TRIGGER scheduler_barrier AFTER UPDATE OF state,orchestration ON attempts
 WHEN NEW.orchestration NOT IN ('queued','preparing','blocked') OR
 json_extract(NEW.state,'$.Execution')!='not_submitted' OR
 json_extract(NEW.state,'$.RemoteActivity')!='not_started'
 BEGIN UPDATE scheduler_queue SET dispatch_barrier=1
 WHERE workspace_id=NEW.workspace_id AND job_id=NEW.job_id AND attempt_id=NEW.attempt_id; END;
CREATE TRIGGER scheduler_identity BEFORE UPDATE OF queue_seq,workspace_id,job_id,attempt_id ON scheduler_queue
 BEGIN SELECT RAISE(ABORT,'immutable queue identity'); END;
CREATE TRIGGER scheduler_barrier_monotonic BEFORE UPDATE OF dispatch_barrier ON scheduler_queue
 WHEN OLD.dispatch_barrier=1 AND NEW.dispatch_barrier=0
 BEGIN SELECT RAISE(ABORT,'dispatch barrier cannot be cleared'); END;
CREATE INDEX scheduler_workspace_order ON scheduler_queue(workspace_id,queue_seq);
CREATE TABLE scheduler_leases (
 queue_seq INTEGER PRIMARY KEY REFERENCES scheduler_queue(queue_seq),
 generation INTEGER NOT NULL CHECK(generation>0),
 owner TEXT NOT NULL,
 fence TEXT NOT NULL,
 until_ms INTEGER NOT NULL CHECK(until_ms>=0),
 held INTEGER NOT NULL CHECK(held IN (0,1)),
 CHECK((held=1 AND length(owner) BETWEEN 1 AND 128 AND length(fence)=64 AND until_ms>0)
 OR (held=0 AND owner='' AND fence='' AND until_ms=0))
) STRICT;
CREATE TRIGGER scheduler_generation BEFORE UPDATE OF generation ON scheduler_leases
 WHEN NEW.generation<=OLD.generation
 BEGIN SELECT RAISE(ABORT,'lease generation must increase'); END;
CREATE TABLE scheduler_accounts (
 account_scope TEXT PRIMARY KEY,
 policy TEXT NOT NULL CHECK(length(policy)<=2048)
) STRICT;
CREATE TABLE scheduler_quotas (
 account_scope TEXT NOT NULL,
 resource TEXT NOT NULL CHECK(resource IN ('cpu','gpu')),
 observation TEXT NOT NULL CHECK(length(observation)<=4096),
 observed_ms INTEGER NOT NULL CHECK(observed_ms>=0),
 exhausted INTEGER NOT NULL CHECK(exhausted IN (0,1)),
 PRIMARY KEY(account_scope,resource)
) STRICT;
