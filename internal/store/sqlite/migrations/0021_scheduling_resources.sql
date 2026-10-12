-- Login accounts remain immutable. A separate verified association shares machine
-- capacity across connection paths without rewriting accepted jobs or reservations.
CREATE TABLE managed_resource_accounts (
 account_scope TEXT PRIMARY KEY,
 resource_kind TEXT NOT NULL CHECK(length(resource_kind) BETWEEN 1 AND 128),
 resource_scope TEXT NOT NULL DEFAULT '' CHECK(resource_scope='' OR length(resource_scope)=73)
) STRICT;
CREATE INDEX managed_resource_members ON managed_resource_accounts(resource_scope,account_scope);
CREATE TRIGGER managed_resource_identity BEFORE UPDATE ON managed_resource_accounts
 WHEN NEW.account_scope!=OLD.account_scope OR NEW.resource_kind!=OLD.resource_kind OR
      (OLD.resource_scope!='' AND NEW.resource_scope!=OLD.resource_scope)
 BEGIN SELECT RAISE(ABORT,'verified scheduling resource identity is immutable'); END;
CREATE TRIGGER managed_resource_retained BEFORE DELETE ON managed_resource_accounts
 BEGIN SELECT RAISE(ABORT,'scheduling resource association is retained'); END;

-- A legacy job claimed after verification records its intended resource before
-- preparation. Pre-upgrade active work has no such evidence and stays unresolved
-- until confirmed inactive; a metadata recheck cannot relocate that old work.
CREATE TABLE scheduler_resource_reservations (
 queue_seq INTEGER PRIMARY KEY REFERENCES scheduler_queue(queue_seq),
 resource_kind TEXT NOT NULL CHECK(length(resource_kind) BETWEEN 1 AND 128),
 resource_scope TEXT NOT NULL CHECK(length(resource_scope)=73)
) STRICT;
CREATE TRIGGER scheduler_resource_reservation_identity BEFORE UPDATE ON scheduler_resource_reservations
 BEGIN SELECT RAISE(ABORT,'immutable attempt scheduling resource'); END;

-- Endpoint-derived SSH accounts cannot prove distinct capacity. Recheck supplies
-- native machine identity; until then old reservations remain unresolved and counted.
INSERT INTO managed_resource_accounts(account_scope,resource_kind)
 SELECT DISTINCT account_scope,'machine' FROM managed_connections
 WHERE provider_type='ssh' AND account_scope!='';
UPDATE profiles SET enabled=0 WHERE profile IN
 (SELECT current_profile FROM managed_connections WHERE provider_type='ssh');
UPDATE managed_connections SET current_profile='' WHERE provider_type='ssh';
