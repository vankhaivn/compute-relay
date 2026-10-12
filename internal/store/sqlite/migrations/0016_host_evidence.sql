-- Public evidence is retained after a failed recheck; it is not current availability.
ALTER TABLE managed_connections ADD COLUMN host_evidence BLOB NOT NULL DEFAULT X''
 CHECK(length(host_evidence)<=1024);
