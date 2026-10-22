ALTER TABLE managed_connection_operations ADD COLUMN cause TEXT
 CHECK(cause IS NULL OR (status='failed' AND length(cause) BETWEEN 1 AND 64
  AND substr(cause,1,1) GLOB '[a-z]' AND cause NOT GLOB '*[^a-z0-9_]*'));
