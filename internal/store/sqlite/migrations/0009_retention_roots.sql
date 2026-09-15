-- Bind deletion destinations before any sweep. A replacement/swapped blob root
-- cannot inherit pending deletion authority from its pathname. Roots are explicit
-- local composition dependencies, not application-selected directories.
CREATE TABLE retention_roots (
 kind TEXT PRIMARY KEY CHECK(kind IN ('input','result')),
 blob_identity TEXT NOT NULL UNIQUE CHECK(length(blob_identity) BETWEEN 1 AND 128)
) STRICT;
CREATE TRIGGER retention_root_immutable BEFORE UPDATE ON retention_roots
 BEGIN SELECT RAISE(ABORT,'retention root identity cannot be remapped'); END;
CREATE TRIGGER retention_root_keep BEFORE DELETE ON retention_roots
 BEGIN SELECT RAISE(ABORT,'retention root binding cannot be forgotten'); END;
