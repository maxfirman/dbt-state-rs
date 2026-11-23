-- #264 (ADR 0007 "Result copy over different inputs"): a Snowflake build's
-- physical binding identity, its SQL and query dependency definitions with
-- every relation reference resolved to the physical relation it read. A
-- Result copy qualifies only when the submit, each copy it reads taken as
-- its source, has the identity of the build the candidate holds. NULL for
-- other Warehouses, for SQL whose references cannot be resolved, and for
-- every row recorded before this migration: such a build is no copy source,
-- and a copy of it is rebuilt once.
ALTER TABLE executions ADD COLUMN node_binding_hash TEXT;
