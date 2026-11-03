-- Captured from real dbt BUILD using commit 3907d3c (ticket #5).
-- DuckDB 1.4.1; format 1 predates durable invocation markers.
-- Replay literal historical DDL/rows, never current metadata constructors.
CREATE SCHEMA IF NOT EXISTS _dbt_state;
CREATE TABLE _dbt_state.database_identity(id INTEGER PRIMARY KEY, "version" INTEGER NOT NULL, generation VARCHAR NOT NULL, CHECK((id = 1)));
INSERT INTO _dbt_state.database_identity VALUES (1, 1, '82a6b6d8-f30f-46b0-ae48-b84ce1f72970');
CREATE TABLE _dbt_state.evidence_clock(id INTEGER PRIMARY KEY, "version" INTEGER NOT NULL, ordinal BIGINT NOT NULL, CHECK((id = 1)));
INSERT INTO _dbt_state.evidence_clock VALUES (1, 1, 2);
CREATE TABLE _dbt_state.relation_evidence("name" VARCHAR PRIMARY KEY, "version" INTEGER NOT NULL, schema_hash VARCHAR NOT NULL, content_hash VARCHAR NOT NULL, ordinal BIGINT NOT NULL);
INSERT INTO _dbt_state.relation_evidence VALUES ('"warehouse"."dev"."numbers"', 1, '70b380e5e9fe0eb2a7368ae4d28f818acc89fc31f6810182198c5231f8d57647', '08a8d41e3f6fe1d1aefb8937a67cbf4fa7bd5ebbbd14f721c603d09016910cdf', 1);
INSERT INTO _dbt_state.relation_evidence VALUES ('"warehouse"."dev"."totals"', 1, '267b03fdea69af6f750b4d46b592d3d6a2e670570db065a98a48703b42833f9e', 'ff55f6e0293ac8e01d8a2506b4775167c81e39cf34be83adf19a08d5cca4a3b9', 2);
