//! Warehouse-specific interpretation of the dialect string on the wire.
//!
//! A parser accepting a dialect is not evidence that two SQL statements or
//! physical relations are equivalent. Only Snowflake's schema abstraction has
//! been characterized against the hosted service. Other warehouses retain exact
//! rendered SQL, except DuckDB's validated formatting subset. DuckDB copying
//! uses separate request eligibility rather than this warehouse-wide capability.
//!
//! Warehouse-specific rules dispatch to the per-warehouse submodules
//! ([`snowflake`], [`duckdb`]); the other variants get conservative defaults
//! (ADR 0004: shared machinery with explicit warehouse rules).

mod duckdb;
mod snowflake;

/// The strict SQL identity's version tag for warehouses without their own.
const SQL_IDENTITY_VERSION: &[u8] = b"dbt-state-sql-v4";

/// The exact namespace identity's version tag.
const EXACT_NAMESPACE_IDENTITY_VERSION: &[u8] = b"dbt-state-exact-ns-v1";

/// The namespace logic identity a warehouse records
/// ([`Warehouse::namespace_identity`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceIdentity {
    /// The strict identity under its own tag: no cross-environment
    /// equivalence.
    Exact { version: &'static [u8] },
    /// Physical schema qualifiers are abstracted, so the same model built in
    /// two environments shares one identity.
    SchemaAbstracted { version: &'static [u8] },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Warehouse {
    Snowflake,
    DuckDb,
    BigQuery,
    Databricks,
    Redshift,
    Other,
}

impl Warehouse {
    pub fn from_dialect(dialect: &str) -> Self {
        // dbt can append dialect options, for example
        // "redshift, normalization_strategy = lowercase". Keep the full wire
        // string in fingerprints. Uncharacterized options cannot inherit the
        // Snowflake reuse rules merely because the family name matches.
        let trimmed = dialect.trim();
        let family = trimmed
            .split(',')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        match family.as_str() {
            "snowflake" if trimmed.eq_ignore_ascii_case("snowflake") => Self::Snowflake,
            "duckdb" if trimmed.eq_ignore_ascii_case("duckdb") => Self::DuckDb,
            "bigquery" if trimmed.eq_ignore_ascii_case("bigquery") => Self::BigQuery,
            "databricks" | "spark"
                if trimmed.eq_ignore_ascii_case("databricks")
                    || trimmed.eq_ignore_ascii_case("spark") =>
            {
                Self::Databricks
            }
            "redshift" => Self::Redshift,
            _ => Self::Other,
        }
    }

    pub fn family_key(self, dialect: &str) -> String {
        match self {
            Self::Snowflake => "snowflake".into(),
            Self::DuckDb => "duckdb".into(),
            Self::BigQuery => "bigquery".into(),
            Self::Databricks => "databricks".into(),
            Self::Redshift => "redshift".into(),
            Self::Other => dialect
                .split(',')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase(),
        }
    }

    pub fn normalized_sql(self, sql: &str) -> String {
        crate::sql_identity::normalized_sql(self, sql)
    }

    /// Whether the namespace logic identity abstracts physical schemas, so
    /// the same model can be reused across environments.
    pub fn supports_namespace_reuse(self) -> bool {
        matches!(
            self.namespace_identity(),
            NamespaceIdentity::SchemaAbstracted { .. }
        )
    }

    // ── Fingerprint version tags ────────────────────────────────────────────
    //
    // Each tag leads its fingerprint. A change to a warehouse's identity
    // semantics takes a new tag, so older history cold-misses instead of being
    // reinterpreted (ADR 0004).

    /// The version tag of the strict SQL identity, in both rendered and
    /// template mode.
    pub fn sql_identity_version(self) -> &'static [u8] {
        match self {
            Self::DuckDb => duckdb::SQL_IDENTITY_VERSION,
            _ => SQL_IDENTITY_VERSION,
        }
    }

    /// How the namespace (cross-environment) logic identity reads this
    /// warehouse's SQL, and its version tag.
    pub fn namespace_identity(self) -> NamespaceIdentity {
        match self {
            Self::Snowflake => snowflake::NAMESPACE_IDENTITY,
            _ => NamespaceIdentity::Exact {
                version: EXACT_NAMESPACE_IDENTITY_VERSION,
            },
        }
    }

    pub fn has_volatile_sql<'a>(
        self,
        root: &str,
        dependencies: impl IntoIterator<Item = &'a str>,
    ) -> bool {
        match self {
            Self::Snowflake => crate::sql_norm::request_has_volatile_sql(root, dependencies),
            // Until each dialect has a checked parser and volatile-function set,
            // suppress the SQL-submit clone fallback. A normal build remains
            // available and can still be reused on a later strict match.
            _ => true,
        }
    }

    // ── Result copy rules ───────────────────────────────────────────────────
    //
    // A result copy populates a target from an existing result instead of
    // executing its transformation (see `result_copy`). Warehouses without
    // their own rules get conservative defaults: no copy unless DDL exists,
    // no relation or provenance requirements beyond it.

    /// Whether a SQL submission can take part in a result copy, as source or
    /// target, and the identity that pairs it with an equivalent. `None`: it
    /// is never copied. Eligibility is explicit: formatting validation alone
    /// never grants copying.
    pub(crate) fn copy_identity(
        self,
        req: &crate::query_cache::SqlExecution,
    ) -> Option<CopyIdentity> {
        match self {
            Self::Snowflake => Some(CopyIdentity::NamespaceLogic),
            Self::DuckDb => duckdb::copy_identity(req),
            _ => None,
        }
    }

    /// The statements that copy `source` into `target`, or why no copy can be
    /// produced. `source_table_type` is the warehouse-reported kind of the
    /// source. Requested retention options and view sources are refused for
    /// every warehouse so the client builds normally instead of silently
    /// creating a copy unlike the requested target.
    pub fn result_copy_ddl(
        self,
        source: &str,
        target: &str,
        source_table_type: Option<&str>,
        properties: Option<&crate::query_cache::TableProperties>,
    ) -> Result<Vec<String>, &'static str> {
        if properties.is_some_and(|p| {
            p.hours_to_expiration.is_some() || p.partition_expiration_days.is_some()
        }) {
            return Err("clone table properties are not implemented");
        }
        if source_table_type.is_some_and(|kind| kind.to_ascii_uppercase().contains("VIEW")) {
            return Err("cloning view sources is not implemented");
        }
        match self {
            Self::Snowflake => Ok(snowflake::copy_ddl(source, target, source_table_type)),
            Self::DuckDb => duckdb::copy_ddl(source, target, source_table_type),
            Self::BigQuery => {
                // GoogleSQL's CREATE TABLE ... CLONE syntax has no OR REPLACE form.
                Ok(vec![format!("CREATE TABLE {target} CLONE {source}")])
            }
            Self::Databricks => {
                // Managed Iceberg supports DEEP but not SHALLOW CLONE. A generic
                // "TABLE" type does not identify the storage format, so only an
                // explicitly Delta source can use this unverified template.
                let is_delta = source_table_type
                    .is_some_and(|kind| kind.to_ascii_uppercase().contains("DELTA"));
                if !is_delta {
                    return Err("Databricks shallow clone requires a known Delta source");
                }
                Ok(vec![format!(
                    "CREATE TABLE {target} SHALLOW CLONE {source}"
                )])
            }
            Self::Redshift | Self::Other => Err("clone DDL is not implemented for this dialect"),
        }
    }

    /// Whether `source` and `target` can be the two ends of one copy. The
    /// clone-equivalent lookup skips candidates that fail this; the DDL
    /// applies the same check.
    pub fn copy_relations_compatible(self, source: &str, target: &str) -> bool {
        match self {
            Self::DuckDb => duckdb::copy_relations_compatible(source, target),
            _ => true,
        }
    }

    /// The copy-chain depth at which a submit stops copying an equivalent and
    /// executes instead (the chain-limit Execute), given the limit the client
    /// requested. `None`: this warehouse does not limit copy chains.
    pub fn copy_chain_depth_limit(self, requested: Option<i64>) -> Option<usize> {
        match self {
            Self::DuckDb => Some(duckdb::copy_chain_depth_limit(requested)),
            _ => None,
        }
    }

    /// `Some(max)`: the clone-equivalent lookup must walk each candidate's
    /// copy provenance (a store read), and a candidate with more than `max`
    /// copy ancestors is ineligible. `None`: copy depth is not tracked.
    pub fn copy_provenance_depth(self) -> Option<usize> {
        match self {
            Self::DuckDb => Some(duckdb::copy_provenance_depth()),
            _ => None,
        }
    }

    /// How many current equivalents the clone-equivalent lookup returns, in
    /// its preference order. `None`: every candidate, because candidates can
    /// still be rejected after the read (relation check, provenance walk).
    pub fn copy_candidate_limit(self) -> Option<i64> {
        match self {
            Self::DuckDb => None,
            _ => Some(1),
        }
    }

    /// Physical evidence the submit fallback requires of a copy source beyond
    /// its DDL: the source's recorded build epoch, if any.
    pub fn copy_source_evidence(
        self,
        last_modified_epoch: Option<i64>,
    ) -> Result<(), &'static str> {
        match self {
            Self::DuckDb => duckdb::copy_source_evidence(last_modified_epoch),
            _ => Ok(()),
        }
    }

    /// Whether the submit fallback skips its volatile-SQL gate for a
    /// submission that has a copy logic identity. DuckDB computes that
    /// identity only for its checked copy subset.
    ///
    /// `dialect` is the wire spelling. TODO(#50): only the exact lowercase
    /// spelling `duckdb` qualifies, unlike every other DuckDB rule; this
    /// case-sensitive quirk is preserved until the dialect-matching ticket.
    pub fn copy_identity_skips_volatility_gate(self, dialect: &str) -> bool {
        match self {
            Self::DuckDb => duckdb::copy_identity_skips_volatility_gate(dialect),
            _ => false,
        }
    }

    /// Whether `values` (submission semantic extras or copy request labels)
    /// declare this warehouse's pinned incremental contract.
    pub fn declares_incremental_contract(
        self,
        values: &std::collections::HashMap<String, String>,
    ) -> bool {
        match self {
            Self::DuckDb => duckdb::declares_incremental_contract(values),
            _ => false,
        }
    }

    /// What a client-requested copy must declare about its source before any
    /// history is read.
    pub fn incremental_copy_contract(
        self,
        req: &crate::query_cache::CloneRequest,
    ) -> IncrementalCopyContract<'_> {
        match self {
            Self::DuckDb => duckdb::incremental_copy_contract(req),
            _ => IncrementalCopyContract::NotRequired,
        }
    }
}

/// A submission's copy logic identity ([`Warehouse::copy_identity`]), which
/// `fingerprint::copy_logic_hash` frames.
pub(crate) enum CopyIdentity {
    /// The namespace logic identity ([`Warehouse::namespace_identity`]).
    NamespaceLogic,
    /// A checked copy graph, framed under `version`.
    CopyGraph {
        version: &'static [u8],
        graph: crate::sql_identity::DuckDbCopyGraph,
    },
}

/// A client-requested copy's incremental contract
/// ([`Warehouse::incremental_copy_contract`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalCopyContract<'a> {
    /// This warehouse copies without one.
    NotRequired,
    /// Required but not declared: the copy is refused.
    Missing,
    /// Declared. The source must currently be the confirmed incremental build
    /// with this initialization identity.
    Declared { identity: &'a str },
}

#[cfg(test)]
mod tests {
    use super::{CopyIdentity, IncrementalCopyContract, NamespaceIdentity, Warehouse};

    #[test]
    fn redshift_options_keep_one_physical_family() {
        let configured = "redshift, normalization_strategy = lowercase";
        assert_eq!(Warehouse::from_dialect(configured), Warehouse::Redshift);
        assert_eq!(
            Warehouse::from_dialect(configured).family_key(configured),
            "redshift"
        );
    }

    #[test]
    fn unverified_dialects_do_not_abstract_or_clone_sql() {
        for dialect in [
            "bigquery",
            "databricks",
            "redshift",
            "unknown",
            "snowflake, mode=x",
        ] {
            let warehouse = Warehouse::from_dialect(dialect);
            assert!(!warehouse.supports_namespace_reuse());
            assert_eq!(warehouse.normalized_sql("SELECT  a"), "SELECT  a");
            assert!(warehouse.has_volatile_sql("select rand()", std::iter::empty()));
        }
    }

    // ---- Fingerprint version tags -------------------------------------------

    const OTHERS: [Warehouse; 4] = [
        Warehouse::BigQuery,
        Warehouse::Databricks,
        Warehouse::Redshift,
        Warehouse::Other,
    ];

    #[test]
    fn duckdb_strict_identities_carry_their_own_version_tag() {
        assert_eq!(
            Warehouse::DuckDb.sql_identity_version(),
            b"dbt-state-duckdb-sql-v1"
        );
        for warehouse in OTHERS.into_iter().chain([Warehouse::Snowflake]) {
            assert_eq!(
                warehouse.sql_identity_version(),
                b"dbt-state-sql-v4",
                "{warehouse:?}"
            );
        }
    }

    #[test]
    fn only_snowflake_abstracts_schemas_in_the_namespace_identity() {
        assert_eq!(
            Warehouse::Snowflake.namespace_identity(),
            NamespaceIdentity::SchemaAbstracted {
                version: b"dbt-state-ns-logic-v4"
            }
        );
        assert!(Warehouse::Snowflake.supports_namespace_reuse());
        for warehouse in OTHERS.into_iter().chain([Warehouse::DuckDb]) {
            assert_eq!(
                warehouse.namespace_identity(),
                NamespaceIdentity::Exact {
                    version: b"dbt-state-exact-ns-v1"
                },
                "{warehouse:?}"
            );
            assert!(!warehouse.supports_namespace_reuse(), "{warehouse:?}");
        }
    }

    // ---- Result copy rules --------------------------------------------------

    /// An eligible DuckDB model: a checked view chain over one physical base
    /// input with recorded evidence, built into a declared environment.
    fn duckdb_copy_submission() -> crate::query_cache::SqlExecution {
        duckdb_copy_submission_in("dev")
    }

    fn duckdb_copy_submission_in(env: &str) -> crate::query_cache::SqlExecution {
        use crate::query_cache as qc;
        let target = format!("\"local\".\"{env}\".\"m\"");
        qc::SqlExecution {
            target_table: Some(target.clone()),
            dialect: "duckdb".into(),
            default_catalog: "local".into(),
            default_schema: Some(env.into()),
            execution_type: qc::ModelExecutionType::Full as i32,
            sql: format!("select id from local.{env}.stg"),
            tables: vec![
                qc::TableModifiedInfo {
                    name: "local.raw.input".into(),
                    last_modified_epoch: Some(10),
                },
                // The target's own (unknown) evidence does not block a copy.
                qc::TableModifiedInfo {
                    name: target,
                    last_modified_epoch: None,
                },
            ],
            query_dependencies: vec![qc::QueryDependency {
                name: format!("\"local\".\"{env}\".\"stg\""),
                query: "select id from local.raw.input".into(),
                default_catalog: "local".into(),
                default_schema: env.into(),
            }],
            semantic_extras: [("duckdb_copy_contract".into(), "rows-schema-v1".into())].into(),
            ..Default::default()
        }
    }

    fn is_duckdb_copy_graph(identity: Option<CopyIdentity>) -> bool {
        matches!(
            identity,
            Some(CopyIdentity::CopyGraph {
                version: b"dbt-state-duckdb-copy-v2",
                ..
            })
        )
    }

    #[test]
    fn duckdb_copies_only_checked_table_models_with_complete_evidence() {
        use crate::query_cache::{ModelExecutionType, TableModifiedInfo};
        let duck = Warehouse::DuckDb;
        assert!(is_duckdb_copy_graph(
            duck.copy_identity(&duckdb_copy_submission())
        ));
        assert!(is_duckdb_copy_graph(
            duck.copy_identity(&duckdb_copy_submission_in("prod"))
        ));
        // Only the declared environments are copied between.
        assert!(duck
            .copy_identity(&duckdb_copy_submission_in("ci"))
            .is_none());
        let mut incremental = duckdb_copy_submission();
        incremental.execution_type = ModelExecutionType::DeleteInsert as i32;
        incremental.semantic_extras.insert(
            "duckdb_incremental_contract".into(),
            "delete-insert-fixed-v1".into(),
        );
        assert!(is_duckdb_copy_graph(duck.copy_identity(&incremental)));

        type Mutation = fn(&mut crate::query_cache::SqlExecution);
        let ineligible: Vec<(&str, Mutation)> = vec![
            ("view model", |r| {
                r.execution_type = ModelExecutionType::View as i32
            }),
            ("merge model", |r| {
                r.execution_type = ModelExecutionType::Merge as i32
            }),
            ("delete+insert without the pinned contract", |r| {
                r.execution_type = ModelExecutionType::DeleteInsert as i32
            }),
            ("no default schema", |r| r.default_schema = None),
            ("no copy contract", |r| {
                r.semantic_extras.remove("duckdb_copy_contract");
            }),
            ("other copy contract", |r| {
                r.semantic_extras
                    .insert("duckdb_copy_contract".into(), "rows-v0".into());
            }),
            ("unsupported SQL", |r| {
                r.sql = "select id from local.dev.stg order by id".into()
            }),
            ("unknown input epoch", |r| {
                r.tables[0].last_modified_epoch = None
            }),
            ("no input evidence", |r| {
                r.tables.remove(0);
            }),
            ("duplicate input evidence", |r| {
                let input = r.tables[0].clone();
                r.tables.push(input);
            }),
            ("unparseable evidence", |r| {
                r.tables.push(TableModifiedInfo {
                    name: "not a relation(".into(),
                    last_modified_epoch: Some(1),
                })
            }),
            ("no target", |r| r.target_table = None),
            ("target outside the default schema", |r| {
                r.target_table = Some(DUCK_PROD.into());
                r.tables[1].name = DUCK_PROD.into();
            }),
            ("target outside the default catalog", |r| {
                r.target_table = Some("\"other\".\"dev\".\"m\"".into());
                r.tables[1].name = "\"other\".\"dev\".\"m\"".into();
            }),
        ];
        for (why, mutate) in ineligible {
            let mut req = duckdb_copy_submission();
            mutate(&mut req);
            assert!(duck.copy_identity(&req).is_none(), "{why}");
        }
    }

    #[test]
    fn snowflake_copies_by_namespace_logic_and_other_warehouses_never_copy() {
        let mut req = duckdb_copy_submission();
        req.dialect = "snowflake".into();
        assert!(matches!(
            Warehouse::Snowflake.copy_identity(&req),
            Some(CopyIdentity::NamespaceLogic)
        ));
        for warehouse in OTHERS {
            assert!(warehouse.copy_identity(&req).is_none(), "{warehouse:?}");
        }
    }

    const DUCK_PROD: &str = "\"local\".\"prod\".\"m\"";
    const DUCK_DEV: &str = "\"local\".\"dev\".\"m\"";
    const CTAS: Option<&str> = Some("DUCKDB CTAS TABLE");

    fn ddl(
        dialect: &str,
        source: &str,
        target: &str,
        kind: Option<&str>,
    ) -> Result<Vec<String>, &'static str> {
        Warehouse::from_dialect(dialect).result_copy_ddl(source, target, kind, None)
    }

    #[test]
    fn snowflake_transient_clone_matches_real_shape() {
        let sqls = ddl(
            "snowflake",
            "\"DB\".\"PROD\".\"T\"",
            "\"DB\".\"DEV\".\"T\"",
            Some("TRANSIENT TABLE"),
        )
        .unwrap();
        assert_eq!(sqls.len(), 1);
        let s = &sqls[0];
        assert!(s.contains("CREATE OR REPLACE TRANSIENT TABLE \"DB\".\"DEV\".\"T\""));
        assert!(s.contains("CLONE \"DB\".\"PROD\".\"T\""));
        assert!(s.contains("COPY GRANTS"));
    }

    #[test]
    fn snowflake_plain_table_clone() {
        let sqls = ddl("snowflake", "p.s.t", "d.s.t", Some("TABLE")).unwrap();
        assert!(sqls[0].contains("CREATE OR REPLACE TABLE"));
        assert!(!sqls[0].contains("TRANSIENT"));
    }

    #[test]
    fn documented_non_snowflake_shapes_and_capabilities() {
        assert_eq!(
            ddl("bigquery", "s", "t", Some("TABLE")).unwrap(),
            ["CREATE TABLE t CLONE s"]
        );
        assert_eq!(
            ddl("databricks", "s", "t", Some("DELTA TABLE")).unwrap(),
            ["CREATE TABLE t SHALLOW CLONE s"]
        );
        for kind in [None, Some("TABLE"), Some("ICEBERG TABLE")] {
            assert!(ddl("databricks", "s", "t", kind).is_err());
        }
    }

    #[test]
    fn unsupported_clone_sources_and_dialects_do_not_emit_guessed_sql() {
        assert!(ddl("snowflake", "s", "t", Some("VIEW")).is_err());
        assert!(ddl("unknown", "s", "t", Some("TABLE")).is_err());
        assert!(ddl("redshift", "s", "t", Some("TABLE")).is_err());
        assert!(Warehouse::from_dialect("bigquery")
            .result_copy_ddl(
                "s",
                "t",
                Some("TABLE"),
                Some(&crate::query_cache::TableProperties {
                    hours_to_expiration: Some(24),
                    partition_expiration_days: None,
                }),
            )
            .is_err());
    }

    #[test]
    fn duckdb_copies_ctas_sources_between_environments_of_one_file() {
        assert_eq!(
            ddl("duckdb", DUCK_PROD, DUCK_DEV, CTAS).unwrap(),
            [format!(
                "CREATE OR REPLACE TABLE {DUCK_DEV} AS SELECT * FROM {DUCK_PROD}"
            )]
        );
        let refused = [
            (
                DUCK_PROD,
                DUCK_DEV,
                Some("BASE TABLE"),
                "DuckDB copy requires inspected CTAS-compatible source",
            ),
            ("not a relation(", DUCK_DEV, CTAS, "invalid source"),
            (DUCK_PROD, "m", CTAS, "invalid target"),
            (
                DUCK_PROD,
                "\"other\".\"dev\".\"m\"",
                CTAS,
                "DuckDB copies require one file and supported environments",
            ),
            (
                DUCK_PROD,
                "\"local\".\"prod\".\"n\"",
                CTAS,
                "DuckDB copies require one file and supported environments",
            ),
            (
                "\"local\".\"ci\".\"m\"",
                DUCK_DEV,
                CTAS,
                "DuckDB copies require one file and supported environments",
            ),
        ];
        for (source, target, kind, why) in refused {
            assert_eq!(
                ddl("duckdb", source, target, kind),
                Err(why),
                "{source} -> {target}"
            );
        }
    }

    #[test]
    fn duckdb_copy_ends_must_be_two_schemas_of_one_file() {
        let duck = Warehouse::DuckDb;
        assert!(duck.copy_relations_compatible(DUCK_PROD, DUCK_DEV));
        // Environment support is a DDL requirement, not a relation identity one.
        assert!(duck.copy_relations_compatible("\"local\".\"ci\".\"m\"", DUCK_DEV));
        assert!(!duck.copy_relations_compatible(DUCK_PROD, "\"other\".\"dev\".\"m\""));
        assert!(!duck.copy_relations_compatible(DUCK_PROD, "\"local\".\"prod\".\"n\""));
        assert!(!duck.copy_relations_compatible("not a relation(", DUCK_DEV));
        assert!(!duck.copy_relations_compatible(DUCK_PROD, "m"));

        for warehouse in [Warehouse::Snowflake, Warehouse::Redshift] {
            assert!(
                warehouse.copy_relations_compatible("a.b.c", "a.b.c"),
                "{warehouse:?}"
            );
        }
    }

    #[test]
    fn duckdb_copy_chains_stop_at_two_ancestors() {
        let duck = Warehouse::DuckDb;
        // (requested limit, effective limit)
        let cases = [
            (None, Some(2)),
            (Some(0), Some(0)),
            (Some(1), Some(1)),
            (Some(2), Some(2)),
            (Some(5), Some(2)),
            // A negative request wraps to an unreachable limit, as it always has.
            (Some(-1), Some(usize::MAX)),
        ];
        for (requested, limit) in cases {
            assert_eq!(
                duck.copy_chain_depth_limit(requested),
                limit,
                "{requested:?}"
            );
        }
        assert_eq!(duck.copy_provenance_depth(), Some(2));

        for warehouse in [Warehouse::Snowflake, Warehouse::BigQuery] {
            for requested in [None, Some(1), Some(5)] {
                assert_eq!(
                    warehouse.copy_chain_depth_limit(requested),
                    None,
                    "{warehouse:?}"
                );
            }
            assert_eq!(warehouse.copy_provenance_depth(), None, "{warehouse:?}");
        }
    }

    #[test]
    fn only_duckdb_considers_every_equivalent_candidate() {
        assert_eq!(Warehouse::DuckDb.copy_candidate_limit(), None);
        assert_eq!(Warehouse::Snowflake.copy_candidate_limit(), Some(1));
        assert_eq!(Warehouse::Other.copy_candidate_limit(), Some(1));
    }

    #[test]
    fn duckdb_copy_sources_need_a_recorded_epoch() {
        assert!(Warehouse::DuckDb.copy_source_evidence(Some(5)).is_ok());
        assert_eq!(
            Warehouse::DuckDb.copy_source_evidence(None),
            Err("DuckDB source requires recorded physical evidence")
        );
        for warehouse in [Warehouse::Snowflake, Warehouse::BigQuery] {
            assert!(
                warehouse.copy_source_evidence(None).is_ok(),
                "{warehouse:?}"
            );
        }
    }

    #[test]
    fn only_exactly_lowercase_duckdb_skips_the_volatility_gate() {
        assert!(Warehouse::from_dialect("duckdb").copy_identity_skips_volatility_gate("duckdb"));
        // TODO(#50): mixed case is still refused here (preserved quirk).
        assert!(!Warehouse::from_dialect("DuckDB").copy_identity_skips_volatility_gate("DuckDB"));
        for dialect in ["snowflake", "bigquery", "unknown"] {
            assert!(
                !Warehouse::from_dialect(dialect).copy_identity_skips_volatility_gate(dialect),
                "{dialect}"
            );
        }
    }

    fn incremental_copy() -> crate::query_cache::CloneRequest {
        let mut req = crate::query_cache::CloneRequest {
            execution_type: crate::query_cache::ModelExecutionType::DeleteInsert as i32,
            clone_source_table_type: CTAS.map(Into::into),
            ..Default::default()
        };
        req.labels.insert(
            "duckdb_incremental_contract".into(),
            "delete-insert-fixed-v1".into(),
        );
        req.labels.insert(
            "duckdb_incremental_identity".into(),
            "duckdb-incremental-init-v2:abc".into(),
        );
        req
    }

    #[test]
    fn duckdb_client_requested_copies_must_declare_the_pinned_incremental_contract() {
        let duck = Warehouse::DuckDb;
        assert_eq!(
            duck.incremental_copy_contract(&incremental_copy()),
            IncrementalCopyContract::Declared {
                identity: "duckdb-incremental-init-v2:abc"
            }
        );

        type Mutation = fn(&mut crate::query_cache::CloneRequest);
        let cases: Vec<(&str, Mutation)> = vec![
            ("not incremental", |r| r.execution_type = 1),
            ("no contract", |r| {
                r.labels.remove("duckdb_incremental_contract");
            }),
            ("other contract", |r| {
                r.labels
                    .insert("duckdb_incremental_contract".into(), "merge".into());
            }),
            ("no identity", |r| {
                r.labels.remove("duckdb_incremental_identity");
            }),
            ("old identity version", |r| {
                r.labels.insert(
                    "duckdb_incremental_identity".into(),
                    "duckdb-incremental-init-v1:abc".into(),
                );
            }),
            ("not inspected as CTAS", |r| {
                r.clone_source_table_type = Some("BASE TABLE".into())
            }),
        ];
        for (why, mutate) in cases {
            let mut req = incremental_copy();
            mutate(&mut req);
            assert_eq!(
                duck.incremental_copy_contract(&req),
                IncrementalCopyContract::Missing,
                "{why}"
            );
        }

        for warehouse in [Warehouse::Snowflake, Warehouse::Redshift] {
            assert_eq!(
                warehouse.incremental_copy_contract(&incremental_copy()),
                IncrementalCopyContract::NotRequired,
                "{warehouse:?}"
            );
        }
    }

    #[test]
    fn the_incremental_contract_is_one_declared_value() {
        let declared = |value: &str| {
            std::collections::HashMap::from([(
                "duckdb_incremental_contract".to_owned(),
                value.to_owned(),
            )])
        };
        assert!(
            Warehouse::DuckDb.declares_incremental_contract(&declared("delete-insert-fixed-v1"))
        );
        assert!(!Warehouse::DuckDb.declares_incremental_contract(&declared("merge")));
        assert!(!Warehouse::DuckDb.declares_incremental_contract(&Default::default()));
        assert!(!Warehouse::Snowflake
            .declares_incremental_contract(&declared("delete-insert-fixed-v1")));
    }
}
