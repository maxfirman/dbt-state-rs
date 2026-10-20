//! Warehouse-specific interpretation of the dialect string on the wire.
//!
//! A parser accepting a dialect is not evidence that two SQL statements or
//! physical relations are equivalent. Only Snowflake's schema abstraction has
//! been characterized against the hosted service. Other warehouses retain exact
//! rendered SQL and do not opt into cross-environment clone reuse yet.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Warehouse {
    Snowflake,
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
        match self {
            Self::Snowflake => crate::sql_norm::normalize_sql(sql),
            _ => sql.to_owned(),
        }
    }

    pub fn supports_namespace_reuse(self) -> bool {
        matches!(self, Self::Snowflake)
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
}

#[cfg(test)]
mod tests {
    use super::Warehouse;

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
}
