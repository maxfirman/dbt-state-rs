//! Pure engine for dbt State-powered `state:*` selectors
//! (`SelectorService.GetStateSelection`).
//!
//! The hosted service compares each submitted node's hashes against the node's
//! own last recorded execution in the `defer_to_target` env (scoped by
//! `project_id`), and returns the `node_unique_id`s matching the selected
//! `SelectorCriteria`. This was characterized LIVE against api.state.dbt.com via
//! the recording proxy (jaffle-shop, Snowflake `prod_demo` baseline, selector
//! queried from `dev_clone` whose `defer_to_target: prod_demo`); see
//! `experiments/selector_experiment.sh` and
//! `golden/fixtures/selector_state_selection.jsonl`.
//!
//! Proven semantics (all 69 captured queries reproduced exactly):
//!   * `new`/`old` are exact complements of the submitted set; `old` ⇔ a node
//!     with the same `node_unique_id` was recorded.
//!   * `modified`/`unmodified` are exact complements; `modified` ⇔ the node is
//!     new OR any of body/configs/persisted_descriptions/macros/contract
//!     differs — **relation is excluded from `modified`**.
//!   * each `modified.X` sub-selector isolates its own component hash; a NEW
//!     node (no recorded state) matches EVERY sub-selector.
//!   * the comparison is a PURE hash comparison of the client-supplied
//!     `DbtNodeData` against the recorded one — the server never recomputes.
//!
//! The engine is I/O-free so it stays table- and property-testable offline; the
//! store/service hydrate the recorded node state and feed it in.

use crate::query_cache as qc;

/// `SelectorCriteria` (selector_service.proto). Mirrors the on-wire enum ints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectorCriteria {
    Modified,
    New,
    Unmodified,
    Old,
    Body,
    Configs,
    Relation,
    PersistedDescriptions,
    Macros,
    Contract,
}

impl SelectorCriteria {
    /// Decode the wire int. `SC_UNSPECIFIED=0` and any unknown value yield None;
    /// the service rejects those rather than guessing (the client only ever
    /// sends 1..=10).
    pub fn from_i32(v: i32) -> Option<Self> {
        Some(match v {
            1 => Self::Modified,
            2 => Self::New,
            3 => Self::Unmodified,
            4 => Self::Old,
            5 => Self::Body,
            6 => Self::Configs,
            7 => Self::Relation,
            8 => Self::PersistedDescriptions,
            9 => Self::Macros,
            10 => Self::Contract,
            _ => return None,
        })
    }
}

/// The recorded comparison state for one node: the hashes the hosted service
/// stored from the node's last execution in the queried `target` env. Mirrors
/// the comparable fields of `DbtNodeData`. Absent (`None`) component hashes are
/// compared verbatim — a `None` recorded value differs from a `Some` submitted
/// value (and vice versa), matching the live service.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecordedNode {
    pub node_body_hash: Option<String>,
    pub node_configs_hash: Option<String>,
    pub node_persisted_descriptions_hash: Option<String>,
    pub node_macros_hash: Option<String>,
    pub node_contract_hash: Option<String>,
    pub node_database_representation: Option<String>,
}

/// Does `submitted` match `criteria` given its `recorded` prior state (or
/// `None` when the node has no recorded execution — i.e. it is new)?
///
/// This is the whole server-side decision for one node, verified live.
pub fn classify(
    criteria: SelectorCriteria,
    submitted: &qc::DbtNodeData,
    recorded: Option<&RecordedNode>,
) -> bool {
    use SelectorCriteria::*;
    match criteria {
        New => recorded.is_none(),
        Old => recorded.is_some(),
        Body => component_modified(recorded, |r| &r.node_body_hash, &submitted.node_body_hash),
        Configs => component_modified(
            recorded,
            |r| &r.node_configs_hash,
            &submitted.node_configs_hash,
        ),
        Relation => component_modified(
            recorded,
            |r| &r.node_database_representation,
            &submitted.node_database_representation,
        ),
        PersistedDescriptions => component_modified(
            recorded,
            |r| &r.node_persisted_descriptions_hash,
            &submitted.node_persisted_descriptions_hash,
        ),
        Macros => component_modified(
            recorded,
            |r| &r.node_macros_hash,
            &submitted.node_macros_hash,
        ),
        Contract => component_modified(
            recorded,
            |r| &r.node_contract_hash,
            &submitted.node_contract_hash,
        ),
        // `modified` = new ∪ body ∪ configs ∪ persisted_descriptions ∪ macros ∪
        // contract. Relation is intentionally EXCLUDED (verified live): a
        // database/schema/alias difference across environments must not count as
        // a content modification.
        Modified => {
            recorded.is_none()
                || classify(Body, submitted, recorded)
                || classify(Configs, submitted, recorded)
                || classify(PersistedDescriptions, submitted, recorded)
                || classify(Macros, submitted, recorded)
                || classify(Contract, submitted, recorded)
        }
        // `unmodified` = old AND not modified (the exact complement of modified
        // within the recorded set).
        Unmodified => recorded.is_some() && !classify(Modified, submitted, recorded),
    }
}

/// A component is "modified" when there is no recorded node (the node is new, so
/// every component counts as changed) or the recorded component hash differs
/// from the submitted one. `None` vs `Some` counts as a difference.
fn component_modified(
    recorded: Option<&RecordedNode>,
    pick: impl Fn(&RecordedNode) -> &Option<String>,
    submitted: &Option<String>,
) -> bool {
    match recorded {
        None => true,
        Some(r) => pick(r) != submitted,
    }
}

/// Apply `classify` across a submitted batch, returning the matching
/// `node_unique_id`s. `lookup` yields the recorded state for a node (or `None`
/// when the node has never been recorded in the queried scope).
pub fn select_matching(
    criteria: SelectorCriteria,
    submitted: &[qc::DbtNodeData],
    lookup: impl Fn(&str) -> Option<RecordedNode>,
) -> Vec<String> {
    submitted
        .iter()
        .filter(|node| classify(criteria, node, lookup(&node.node_unique_id).as_ref()))
        .map(|node| node.node_unique_id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(uid: &str) -> qc::DbtNodeData {
        qc::DbtNodeData {
            node_unique_id: uid.to_string(),
            node_hash: "h".to_string(),
            node_body_hash: Some("body0".to_string()),
            node_configs_hash: Some("cfg0".to_string()),
            node_persisted_descriptions_hash: Some("pd0".to_string()),
            node_macros_hash: Some("mac0".to_string()),
            node_contract_hash: Some("con0".to_string()),
            node_database_representation: Some("db.sc.al".to_string()),
        }
    }

    /// A recorded node identical to `node("x")` — the unmodified baseline.
    fn recorded_same() -> RecordedNode {
        RecordedNode {
            node_body_hash: Some("body0".to_string()),
            node_configs_hash: Some("cfg0".to_string()),
            node_persisted_descriptions_hash: Some("pd0".to_string()),
            node_macros_hash: Some("mac0".to_string()),
            node_contract_hash: Some("con0".to_string()),
            node_database_representation: Some("db.sc.al".to_string()),
        }
    }

    use SelectorCriteria::*;
    const ALL: [SelectorCriteria; 10] = [
        Modified,
        New,
        Unmodified,
        Old,
        Body,
        Configs,
        Relation,
        PersistedDescriptions,
        Macros,
        Contract,
    ];

    #[test]
    fn criteria_wire_decoding_matches_proto() {
        assert_eq!(SelectorCriteria::from_i32(1), Some(Modified));
        assert_eq!(SelectorCriteria::from_i32(2), Some(New));
        assert_eq!(SelectorCriteria::from_i32(3), Some(Unmodified));
        assert_eq!(SelectorCriteria::from_i32(4), Some(Old));
        assert_eq!(SelectorCriteria::from_i32(5), Some(Body));
        assert_eq!(SelectorCriteria::from_i32(6), Some(Configs));
        assert_eq!(SelectorCriteria::from_i32(7), Some(Relation));
        assert_eq!(SelectorCriteria::from_i32(8), Some(PersistedDescriptions));
        assert_eq!(SelectorCriteria::from_i32(9), Some(Macros));
        assert_eq!(SelectorCriteria::from_i32(10), Some(Contract));
        // SC_UNSPECIFIED and out-of-range are rejected, not guessed.
        assert_eq!(SelectorCriteria::from_i32(0), None);
        assert_eq!(SelectorCriteria::from_i32(11), None);
        assert_eq!(SelectorCriteria::from_i32(-1), None);
    }

    #[test]
    fn new_node_matches_new_and_modified_and_every_sub_selector() {
        // VERIFIED LIVE (add.py): a node with no recorded state is selected by
        // new, modified, and ALL modified.* sub-selectors; not by old/unmodified.
        let n = node("model.p.brand_new");
        for c in ALL {
            let hit = classify(c, &n, None);
            let expected = !matches!(c, Old | Unmodified);
            assert_eq!(hit, expected, "criterion {c:?} on a new node");
        }
    }

    #[test]
    fn unchanged_node_is_old_and_unmodified_only() {
        // A recorded node identical to the submission: old + unmodified, and no
        // content sub-selector fires. relation also does not fire (same repr).
        let n = node("model.p.x");
        let rec = recorded_same();
        for c in ALL {
            let hit = classify(c, &n, Some(&rec));
            let expected = matches!(c, Old | Unmodified);
            assert_eq!(hit, expected, "criterion {c:?} on an unchanged node");
        }
    }

    #[test]
    fn body_change_fires_body_and_modified_only() {
        let mut n = node("model.p.x");
        n.node_body_hash = Some("body1".to_string());
        let rec = recorded_same();
        assert!(classify(Body, &n, Some(&rec)));
        assert!(classify(Modified, &n, Some(&rec)));
        assert!(classify(Old, &n, Some(&rec)));
        for c in [
            New,
            Unmodified,
            Configs,
            Relation,
            PersistedDescriptions,
            Macros,
            Contract,
        ] {
            assert!(
                !classify(c, &n, Some(&rec)),
                "criterion {c:?} must not fire"
            );
        }
    }

    #[test]
    fn configs_change_fires_configs_and_modified_only() {
        let mut n = node("model.p.x");
        n.node_configs_hash = Some("cfg1".to_string());
        let rec = recorded_same();
        assert!(classify(Configs, &n, Some(&rec)));
        assert!(classify(Modified, &n, Some(&rec)));
        for c in [
            New,
            Unmodified,
            Body,
            Relation,
            PersistedDescriptions,
            Macros,
            Contract,
        ] {
            assert!(
                !classify(c, &n, Some(&rec)),
                "criterion {c:?} must not fire"
            );
        }
    }

    #[test]
    fn contract_change_fires_contract_and_modified_only() {
        let mut n = node("model.p.x");
        n.node_contract_hash = Some("con1".to_string());
        let rec = recorded_same();
        assert!(classify(Contract, &n, Some(&rec)));
        assert!(classify(Modified, &n, Some(&rec)));
        for c in [
            New,
            Unmodified,
            Body,
            Configs,
            Relation,
            PersistedDescriptions,
            Macros,
        ] {
            assert!(
                !classify(c, &n, Some(&rec)),
                "criterion {c:?} must not fire"
            );
        }
    }

    #[test]
    fn macros_change_fires_macros_and_modified_only() {
        let mut n = node("model.p.x");
        n.node_macros_hash = Some("mac1".to_string());
        let rec = recorded_same();
        assert!(classify(Macros, &n, Some(&rec)));
        assert!(classify(Modified, &n, Some(&rec)));
        for c in [
            New,
            Unmodified,
            Body,
            Configs,
            Relation,
            PersistedDescriptions,
            Contract,
        ] {
            assert!(
                !classify(c, &n, Some(&rec)),
                "criterion {c:?} must not fire"
            );
        }
    }

    #[test]
    fn persisted_descriptions_change_fires_pd_and_modified_only() {
        let mut n = node("model.p.x");
        n.node_persisted_descriptions_hash = Some("pd1".to_string());
        let rec = recorded_same();
        assert!(classify(PersistedDescriptions, &n, Some(&rec)));
        assert!(classify(Modified, &n, Some(&rec)));
        for c in [New, Unmodified, Body, Configs, Relation, Macros, Contract] {
            assert!(
                !classify(c, &n, Some(&rec)),
                "criterion {c:?} must not fire"
            );
        }
    }

    #[test]
    fn relation_change_fires_relation_but_not_modified() {
        // CRITICAL (verified live): a database/schema/alias change fires
        // modified.relation but is EXCLUDED from state:modified.
        let mut n = node("model.p.x");
        n.node_database_representation = Some("db.sc.al2".to_string());
        let rec = recorded_same();
        assert!(classify(Relation, &n, Some(&rec)));
        assert!(
            !classify(Modified, &n, Some(&rec)),
            "relation must NOT imply modified"
        );
        assert!(
            classify(Unmodified, &n, Some(&rec)),
            "relation-only change stays unmodified"
        );
        for c in [New, Body, Configs, PersistedDescriptions, Macros, Contract] {
            assert!(
                !classify(c, &n, Some(&rec)),
                "criterion {c:?} must not fire"
            );
        }
    }

    #[test]
    fn recorded_null_relation_vs_submitted_value_fires_relation() {
        // Live: builds record node_database_representation=null while the
        // selector submits a real value -> relation fires for every built node.
        let n = node("model.p.x"); // submitted repr = Some("db.sc.al")
        let rec = RecordedNode {
            node_database_representation: None,
            ..recorded_same()
        };
        assert!(classify(Relation, &n, Some(&rec)));
        // ...but still not modified (relation excluded).
        assert!(!classify(Modified, &n, Some(&rec)));
    }

    #[test]
    fn none_vs_some_component_counts_as_modified() {
        // A node whose recorded contract hash is absent but now present.
        let n = node("model.p.x"); // contract = Some("con0")
        let rec = RecordedNode {
            node_contract_hash: None,
            ..recorded_same()
        };
        assert!(classify(Contract, &n, Some(&rec)));
        assert!(classify(Modified, &n, Some(&rec)));
    }

    #[test]
    fn new_old_partition_and_modified_unmodified_partition() {
        // new ∩ old = ∅, new ∪ old = all; modified ∩ unmodified = ∅,
        // modified ∪ unmodified = all — for both recorded states.
        let n = node("model.p.x");
        for rec in [None, Some(recorded_same())] {
            let r = rec.as_ref();
            assert_ne!(
                classify(New, &n, r),
                classify(Old, &n, r),
                "new/old partition"
            );
            assert_ne!(
                classify(Modified, &n, r),
                classify(Unmodified, &n, r),
                "modified/unmodified partition"
            );
        }
    }

    #[test]
    fn select_matching_filters_and_preserves_ids() {
        let nodes = vec![node("model.p.a"), node("model.p.b"), node("model.p.c")];
        // a is recorded-unchanged, b has a body change, c is new.
        let lookup = |uid: &str| match uid {
            "model.p.a" => Some(recorded_same()),
            "model.p.b" => Some(RecordedNode {
                node_body_hash: Some("different".to_string()),
                ..recorded_same()
            }),
            _ => None,
        };
        let modified = select_matching(Modified, &nodes, lookup);
        assert_eq!(
            modified,
            vec!["model.p.b".to_string(), "model.p.c".to_string()]
        );
        let newish = select_matching(New, &nodes, lookup);
        assert_eq!(newish, vec!["model.p.c".to_string()]);
        let old = select_matching(Old, &nodes, lookup);
        assert_eq!(old, vec!["model.p.a".to_string(), "model.p.b".to_string()]);
    }
}
