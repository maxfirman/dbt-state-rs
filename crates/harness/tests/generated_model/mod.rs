//! Test-owned closed relational grammar. No production identity/decision helpers.
use serde::{Deserialize, Serialize};
pub type Row = (i64, Option<i32>, Option<String>);
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relation {
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Row>,
    pub exists: bool,
    // These are modeled expectations, never measured warehouse properties.
    pub modeled_properties: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Query {
    Source(i32),
    Literal(i32),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Op {
    Observe,
    Insert(i64, i32),
    Update(i32),
    Delete(i64),
    K(i32),
    Literal(i32),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Step {
    pub op: Op,
    pub required: String,
    pub depends_on: Option<usize>,
}
pub fn initial() -> Vec<Row> {
    vec![
        (1, Some(10), Some("one".into())),
        (1, Some(10), Some("one".into())),
        (2, None, None),
    ]
}
fn relation(rows: Vec<Row>) -> Relation {
    Relation {
        columns: vec![
            ("id".into(), "BIGINT".into()),
            ("amount".into(), "INT".into()),
            ("tag".into(), "STRING".into()),
        ],
        rows,
        exists: true,
        modeled_properties: vec!["MANAGED".into(), "delta".into()],
    }
}
/// Independent reference: compute F afresh from the source ledger and intent.
/// Widen arithmetic first, then check the bounded INT domain.
pub fn reference(source: &[Row], query: &Query) -> Relation {
    match query {
        Query::Literal(n) => relation(vec![
            (1, Some(*n), Some("one".into())),
            (1, Some(*n), Some("one".into())),
            (2, None, None),
        ]),
        Query::Source(k) => relation(
            source
                .iter()
                .map(|(id, amount, tag)| {
                    (
                        *id,
                        amount.map(|v| i32::try_from(i64::from(v) + i64::from(*k)).unwrap()),
                        tag.clone(),
                    )
                })
                .collect(),
        ),
    }
}
/// Virtual BUILD interpreter, independently implemented from F. This evaluates
/// declared symbols; it does not execute or parse arbitrary Databricks SQL.
pub fn execute(source: &[Row], query: &Query) -> Relation {
    let mut result = Vec::new();
    match query {
        Query::Source(k) => {
            for row in source {
                let mut output = row.clone();
                if let Some(amount) = output.1.as_mut() {
                    *amount = amount.checked_add(*k).unwrap();
                }
                result.push(output);
            }
        }
        Query::Literal(n) => {
            for _ in 0..2 {
                result.push((1, Some(*n), Some(String::from("one"))));
            }
            result.push((2, None, None));
        }
    }
    relation(result)
}
pub fn absent() -> Relation {
    let mut r = relation(vec![]);
    r.exists = false;
    r
}
pub fn check(r: &Relation, f: &Relation) -> Result<(), &'static str> {
    if r.exists != f.exists {
        return Err("wrong-skip:existence");
    }
    if r.columns != f.columns {
        return Err("wrong-skip:schema");
    }
    if r.modeled_properties != f.modeled_properties {
        return Err("wrong-skip:modeled-properties");
    }
    let mut a = r.rows.clone();
    let mut b = f.rows.clone();
    a.sort();
    b.sort();
    if a != b {
        return Err("wrong-skip:rows");
    }
    Ok(())
}
pub fn contract(effective: &str, required: &str) -> Result<(), &'static str> {
    match (effective, required) {
        ("BUILD", "SKIP") => Err("required-reuse:decision"),
        ("SKIP", "BUILD") => Err("required-build:decision"),
        (a, b) if a == b => Ok(()),
        _ => Err("unsupported:decision"),
    }
}
pub fn render(query: &Query, source: &str) -> String {
    match query {
        Query::Source(0) => format!("select id, amount, tag from {source}"),
        Query::Source(k) => format!("select id, cast(amount+{k} as int) amount, tag from {source}"),
        Query::Literal(n) => format!("select cast(id as bigint) id, cast(amount as int) amount, cast(tag as string) tag from values (1,{n},'one'),(1,{n},'one'),(2,cast(null as int),cast(null as string)) as declared(id,amount,tag)"),
    }
}
pub fn plan(seed: u64, literal: bool) -> Vec<Step> {
    let mut steps = Vec::new();
    let mut push = |op, required: &str| {
        let depends_on = steps.len().checked_sub(1);
        steps.push(Step {
            op,
            required: required.into(),
            depends_on,
        });
    };
    push(Op::Observe, "BUILD");
    push(Op::Observe, "SKIP");
    if literal {
        // A -> B -> A must rebuild from the latest physical row each time.
        for n in [17, 10] {
            push(Op::Literal(n), "BUILD");
            push(Op::Observe, "SKIP");
        }
    } else {
        for op in [
            Op::Insert(3, 12),
            Op::Update(17),
            Op::Delete(3),
            Op::K(1),
            Op::K(0),
        ] {
            push(op, "BUILD");
            push(Op::Observe, "SKIP");
        }
        let mut random = seed.wrapping_add(1);
        for episode in 0..8 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let n = 11 + (random % 9) as i32;
            // Each whole episode has dependencies; never generate no-op DML.
            let id = 4 + episode;
            push(Op::Insert(id, n), "BUILD");
            push(Op::Observe, "SKIP");
            push(Op::Delete(id), "BUILD");
            push(Op::Observe, "SKIP");
        }
    }
    steps
}
pub fn apply(source: &mut Vec<Row>, query: &mut Query, op: &Op) -> bool {
    match op {
        Op::Observe => return false,
        Op::Insert(id, n) => {
            assert!(!source.iter().any(|r| r.0 == *id));
            source.push((*id, Some(*n), Some("inserted".into())));
        }
        Op::Update(n) => {
            for row in source.iter_mut().filter(|r| r.0 == 1) {
                assert_ne!(row.1, Some(*n));
                row.1 = Some(*n);
            }
        }
        Op::Delete(id) => {
            let before = source.len();
            source.retain(|r| r.0 != *id);
            assert!(source.len() < before);
        }
        Op::K(k) => {
            assert_ne!(format!("{query:?}"), format!("{:?}", Query::Source(*k)));
            *query = Query::Source(*k);
            return false;
        }
        Op::Literal(n) => {
            *query = Query::Literal(*n);
            return false;
        }
    }
    true
}
