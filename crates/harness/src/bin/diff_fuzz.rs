//! Strategy #2 — differential fuzzing against the REAL dbt State service.
//!
//! OPT-IN tool, NOT a CI test. It burns dbt State trial metering (real calls to
//! api.state.dbt.com) and needs the dbt Cloud credential in ~/.dbt/dbt_cloud.yml,
//! so it is a standalone binary you run manually to DISCOVER divergences and
//! harvest new golden cases.
//!
//! What it does (semantic/structural fuzzing — NOT byte fuzzing, which would
//! only exercise prost/tonic rather than our decision logic):
//!   1. Loads real captured SubmitEnrichedSQL requests as seeds.
//!   2. Mutates each along MEANINGFUL axes: stale_upstream_policy, per-input
//!      last_modified_epoch perturbed around the freshness boundary,
//!      execution_type, adding/removing upstream tables, node_body_hash
//!      (match vs forced miss), freshness tolerance, AND the fields our server
//!      currently ignores — node_configs_hash, node_contract_hash,
//!      tolerate_nondeterminism, ignore_external_modifications,
//!      compare_unrendered_code, lenient_dependencies — to discover whether the
//!      real service acts on them (review finding C1).
//!   3. Sends each mutant to BOTH the real service and a local instance of our
//!      server (empty state), normalizes nondeterministic fields, and compares
//!      the decision variant only. This is discovery, not a conformance oracle.
//!   4. Classifies each result and prints a report. Divergences on EXECUTE
//!      decisions (reproducible from empty state) are the high-signal findings;
//!      SKIPs by the real service that we cannot reproduce are expected (it has
//!      pre-existing history) and reported separately as candidate golden cases.
//!
//! Usage:
//!   cargo run -p dbt-state-harness --bin diff-fuzz -- \
//!     --seeds golden/fixtures/golden_20261008T222744.061Z.jsonl --iterations 200
//!
//! Env: DATABASE_URL (temp schema for our server), PROXY_UPSTREAM (default
//! https://api.state.dbt.com:443).

use std::collections::BTreeMap;

use dbt_state_harness::auth::{DbtCloudCredential, TokenMinter};
use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use sqlx::postgres::PgPoolOptions;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;

use qc::sql_client::SqlClient;

const DEFAULT_UPSTREAM: &str = "https://api.state.dbt.com:443";
const DEFAULT_DSN: &str = "postgres://dbtstate:dbtstate@localhost:55441/dbtstate";

#[derive(Default)]
struct Report {
    total: usize,
    errors: usize,
    agree: usize,
    // real executed and we executed (reproducible agreement — strongest signal).
    agree_execute: usize,
    // real skipped, we executed (expected: real has pre-existing history).
    real_skip_we_execute: usize,
    // DIVERGENCE: real executed but we skipped, or structural mismatch.
    divergence: usize,
    divergent_samples: Vec<String>,
    // Per-mutation-axis breakdowns for triage.
    by_axis: BTreeMap<String, usize>,
    real_skip_by_axis: BTreeMap<String, usize>,
    divergence_by_axis: BTreeMap<String, usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("warn").init();

    let args: Vec<String> = std::env::args().collect();
    let seeds_path = flag(&args, "--seeds")
        .unwrap_or_else(|| "golden/fixtures/golden_20261008T222744.061Z.jsonl".to_string());
    let iterations: usize = flag(&args, "--iterations")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let upstream = std::env::var("PROXY_UPSTREAM").unwrap_or_else(|_| DEFAULT_UPSTREAM.to_string());
    let dsn = std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DSN.to_string());

    anyhow::ensure!(iterations > 0, "iterations must be positive");

    // --- Real service client (authenticated) ---
    let credential = DbtCloudCredential::from_default_config()?;
    let minter = TokenMinter::new(credential);
    let token = minter.token().await?;
    let real_channel = Channel::from_shared(upstream.clone())?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;
    println!(
        "connected to real service {upstream} (org {})",
        token.org_id
    );

    // --- Our server (in-process, temp schema) ---
    let our_addr = start_our_server(&dsn).await?;
    let our_channel = Channel::from_shared(format!("http://{our_addr}"))?
        .connect()
        .await?;
    println!("our server on {our_addr}");

    // --- Seeds ---
    let entries = diff::load_golden(&seeds_path)?;
    let seeds: Vec<serde_json::Value> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .map(|e| e.request.clone())
        .collect();
    anyhow::ensure!(
        !seeds.is_empty(),
        "no SubmitEnrichedSQL seeds in {seeds_path}"
    );
    println!("{} seeds, {iterations} mutants", seeds.len());

    let mut rng = SimpleRng::new(0xC0FFEE);
    let mut report = Report::default();

    for i in 0..iterations {
        let seed = &seeds[i % seeds.len()];
        let (mutant, axis) = mutate(seed, &mut rng);
        let req: qc::SubmitEnrichedSqlRequest = match serde_json::from_value(mutant.clone()) {
            Ok(r) => r,
            Err(_) => {
                report.errors += 1;
                continue;
            }
        };

        let real = call(&real_channel, req.clone(), Some(&token)).await;
        let ours = call(&our_channel, req.clone(), None).await;

        let (real_v, ours_v) = match (real, ours) {
            (Ok(a), Ok(b)) => (a, b),
            _ => {
                report.errors += 1;
                continue;
            }
        };

        report.total += 1;
        *report.by_axis.entry(axis.to_string()).or_default() += 1;
        if real_v == ours_v {
            report.agree += 1;
            if real_v == "ready_to_execute" {
                report.agree_execute += 1;
            }
        } else if real_v == "skip_execution" && ours_v == "ready_to_execute" {
            report.real_skip_we_execute += 1;
            *report
                .real_skip_by_axis
                .entry(axis.to_string())
                .or_default() += 1;
        } else {
            report.divergence += 1;
            *report
                .divergence_by_axis
                .entry(axis.to_string())
                .or_default() += 1;
            if report.divergent_samples.len() < 40 {
                report.divergent_samples.push(format!(
                    "axis={axis} real={real_v} ours={ours_v} req={}",
                    summarize(&mutant)
                ));
            }
        }
    }

    print_report(&report);
    anyhow::ensure!(
        report.errors == 0 && report.total == iterations,
        "inconclusive discovery run: {} failed comparisons",
        report.errors
    );
    anyhow::ensure!(
        report.divergence == 0,
        "{} decision divergences",
        report.divergence
    );
    Ok(())
}

fn print_report(r: &Report) {
    println!("\n==== differential-fuzz report ====");
    println!("comparisons:            {}", r.total);
    println!("failed comparisons:     {}", r.errors);
    println!("Empty local history: this is a discovery report, not a conformance verdict.");
    println!("agree:                  {}", r.agree);
    println!("  agree (execute):      {}", r.agree_execute);
    println!(
        "real-skip / we-execute: {} (expected: real has prior history)",
        r.real_skip_we_execute
    );
    println!("DIVERGENCES:            {}", r.divergence);
    println!("\nper-axis comparisons / real-skip / DIVERGENCE:");
    let mut axes: Vec<&String> = r.by_axis.keys().collect();
    axes.sort();
    for a in axes {
        let total = r.by_axis.get(a).copied().unwrap_or(0);
        let rs = r.real_skip_by_axis.get(a).copied().unwrap_or(0);
        let dv = r.divergence_by_axis.get(a).copied().unwrap_or(0);
        let flag = if dv > 0 { "  <-- DIVERGENCE" } else { "" };
        println!("  {a:28} n={total:<4} real_skip={rs:<4} div={dv}{flag}");
    }
    for s in &r.divergent_samples {
        println!("  !! {s}");
    }
    if r.divergence == 0 {
        println!(
            "\nNo high-signal divergences. Promote real-skip cases to golden fixtures if desired."
        );
    } else {
        println!(
            "\nInvestigate divergences above — each is a candidate new golden case / logic fix."
        );
    }
}

/// Call SubmitEnrichedSQL and return the decision variant name.
async fn call(
    channel: &Channel,
    req: qc::SubmitEnrichedSqlRequest,
    token: Option<&dbt_state_harness::auth::StateToken>,
) -> Result<&'static str, tonic::Status> {
    let mut request = Request::new(req);
    if let Some(t) = token {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", t.id_token).parse().unwrap(),
        );
        request
            .metadata_mut()
            .insert("x-organization-id", t.org_id.parse().unwrap());
    }
    let resp = SqlClient::new(channel.clone())
        .submit_enriched_sql(request)
        .await?
        .into_inner();
    Ok(match resp.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(_)) => "ready_to_execute",
        Some(qc::submit_sql_response::Response::SkipExecution(_)) => "skip_execution",
        Some(qc::submit_sql_response::Response::ReadyToClone(_)) => "ready_to_clone",
        None => "none",
    })
}

/// Mutate a seed request along meaningful semantic axes.
///
/// The axes beyond index 5 specifically probe request fields our server
/// currently IGNORES in the decision (node_configs_hash / node_contract_hash /
/// tolerate_nondeterminism / ignore_external_modifications /
/// compare_unrendered_code / lenient_dependencies) plus an add-upstream axis.
/// If the real service acts on any of these while we don't, the differential
/// run surfaces it as a `real-skip/we-execute` or a DIVERGENCE, which is the
/// evidence needed to decide whether the field must enter our match/freshness
/// logic (review finding C1).
fn mutate(seed: &serde_json::Value, rng: &mut SimpleRng) -> (serde_json::Value, &'static str) {
    let mut m = seed.clone();
    let obj = m.as_object_mut().unwrap();

    let axis = rng.next() % 14;
    let label = match axis {
        0 => {
            // Toggle stale_upstream_policy.
            obj.insert(
                "stale_upstream_policy".into(),
                serde_json::json!(rng.next() % 2),
            );
            "stale_upstream_policy"
        }
        1 => {
            // Perturb each input epoch around a boundary.
            if let Some(tabs) = obj.get_mut("tables").and_then(|t| t.as_array_mut()) {
                for t in tabs {
                    if let Some(e) = t.get("last_modified_epoch").and_then(|v| v.as_i64()) {
                        let delta = (rng.next() as i64 % 10_000_000) - 5_000_000;
                        t["last_modified_epoch"] = serde_json::json!(e + delta);
                    }
                }
            }
            "input_epoch_perturb"
        }
        2 => {
            // Flip execution_type.
            obj.insert("execution_type".into(), serde_json::json!(rng.next() % 12));
            "execution_type"
        }
        3 => {
            // Drop an upstream table.
            if let Some(tabs) = obj.get_mut("tables").and_then(|t| t.as_array_mut()) {
                if !tabs.is_empty() {
                    tabs.remove(0);
                }
            }
            "drop_upstream"
        }
        4 => {
            // Force a body-hash miss (random hash).
            set_node_state_hash(obj, "node_body_hash", &format!("{:032x}", rng.next()));
            "body_hash_miss"
        }
        5 => {
            // Change freshness tolerance.
            obj.insert(
                "freshness_tolerance_seconds".into(),
                serde_json::json!((rng.next() % 7200) as i64),
            );
            "freshness_tolerance"
        }
        6 => {
            // IGNORED-FIELD PROBE: mutate node_configs_hash while keeping
            // node_body_hash fixed. If the real service treats a config change
            // as a logic change (we don't), this shows as real-skip/we-skip
            // divergence from a hydrated seed, or real-execute/we-skip.
            set_node_state_hash(obj, "node_configs_hash", &format!("cfg{:028x}", rng.next()));
            "node_configs_hash"
        }
        7 => {
            // IGNORED-FIELD PROBE: mutate node_contract_hash, body fixed.
            set_node_state_hash(
                obj,
                "node_contract_hash",
                &format!("con{:028x}", rng.next()),
            );
            "node_contract_hash"
        }
        8 => {
            // IGNORED-FIELD PROBE: toggle tolerate_nondeterminism.
            obj.insert(
                "tolerate_nondeterminism".into(),
                serde_json::json!(rng.next().is_multiple_of(2)),
            );
            "tolerate_nondeterminism"
        }
        9 => {
            // IGNORED-FIELD PROBE: toggle ignore_external_modifications +
            // compare_unrendered_code together (both freshness/logic knobs).
            obj.insert(
                "ignore_external_modifications".into(),
                serde_json::json!(rng.next().is_multiple_of(2)),
            );
            obj.insert(
                "compare_unrendered_code".into(),
                serde_json::json!(rng.next().is_multiple_of(2)),
            );
            "external_mods+unrendered"
        }
        10 => {
            // IGNORED-FIELD PROBE: mark the first upstream as a lenient
            // dependency (per-dependency freshness relaxation).
            if let Some(first) = obj
                .get("tables")
                .and_then(|t| t.as_array())
                .and_then(|a| a.first())
                .and_then(|t| t.get("name"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
            {
                obj.insert("lenient_dependencies".into(), serde_json::json!([first]));
            }
            "lenient_dependencies"
        }
        12 => {
            let sql = obj.get("sql").and_then(|v| v.as_str()).unwrap_or("");
            obj.insert(
                "sql".into(),
                serde_json::json!(format!("select * from ({sql}) as fuzz_sql where false")),
            );
            "rendered_sql"
        }
        13 => {
            let extras = obj
                .entry("semantic_extras")
                .or_insert_with(|| serde_json::json!({}));
            extras["sql_header"] = serde_json::json!(format!("-- fuzz {}", rng.next()));
            "semantic_extras"
        }
        _ => {
            // ADD-UPSTREAM: inject a brand-new upstream, far newer than any
            // existing one. If the real service treats a never-seen dependency
            // as a change (we fall back to the recorded build epoch), this
            // probes review finding C3.
            if let Some(tabs) = obj.get_mut("tables").and_then(|t| t.as_array_mut()) {
                tabs.push(serde_json::json!({
                    "name": format!("\"DB\".\"PROD\".\"fuzz_new_{:x}\"", rng.next() % 0xffff),
                    "last_modified_epoch": 4_000_000_000_000i64,
                }));
            }
            "add_upstream"
        }
    };
    (m, label)
}

/// Set a hash field inside `dbt_node_state`, creating the object if needed.
fn set_node_state_hash(obj: &mut serde_json::Map<String, serde_json::Value>, key: &str, val: &str) {
    let ns = obj
        .entry("dbt_node_state")
        .or_insert_with(|| serde_json::json!({}));
    if let Some(map) = ns.as_object_mut() {
        map.insert(key.into(), serde_json::json!(val));
    }
}

fn summarize(req: &serde_json::Value) -> String {
    let node = req
        .get("labels")
        .and_then(|l| l.get("dbt_node_name"))
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    let etype = req
        .get("execution_type")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);
    let pol = req
        .get("stale_upstream_policy")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let tol = req
        .get("freshness_tolerance_seconds")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    format!("node={node} etype={etype} policy={pol} tol={tol}")
}

/// Spin up our server in-process against a fresh temp schema. Returns its addr.
async fn start_our_server(dsn: &str) -> anyhow::Result<std::net::SocketAddr> {
    let schema = format!("fuzz_{}", uuid::Uuid::new_v4().simple());
    let admin = PgPoolOptions::new().max_connections(1).connect(dsn).await?;
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&admin)
        .await?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .after_connect(move |conn, _| {
            let schema = schema.clone();
            Box::pin(async move {
                use sqlx::Executor;
                conn.execute(format!("SET search_path TO \"{schema}\"").as_str())
                    .await?;
                Ok(())
            })
        })
        .connect(dsn)
        .await?;
    sqlx::migrate!("../server/migrations").run(&pool).await?;

    let state = dbt_state_server::AppState::from_pool(pool);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tokio::spawn(async move {
        let _ = dbt_state_server::build_router(state)
            .serve_with_incoming(incoming)
            .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    Ok(addr)
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

/// Tiny deterministic xorshift RNG (no extra dep; reproducible fuzz runs).
struct SimpleRng(u64);
impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}
