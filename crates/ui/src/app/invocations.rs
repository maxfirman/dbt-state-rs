//! Invocations list and the per-invocation decision audit log.

use serde::Serialize;
use topcoat::{
    context::Cx,
    router::{href, module_param, page, path_param, query_params},
    view::{View, view},
};

use crate::db;
use crate::view_helpers::{decision_badge, fmt_runtime, fmt_time, short};

/// /invocations — recent runs.
#[page]
pub async fn index(cx: &Cx) -> topcoat::Result<impl View> {
    let pool = crate::pool(cx);
    let rows = db::recent_invocations(pool, crate::org_id(cx), 100)
        .await
        .unwrap_or_default();

    Ok(view! {
        <h1>"Invocations"</h1>
        <p class="lede">"The audit log of dbt runs observed by dbt State."</p>
        if rows.is_empty() {
            <p class="empty">"No invocations recorded yet."</p>
        } else {
            <table class="data-table">
                <thead>
                    <tr>
                        <th scope="col">"Project"</th>
                        <th scope="col">"Environment"</th>
                        <th scope="col">"Started"</th>
                        <th scope="col" class="num">"Built"</th>
                        <th scope="col" class="num">"Reused"</th>
                        <th scope="col" class="num">"Cloned"</th>
                        <th scope="col"><span class="sr-only">"Actions"</span></th>
                    </tr>
                </thead>
                <tbody>
                    for r in &rows {
                        <tr>
                            <td>(&r.project_name)</td>
                            <td>(&r.environment_name)</td>
                            <td class="muted">(fmt_time(r.started_at))</td>
                            <td class="num">(r.built_count)</td>
                            <td class="num">(r.reused_count)</td>
                            <td class="num">(r.cloned_count)</td>
                            <td><a href=(href!(invocation_id::detail, invocation_id::InvocationId(r.id)))>"View"</a></td>
                        </tr>
                    }
                </tbody>
            </table>
        }
    })
}

/// Query string for the decision filter on the detail page.
#[derive(Serialize)]
struct FilterQuery {
    decision: String,
}

pub mod invocation_id {
    use super::*;

    module_param!(pub invocation_id: i64, error = bad_request);

    #[query_params(error = bad_request)]
    struct Filter {
        decision: Option<String>,
    }

    /// /invocations/{invocation_id} — the per-node decision log.
    #[page]
    pub async fn detail(cx: &Cx) -> topcoat::Result<impl View> {
        let pool = crate::pool(cx);
        let id = *path_param::<InvocationId>(cx)?;
        let filter = query_params::<Filter>(cx)?;
        let active = filter.decision.clone();

        let inv = db::invocation(pool, crate::org_id(cx), id)
            .await
            .ok()
            .flatten();
        let mut decisions = db::decisions_for_invocation(pool, crate::org_id(cx), id)
            .await
            .unwrap_or_default();
        if let Some(f) = active.as_deref() {
            decisions.retain(|d| d.decision == f);
        }

        let header = match &inv {
            Some(i) => format!("{} · {}", i.project_name, i.environment_name),
            None => "Invocation".to_string(),
        };

        // Precompute filter links + active flags (href! borrows must be local).
        let all_link = href!(detail, InvocationId(id));
        let built_link = href!(detail, InvocationId(id)).query(FilterQuery {
            decision: "build".into(),
        });
        let reused_link = href!(detail, InvocationId(id)).query(FilterQuery {
            decision: "skip".into(),
        });
        let cloned_link = href!(detail, InvocationId(id)).query(FilterQuery {
            decision: "clone".into(),
        });
        let is_all = active.is_none();
        let is_built = active.as_deref() == Some("build");
        let is_reused = active.as_deref() == Some("skip");
        let is_cloned = active.as_deref() == Some("clone");

        Ok(view! {
            <p class="breadcrumb">
                <a href=(href!(super::index))>"Invocations"</a>" / "(&header)
            </p>
            <h1>(&header)</h1>
            if let Some(i) = &inv {
                <p class="lede">
                    (fmt_time(i.started_at))" · "
                    (i.built_count)" built · "(i.reused_count)" reused · "(i.cloned_count)" cloned"
                </p>
            }

            <nav class="filters" aria-label="Filter decisions">
                <a class=(topcoat::view::class!("filter", "filter--active" if is_all)) href=(all_link) aria-current=(is_all.then_some("true"))>"All"</a>
                <a class=(topcoat::view::class!("filter", "filter--active" if is_built)) href=(built_link) aria-current=(is_built.then_some("true"))>"Built"</a>
                <a class=(topcoat::view::class!("filter", "filter--active" if is_reused)) href=(reused_link) aria-current=(is_reused.then_some("true"))>"Reused"</a>
                <a class=(topcoat::view::class!("filter", "filter--active" if is_cloned)) href=(cloned_link) aria-current=(is_cloned.then_some("true"))>"Cloned"</a>
            </nav>

            <section aria-label="Node decisions">
                if decisions.is_empty() {
                    <p class="empty">"No decisions match this filter."</p>
                } else {
                    for d in &decisions {
                        node_row(d: d)
                    }
                }
            </section>
        })
    }

    /// One node decision as an expandable disclosure. A `#[component]` so it can
    /// be embedded directly in the `view!` loop.
    #[topcoat::view::component]
    async fn node_row(d: &db::DecisionRow) -> topcoat::Result<impl View> {
        let name = d.node_name.as_deref().unwrap_or("(unnamed)");
        let runtime = fmt_runtime(d.execution_runtime_ms);
        let sqls: Vec<String> = d
            .clone_sqls
            .as_ref()
            .and_then(|v| serde_json::from_value::<Vec<String>>(v.clone()).ok())
            .unwrap_or_default();
        Ok(view! {
            <details class="node">
                <summary>
                    decision_badge(decision: &d.decision)
                    <strong>(name)</strong>
                    <span class="muted">(d.resource_type.as_deref().unwrap_or(""))</span>
                    <span class="muted" style="margin-left:auto">(runtime)</span>
                </summary>
                <div class="node-body">
                    if let Some(desc) = &d.decision_description {
                        <p>(desc)</p>
                    }
                    <dl class="kv">
                        <dt>"Unique id"</dt>
                        <dd><code>(d.node_unique_id.as_deref().unwrap_or("—"))</code></dd>
                        if let Some(t) = &d.target_table {
                            <dt>"Target"</dt><dd><code>(t)</code></dd>
                        }
                        if let Some(h) = &d.node_body_hash {
                            <dt>"Body hash"</dt><dd><code>(short(h))</code></dd>
                        }
                        if d.is_stale {
                            <dt>"Stale"</dt><dd>"upstream data changed"</dd>
                        }
                        if let Some(src) = &d.clone_source {
                            <dt>"Clone source"</dt><dd><code>(src)</code></dd>
                        }
                    </dl>
                    if !sqls.is_empty() {
                        <h3>"Clone SQL"</h3>
                        for s in &sqls {
                            <pre class="sql">(s.trim())</pre>
                        }
                    }
                </div>
            </details>
        })
    }
}
