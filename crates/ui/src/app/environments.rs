//! Per-environment catalog / lineage view.

use topcoat::{
    context::Cx,
    router::{module_param, page, path_param},
    view::{Unescaped, View, view},
};

use crate::db;
use crate::lineage;
use crate::view_helpers::decision_badge;

pub mod environment_id {
    use super::*;

    module_param!(pub environment_id: i64, error = bad_request);

    /// /environments/{environment_id}/lineage — latest state + lineage graph.
    #[page]
    pub async fn catalog(cx: &Cx) -> topcoat::Result<impl View> {
        let pool = crate::pool(cx);
        let id = *path_param::<EnvironmentId>(cx)?;
        let decisions = db::latest_decisions_for_environment(pool, crate::org_id(cx), id)
            .await
            .unwrap_or_default();

        let layout = lineage::build(&decisions);
        let svg = layout.as_ref().map(lineage::render_svg);

        Ok(view! {
            <h1>"Target lineage"</h1>
            <p class="lede">"Latest known state of each node for this dbt target (profile target name, not a managed environment)."</p>

            if decisions.is_empty() {
                <p class="empty">"No nodes recorded for this environment yet."</p>
            } else {
                if let Some(svg) = svg {
                    <div class="lineage-wrap">
                        (Unescaped::new_unchecked(svg))
                    </div>
                }

                <h2>"Nodes"</h2>
                <p class="muted">"A text alternative to the graph above."</p>
                <ul class="node-list">
                    for d in &decisions {
                        <li>
                            decision_badge(decision: &d.decision)
                            <strong>(d.node_name.as_deref().unwrap_or("(unnamed)"))</strong>
                            <span class="muted">" — "(d.resource_type.as_deref().unwrap_or(""))</span>
                        </li>
                    }
                </ul>
            }
        })
    }
}
