//! Projects list and per-project environment pages.

use topcoat::{
    context::Cx,
    router::{href, module_param, page, path_param},
    view::{view, View},
};

use crate::db;
use crate::view_helpers::fmt_time;

/// /projects — list of projects with reuse stats.
#[page]
pub async fn index(cx: &Cx) -> topcoat::Result<impl View> {
    let pool = crate::pool(cx);
    let projects = db::projects(pool).await.unwrap_or_default();

    Ok(view! {
        <h1>"Projects"</h1>
        <p class="lede">"Every project that has submitted to dbt State."</p>
        if projects.is_empty() {
            <p class="empty">"No projects yet."</p>
        } else {
            <table class="data-table">
                <thead>
                    <tr>
                        <th scope="col">"Project"</th>
                        <th scope="col">"Dialect"</th>
                        <th scope="col" class="num">"Environments"</th>
                        <th scope="col" class="num">"Built"</th>
                        <th scope="col" class="num">"Reused"</th>
                        <th scope="col" class="num">"Cloned"</th>
                    </tr>
                </thead>
                <tbody>
                    for p in &projects {
                        <tr>
                            <td><a href=(href!(project_id::detail, project_id::ProjectId(p.id)))>(&p.name)</a></td>
                            <td class="muted">(p.dialect.as_deref().unwrap_or("—"))</td>
                            <td class="num">(p.environment_count)</td>
                            <td class="num">(p.built)</td>
                            <td class="num">(p.reused)</td>
                            <td class="num">(p.cloned)</td>
                        </tr>
                    }
                </tbody>
            </table>
        }
    })
}

pub mod project_id {
    use super::*;

    module_param!(pub project_id: i64, error = bad_request);

    /// /projects/{project_id} — environments in a project.
    #[page]
    pub async fn detail(cx: &Cx) -> topcoat::Result<impl View> {
        let pool = crate::pool(cx);
        let id = *path_param::<ProjectId>(cx)?;
        let name = db::project_name(pool, id)
            .await
            .ok()
            .flatten()
            .unwrap_or_else(|| "Unknown project".to_string());
        let envs = db::environments_for_project(pool, id).await.unwrap_or_default();

        Ok(view! {
            <p class="breadcrumb">
                <a href=(href!(super::index))>"Projects"</a>" / "(&name)
            </p>
            <h1>(&name)</h1>
            <h2>"Environments"</h2>
            if envs.is_empty() {
                <p class="empty">"No environments observed for this project."</p>
            } else {
                <table class="data-table">
                    <thead>
                        <tr>
                            <th scope="col">"Environment"</th>
                            <th scope="col">"Profile"</th>
                            <th scope="col">"dbt State"</th>
                            <th scope="col">"Last seen"</th>
                            <th scope="col" class="num">"Built"</th>
                            <th scope="col" class="num">"Reused"</th>
                            <th scope="col" class="num">"Cloned"</th>
                        </tr>
                    </thead>
                    <tbody>
                        for e in &envs {
                            <tr>
                                <td>(&e.name)</td>
                                <td class="muted">(e.profile_name.as_deref().unwrap_or("—"))</td>
                                <td>
                                    if e.dbt_state_enabled {
                                        <span class="flag flag--on">"enabled"</span>
                                    } else {
                                        <span class="flag">"disabled"</span>
                                    }
                                </td>
                                <td class="muted">(fmt_time(e.last_seen_at))</td>
                                <td class="num">(e.built)</td>
                                <td class="num">(e.reused)</td>
                                <td class="num">(e.cloned)</td>
                            </tr>
                        }
                    </tbody>
                </table>
            }
        })
    }
}
