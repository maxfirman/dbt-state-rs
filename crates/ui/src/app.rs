//! Root layout, home page, and route tree for the dbt State console.

use topcoat::{
    context::Cx,
    router::{href, layout, module_router, page, RouterBuilder, Slot},
    view::{class, view, View},
};

use crate::db;
use crate::view_helpers::{fmt_time, stat_card};

pub mod environments;
pub mod invocations;
pub mod projects;

/// Build the route tree. Pages are collected from this module and submodules.
pub fn router() -> RouterBuilder {
    module_router!()
}

/// Root layout: the accessible app shell wrapping every page.
#[layout]
async fn root_layout(cx: &Cx, slot: Slot<'_>) -> topcoat::Result<impl View> {
    let home_link = href!(home);
    let projects_link = href!(projects::index);
    let invocations_link = href!(invocations::index);
    let home_current = home_link.is_current(cx);
    let projects_current = projects_link.is_current(cx);
    let invocations_current = invocations_link.is_current(cx);

    Ok(view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <title>"dbt State console"</title>
                <style>(STYLES)</style>
                topcoat::dev::script()
            </head>
            <body>
                <a class="skip-link" href="#main">"Skip to main content"</a>
                <header class="app-header">
                    <a class="brand" href=(href!(home))>
                        <span class="brand-mark" aria-hidden="true">"◆"</span>
                        <span>"dbt State"</span>
                    </a>
                    <nav class="app-nav" aria-label="Primary">
                        <a
                            class=(class!("nav-link", "nav-link--active" if home_current))
                            href=(home_link)
                            aria-current=(home_current.then_some("page"))
                        >"Overview"</a>
                        <a
                            class=(class!("nav-link", "nav-link--active" if projects_current))
                            href=(projects_link)
                            aria-current=(projects_current.then_some("page"))
                        >"Projects"</a>
                        <a
                            class=(class!("nav-link", "nav-link--active" if invocations_current))
                            href=(invocations_link)
                            aria-current=(invocations_current.then_some("page"))
                        >"Invocations"</a>
                    </nav>
                </header>
                <main id="main" class="app-main" tabindex="-1">
                    (slot)
                </main>
                <footer class="app-footer">
                    <p>"Read-only console · dbt State server"</p>
                </footer>
            </body>
        </html>
    })
}

/// Home / overview.
#[page]
async fn home(cx: &Cx) -> topcoat::Result<impl View> {
    let pool = crate::pool(cx);
    let totals = db::overview_totals(pool).await.unwrap_or(db::OverviewTotals {
        built: 0,
        reused: 0,
        cloned: 0,
        invocations: 0,
        projects: 0,
        environments: 0,
    });
    let recent = db::recent_invocations(pool, 10).await.unwrap_or_default();

    let total_decisions = totals.built + totals.reused + totals.cloned;
    let reuse_pct = if total_decisions > 0 {
        ((totals.reused + totals.cloned) as f64 / total_decisions as f64 * 100.0).round() as i64
    } else {
        0
    };
    let reuse_str = format!("{reuse_pct}%");
    let built_str = totals.built.to_string();
    let reused_str = totals.reused.to_string();
    let cloned_str = totals.cloned.to_string();
    let inv_str = totals.invocations.to_string();
    let proj_str = totals.projects.to_string();

    Ok(view! {
        <h1>"Overview"</h1>
        <p class="lede">
            "What dbt State decided across your projects and environments."
        </p>

        <section class="cards" aria-label="Summary">
            stat_card(value: &reuse_str, label: "Reuse rate", hint: "of nodes reused or cloned")
            stat_card(value: &built_str, label: "Built", hint: "nodes executed")
            stat_card(value: &reused_str, label: "Reused", hint: "no-op skips")
            stat_card(value: &cloned_str, label: "Cloned", hint: "cloned from another environment")
            stat_card(value: &inv_str, label: "Invocations", hint: "runs observed")
            stat_card(value: &proj_str, label: "Projects", hint: "across environments")
        </section>

        <section aria-labelledby="recent-h">
            <h2 id="recent-h">"Recent invocations"</h2>
            if recent.is_empty() {
                <p class="empty">"No invocations recorded yet. Run dbt with dbt State pointed at this server."</p>
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
                        for row in &recent {
                            <tr>
                                <td>(&row.project_name)</td>
                                <td>(&row.environment_name)</td>
                                <td class="muted">(fmt_time(row.started_at))</td>
                                <td class="num">(row.built_count)</td>
                                <td class="num">(row.reused_count)</td>
                                <td class="num">(row.cloned_count)</td>
                                <td>
                                    <a href=(href!(invocations::invocation_id::detail, invocations::invocation_id::InvocationId(row.id)))>"View"</a>
                                </td>
                            </tr>
                        }
                    </tbody>
                </table>
            }
        </section>
    })
}


/// Minimal, clean, accessible stylesheet (no build step). Dark-on-light,
/// high-contrast, generous spacing, visible focus rings.
const STYLES: &str = r#"
:root {
  --bg: #fbfbfd; --panel: #ffffff; --ink: #1b1f24; --muted: #5c6672;
  --line: #e4e7ec; --accent: #2f6feb; --accent-ink: #ffffff;
  --built: #b4530a; --reused: #1a7f4b; --cloned: #6b3fa0;
  --radius: 10px; --shadow: 0 1px 2px rgba(16,24,40,.06), 0 1px 3px rgba(16,24,40,.1);
}
* { box-sizing: border-box; }
body { margin: 0; font: 15px/1.5 system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
  color: var(--ink); background: var(--bg); }
.skip-link { position: absolute; left: -999px; top: 0; background: var(--accent);
  color: var(--accent-ink); padding: .5rem .75rem; border-radius: 0 0 var(--radius) 0; z-index: 10; }
.skip-link:focus { left: 0; }
.app-header { display: flex; align-items: center; gap: 2rem; padding: .75rem 1.5rem;
  background: var(--panel); border-bottom: 1px solid var(--line); position: sticky; top: 0; z-index: 5; }
.brand { display: flex; align-items: center; gap: .5rem; font-weight: 700; color: var(--ink);
  text-decoration: none; font-size: 1.05rem; }
.brand-mark { color: var(--accent); }
.app-nav { display: flex; gap: .25rem; }
.nav-link { padding: .4rem .75rem; border-radius: 8px; color: var(--muted); text-decoration: none; font-weight: 500; }
.nav-link:hover { background: #f0f3f8; color: var(--ink); }
.nav-link--active { color: var(--accent); background: #eaf1fe; }
a { color: var(--accent); }
:focus-visible { outline: 3px solid #94bbff; outline-offset: 2px; border-radius: 4px; }
.app-main { max-width: 1100px; margin: 0 auto; padding: 2rem 1.5rem 4rem; }
.app-main:focus { outline: none; }
h1 { font-size: 1.6rem; margin: 0 0 .25rem; }
h2 { font-size: 1.2rem; margin: 2rem 0 .75rem; }
.lede { color: var(--muted); margin: 0 0 1.5rem; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(165px, 1fr)); gap: 1rem; }
.card { background: var(--panel); border: 1px solid var(--line); border-radius: var(--radius);
  padding: 1rem 1.1rem; box-shadow: var(--shadow); }
.card-value { font-size: 1.8rem; font-weight: 700; letter-spacing: -.02em; }
.card-label { font-weight: 600; margin-top: .1rem; }
.card-hint { color: var(--muted); font-size: .85rem; margin-top: .15rem; }
.data-table { width: 100%; border-collapse: collapse; background: var(--panel);
  border: 1px solid var(--line); border-radius: var(--radius); overflow: hidden; box-shadow: var(--shadow); }
.data-table th, .data-table td { text-align: left; padding: .6rem .85rem; border-bottom: 1px solid var(--line); }
.data-table th { background: #f7f8fa; font-size: .8rem; text-transform: uppercase; letter-spacing: .04em; color: var(--muted); }
.data-table tr:last-child td { border-bottom: none; }
.data-table .num { text-align: right; font-variant-numeric: tabular-nums; }
.muted { color: var(--muted); }
.empty { color: var(--muted); background: var(--panel); border: 1px dashed var(--line);
  border-radius: var(--radius); padding: 1.5rem; text-align: center; }
.badge { display: inline-block; padding: .15rem .55rem; border-radius: 999px; font-size: .8rem; font-weight: 600; }
.badge-built { background: #fdeede; color: var(--built); }
.badge-reused { background: #e4f6ec; color: var(--reused); }
.badge-cloned { background: #efe8f8; color: var(--cloned); }
.breadcrumb { color: var(--muted); margin: 0 0 1rem; font-size: .9rem; }
.breadcrumb a { text-decoration: none; }
.flags { display: flex; gap: .5rem; flex-wrap: wrap; margin: .5rem 0 1rem; }
.flag { font-size: .8rem; padding: .15rem .55rem; border-radius: 999px; background: #eef1f5; color: var(--muted); }
.flag--on { background: #e4f6ec; color: var(--reused); }
.filters { display: flex; gap: .5rem; margin: 1rem 0; flex-wrap: wrap; }
.filter { padding: .35rem .7rem; border-radius: 999px; border: 1px solid var(--line);
  background: var(--panel); color: var(--muted); text-decoration: none; font-size: .85rem; }
.filter--active { background: var(--accent); color: var(--accent-ink); border-color: var(--accent); }
details.node { border: 1px solid var(--line); border-radius: var(--radius); background: var(--panel); margin-bottom: .5rem; }
details.node > summary { padding: .6rem .85rem; cursor: pointer; display: flex; align-items: center; gap: .75rem; }
details.node .node-body { padding: 0 .85rem .85rem; }
code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
pre.sql { background: #0f172a; color: #e2e8f0; padding: 1rem; border-radius: 8px; overflow-x: auto; font-size: .85rem; }
.kv { display: grid; grid-template-columns: max-content 1fr; gap: .25rem 1rem; font-size: .9rem; margin: .5rem 0; }
.kv dt { color: var(--muted); }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0,0,0,0); border: 0; }
.node-list { list-style: none; padding: 0; display: grid; gap: .4rem; }
.node-list li { display: flex; align-items: center; gap: .6rem; background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: .5rem .75rem; }
.lineage-svg { display: block; }
.lineage-wrap { overflow-x: auto; border: 1px solid var(--line); border-radius: var(--radius); background: var(--panel); padding: 1rem; }
"#;
