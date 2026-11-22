//! Read-only web console for the dbt State server, built with Topcoat.
//!
//! Server-rendered, multipage, accessible. Reads the same Postgres the dbt
//! State server writes to. In `none` mode it shows one configured
//! Organization; in `oidc` mode people sign in through the Token broker
//! and see only their Organizations ([`sign_in`]).

mod app;
mod db;
mod lineage;
mod settings;
mod sign_in;
mod view_helpers;

use std::sync::Arc;

use topcoat::context::{Cx, app_context, request_context};
use topcoat::cookie::RouterBuilderCookieExt;

pub use db::connect;
pub use dbt_state_broker::Clock;
pub use settings::Settings;

use settings::Mode;
use sign_in::{Oidc, Viewer};

/// Shared application state: the read pool and how requests are scoped.
struct Ctx {
    pool: sqlx::PgPool,
    access: Access,
}

enum Access {
    /// `none` mode: one Organization; URL ids cannot change it.
    Organization(String),
    /// `oidc` mode: the signed-in person's selected Organization.
    Oidc(Arc<Oidc>),
}

/// The console's router over `pool`, configured by `settings`. `clock`
/// decides token, sign-in and session-status ages in `oidc` mode (the
/// host's clock outside tests).
pub fn router(
    pool: sqlx::PgPool,
    settings: Settings,
    clock: Clock,
) -> anyhow::Result<topcoat::router::Router> {
    let access = match settings.mode {
        Mode::None { organization } => Access::Organization(organization),
        Mode::Oidc(oidc) => Access::Oidc(Arc::new(Oidc::new(*oidc, pool.clone(), clock)?)),
    };
    Ok(app::router()
        .cookies()
        .app_context(Ctx { pool, access })
        .build())
}

/// Convenience accessor used by pages.
fn pool(cx: &Cx) -> &sqlx::PgPool {
    &app_context::<Ctx>(cx).pool
}

/// The Organization every query of this request is scoped to.
fn org_id(cx: &Cx) -> &str {
    match &app_context::<Ctx>(cx).access {
        Access::Organization(id) => id,
        Access::Oidc(_) => &request_context::<Viewer>(cx).organization,
    }
}

/// `oidc` mode's sign-in, if the console runs it.
fn oidc(cx: &Cx) -> Option<&Oidc> {
    match &app_context::<Ctx>(cx).access {
        Access::Organization(_) => None,
        Access::Oidc(oidc) => Some(oidc),
    }
}

/// The signed-in person, in `oidc` mode on a page behind sign-in.
fn viewer(cx: &Cx) -> Option<&Viewer> {
    topcoat::context::try_request_context::<Viewer>(cx)
}
