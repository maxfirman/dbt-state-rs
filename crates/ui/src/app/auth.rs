//! Sign-in routes (`oidc` mode; 404 in `none` mode): `/auth/login` starts a
//! sign-in at the Token broker, and `/auth/callback` (`[clients.ui]
//! redirect_uri`) completes it.

pub mod login {
    use topcoat::{
        context::Cx,
        router::{
            error::{SeeOther, not_found},
            query_params, route,
        },
    };

    #[query_params(error = bad_request)]
    pub struct LoginQuery {
        next: Option<String>,
    }

    /// Sign in at the broker, then return to `next` (a local path; the
    /// overview otherwise).
    #[route(GET)]
    pub async fn start(cx: &Cx) -> topcoat::Result<SeeOther> {
        let Some(oidc) = crate::oidc(cx) else {
            return Err(not_found().into());
        };
        let query = query_params::<LoginQuery>(cx)?;
        Ok(oidc.sign_in(cx, query.next.as_deref().unwrap_or("/")))
    }
}

pub mod callback {
    use topcoat::{
        context::Cx,
        router::{
            StatusCode,
            error::{not_found, see_other},
            page, query_params,
        },
        view::{View, view},
    };

    use crate::sign_in::CallbackError;

    #[query_params(error = bad_request)]
    pub struct CallbackQuery {
        code: Option<String>,
        state: Option<String>,
        error: Option<String>,
    }

    /// The broker returns the browser here with a code (or an OAuth
    /// error). A completed sign-in returns to the page it started from;
    /// anything else is a page that says why, with fixed text.
    #[page]
    pub async fn finish(cx: &Cx) -> topcoat::Result<impl View> {
        let Some(oidc) = crate::oidc(cx) else {
            return Err(not_found().into());
        };
        let query = query_params::<CallbackQuery>(cx)?;
        let error = match oidc
            .complete_sign_in(
                cx,
                query.code.as_deref(),
                query.state.as_deref(),
                query.error.as_deref(),
            )
            .await
        {
            Ok(next) => return Err(see_other(next).into()),
            Err(error) => error,
        };
        let (status, message) = match error {
            CallbackError::Expired => (
                StatusCode::BAD_REQUEST,
                "This sign-in expired or was already completed.",
            ),
            CallbackError::Denied => (
                StatusCode::FORBIDDEN,
                "You have no access to dbt State. Ask your dbt State administrator for access.",
            ),
            CallbackError::NoOrganization => (
                StatusCode::FORBIDDEN,
                "You have no access to an Organization this console shows.",
            ),
            CallbackError::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Sign-in is unavailable. Try again later.",
            ),
            CallbackError::Failed => (StatusCode::BAD_REQUEST, "Sign-in failed."),
        };
        Ok(view! {
            (status)
            <h1>"Sign-in"</h1>
            <p class="lede">(message)</p>
            <p><a href="/auth/login">"Sign in again"</a></p>
        })
    }
}
