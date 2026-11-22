//! The Organization switcher (`oidc` mode): `POST /organization` selects
//! one of the signed-in person's Organizations for every page after it.

use serde::Deserialize;
use topcoat::{
    context::Cx,
    router::{
        content::Form,
        error::{SeeOther, not_found, see_other},
        route,
    },
};

#[derive(Deserialize)]
pub struct Switch {
    organization: String,
    csrf: String,
}

/// Select `organization`, if the person has it and the form carries their
/// session's CSRF token; otherwise 403. Then back to the overview, since
/// the previous page's ids belong to the previous Organization.
#[route(POST)]
pub async fn switch(cx: &Cx, Form(form): Form<Switch>) -> topcoat::Result<SeeOther> {
    let (Some(oidc), Some(viewer)) = (crate::oidc(cx), crate::viewer(cx)) else {
        return Err(not_found().into());
    };
    oidc.switch(cx, viewer, &form.organization, &form.csrf)?;
    Ok(see_other("/"))
}
