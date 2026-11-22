//! The harness browser: what Fusion's sign-in URL opens.
//!
//! On Linux, Fusion opens its sign-in URL with the `open` crate, which runs
//! the desktop's URL opener, `xdg-open`, found on `PATH`. The harness puts
//! an `xdg-open` there that runs `fusion-driver browser <url>`, so the
//! browser is this module. It does what a person's browser does when they
//! are already signed in at the Upstream IdP: it follows the redirects from
//! the Token broker's sign-in URL, through the IdP and back through the
//! broker, to Fusion's loopback handler, which answers its page. It trusts
//! the CA the host trusts (`SSL_CERT_FILE`).
//!
//! Each visit is appended as one JSON line to `DRIVER_BROWSER_LOG`: the URL
//! opened, every redirect followed and where it landed (without queries,
//! which carry the authorization code), and the final status.

use std::io::Write;
use std::process::ExitCode;
use std::time::Duration;

use serde_json::json;
use url::Url;

const MAX_REDIRECTS: usize = 10;

fn without_query(url: &Url) -> String {
    let mut url = url.clone();
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

pub async fn main(args: &[String]) -> ExitCode {
    let [opened] = args else {
        eprintln!("usage: fusion-driver browser <url>");
        return ExitCode::from(2);
    };
    let log = match std::env::var("DRIVER_BROWSER_LOG") {
        Ok(log) => log,
        Err(_) => {
            eprintln!("fusion-driver browser: DRIVER_BROWSER_LOG is not set");
            return ExitCode::from(2);
        }
    };
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
        .expect("browser HTTP client");
    let mut url = match Url::parse(opened) {
        Ok(url) => url,
        Err(error) => {
            eprintln!("fusion-driver browser: {opened}: {error}");
            return ExitCode::from(2);
        }
    };
    let mut redirects = Vec::new();
    let status = loop {
        let response = match http.get(url.clone()).send().await {
            Ok(response) => response,
            Err(error) => {
                eprintln!(
                    "fusion-driver browser: GET {}: {error}",
                    without_query(&url)
                );
                return ExitCode::FAILURE;
            }
        };
        let status = response.status();
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|location| location.to_str().ok())
            .map(str::to_owned);
        match location {
            Some(location) if status.is_redirection() && redirects.len() < MAX_REDIRECTS => {
                url = match url.join(&location) {
                    Ok(next) => next,
                    Err(error) => {
                        eprintln!("fusion-driver browser: redirect to {location}: {error}");
                        return ExitCode::FAILURE;
                    }
                };
                redirects.push(without_query(&url));
            }
            _ => {
                // Read the page, as a browser does, so the handler's
                // response completes.
                let _ = response.bytes().await;
                break status;
            }
        }
    };
    let visit = json!({
        "opened": opened,
        "redirects": redirects,
        "landed": without_query(&url),
        "status": status.as_u16(),
    });
    let written = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .and_then(|mut file| writeln!(file, "{visit}"));
    if let Err(error) = written {
        eprintln!("fusion-driver browser: {log}: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
