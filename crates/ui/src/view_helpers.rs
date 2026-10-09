//! Shared Topcoat components and formatting helpers used across pages.

use topcoat::{
    Result,
    view::{View, component, view},
};

/// A coloured status badge for a decision ("build" | "skip" | "clone").
/// Uses text + colour (never colour alone) for accessibility.
#[component]
pub async fn decision_badge(decision: &str) -> Result<impl View> {
    let (label, class) = match decision {
        "build" => ("Built", "badge badge-built"),
        "skip" => ("Reused", "badge badge-reused"),
        "clone" => ("Cloned", "badge badge-cloned"),
        other => (other, "badge"),
    };
    Ok(view! { <span class=(class)>(label)</span> })
}

/// A single summary statistic card.
#[component]
pub async fn stat_card(value: &str, label: &str, hint: &str) -> Result<impl View> {
    Ok(view! {
        <div class="card">
            <div class="card-value">(value)</div>
            <div class="card-label">(label)</div>
            <div class="card-hint">(hint)</div>
        </div>
    })
}

// ---- plain formatting helpers (not views) ----------------------------------

pub fn fmt_runtime(ms: Option<i64>) -> String {
    match ms {
        Some(ms) if ms >= 1000 => format!("{:.1}s", ms as f64 / 1000.0),
        Some(ms) => format!("{ms}ms"),
        None => "—".to_string(),
    }
}

pub fn fmt_time(t: chrono::DateTime<chrono::Utc>) -> String {
    t.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

pub fn short(id: &str) -> String {
    id.chars().take(8).collect()
}
