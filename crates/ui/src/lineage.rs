//! Backend-rendered lineage. Filled in by task 8 (dagre-rs layout -> SVG).
//! Placeholder kept minimal so the crate compiles while pages are built.

/// A node to lay out: logical id + display label + status (decision kind).
#[derive(Debug, Clone)]
pub struct LineageNode {
    pub id: String,
    pub label: String,
    pub decision: String,
}

/// A directed edge from upstream `from` to downstream `to` (by node id).
#[derive(Debug, Clone)]
pub struct LineageEdge {
    pub from: String,
    pub to: String,
}
