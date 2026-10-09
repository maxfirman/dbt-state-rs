//! Backend-rendered lineage.
//!
//! Builds a petgraph DAG from the latest per-node decisions in an environment
//! (edges inferred by matching each node's upstream `input_tables` to other
//! nodes' `target_table`s), lays it out with the `dagrers` Sugiyama/dagre.js
//! layout, and renders an accessible inline SVG plus a text/list fallback.

use std::collections::HashMap;

use dagre_rs::{DagreLayout, LayoutOptions, RankDir};
use petgraph::graph::{DiGraph, NodeIndex};

use crate::db::DecisionRow;

const NODE_W: f32 = 180.0;
const NODE_H: f32 = 44.0;
const PAD: f32 = 24.0;

/// A laid-out node ready to render.
pub struct PlacedNode {
    pub id: String,
    pub label: String,
    pub decision: String,
    pub x: f32,
    pub y: f32,
}

/// A laid-out edge (centre-to-centre; the SVG draws an orthogonal-ish path).
pub struct PlacedEdge {
    pub x1: f32,
    pub y1: f32,
    pub x2: f32,
    pub y2: f32,
}

pub struct Layout {
    pub nodes: Vec<PlacedNode>,
    pub edges: Vec<PlacedEdge>,
    pub width: f32,
    pub height: f32,
}

/// Normalize a relation name to a comparison key (strip quotes + case).
fn norm(name: &str) -> String {
    name.replace('"', "").to_ascii_lowercase()
}

/// Build + lay out the lineage for a set of latest decisions.
pub fn build(decisions: &[DecisionRow]) -> Option<Layout> {
    if decisions.is_empty() {
        return None;
    }

    // Map a node's produced relation (target_table, normalized) -> node id.
    let mut produced: HashMap<String, String> = HashMap::new();
    for d in decisions {
        if let (Some(uid), Some(tt)) = (&d.node_unique_id, &d.target_table) {
            produced.insert(norm(tt), uid.clone());
        }
    }

    let mut graph: DiGraph<String, ()> = DiGraph::new();
    let mut index_of: HashMap<String, NodeIndex> = HashMap::new();
    let mut meta: HashMap<String, (String, String)> = HashMap::new(); // id -> (label, decision)

    for d in decisions {
        let Some(uid) = &d.node_unique_id else {
            continue;
        };
        let idx = *index_of
            .entry(uid.clone())
            .or_insert_with(|| graph.add_node(uid.clone()));
        let label = d
            .node_name
            .clone()
            .unwrap_or_else(|| uid.rsplit('.').next().unwrap_or(uid).to_string());
        meta.insert(uid.clone(), (label, d.decision.clone()));
        let _ = idx;
    }

    // Edges: upstream input relation -> this node, when the input is produced
    // by another known node in this environment.
    for d in decisions {
        let Some(uid) = &d.node_unique_id else {
            continue;
        };
        let inputs: Vec<String> =
            serde_json::from_value::<Vec<serde_json::Value>>(d.input_tables.clone())
                .unwrap_or_default()
                .into_iter()
                .filter_map(|t| t.get("name").and_then(|n| n.as_str()).map(norm))
                .collect();
        for inp in inputs {
            // Skip self-reference (a node's own target table appears in inputs).
            if let Some(src_uid) = produced.get(&inp)
                && src_uid != uid
            {
                let (Some(&a), Some(&b)) = (index_of.get(src_uid), index_of.get(uid)) else {
                    continue;
                };
                graph.update_edge(a, b, ());
            }
        }
    }

    let layout = DagreLayout::with_options(LayoutOptions {
        rank_dir: RankDir::LeftToRight,
        ..Default::default()
    })
    .compute(&graph);

    // Collect placed nodes.
    let mut nodes = Vec::new();
    for (uid, &idx) in &index_of {
        let (x, y) = layout
            .node_positions
            .get(&idx)
            .copied()
            .unwrap_or((0.0, 0.0));
        let (label, decision) = meta
            .get(uid)
            .cloned()
            .unwrap_or_else(|| (uid.clone(), "build".to_string()));
        nodes.push(PlacedNode {
            id: uid.clone(),
            label,
            decision,
            x: x + PAD,
            y: y + PAD,
        });
    }
    // Stable order for deterministic rendering.
    nodes.sort_by(|a, b| a.x.partial_cmp(&b.x).unwrap().then(a.id.cmp(&b.id)));

    // Collect placed edges (centre to centre).
    let pos = |idx: NodeIndex| layout.node_positions.get(&idx).copied();
    let mut edges = Vec::new();
    for e in graph.edge_indices() {
        if let Some((a, b)) = graph.edge_endpoints(e)
            && let (Some((x1, y1)), Some((x2, y2))) = (pos(a), pos(b))
        {
            edges.push(PlacedEdge {
                x1: x1 + PAD + NODE_W / 2.0,
                y1: y1 + PAD + NODE_H / 2.0,
                x2: x2 + PAD - NODE_W / 2.0,
                y2: y2 + PAD + NODE_H / 2.0,
            });
        }
    }

    Some(Layout {
        nodes,
        edges,
        width: layout.width + PAD * 2.0 + NODE_W,
        height: layout.height + PAD * 2.0 + NODE_H,
    })
}

/// Render the layout to an accessible inline SVG string. The SVG has
/// role="img" + <title>/<desc>; callers also render a text fallback list.
pub fn render_svg(layout: &Layout) -> String {
    let fill = |decision: &str| match decision {
        "skip" => "#e4f6ec",
        "clone" => "#efe8f8",
        _ => "#fdeede",
    };
    let stroke = |decision: &str| match decision {
        "skip" => "#1a7f4b",
        "clone" => "#6b3fa0",
        _ => "#b4530a",
    };

    let mut s = String::new();
    s.push_str(&format!(
        "<svg viewBox=\"0 0 {w:.0} {h:.0}\" width=\"{w:.0}\" height=\"{h:.0}\" \
         role=\"img\" aria-labelledby=\"lin-title lin-desc\" \
         xmlns=\"http://www.w3.org/2000/svg\" class=\"lineage-svg\">",
        w = layout.width.max(1.0),
        h = layout.height.max(1.0),
    ));
    s.push_str("<title id=\"lin-title\">Lineage graph</title>");
    s.push_str(&format!(
        "<desc id=\"lin-desc\">{} nodes, {} dependencies, laid out left to right.</desc>",
        layout.nodes.len(),
        layout.edges.len()
    ));
    s.push_str(
        "<defs><marker id=\"arrow\" markerWidth=\"8\" markerHeight=\"8\" refX=\"7\" refY=\"3\" \
         orient=\"auto\"><path d=\"M0,0 L7,3 L0,6 Z\" fill=\"#9aa4b2\"/></marker></defs>",
    );

    // Edges first (under nodes).
    for e in &layout.edges {
        let mx = (e.x1 + e.x2) / 2.0;
        s.push_str(&format!(
            "<path d=\"M{:.1},{:.1} C{:.1},{:.1} {:.1},{:.1} {:.1},{:.1}\" \
             fill=\"none\" stroke=\"#9aa4b2\" stroke-width=\"1.5\" marker-end=\"url(#arrow)\"/>",
            e.x1, e.y1, mx, e.y1, mx, e.y2, e.x2, e.y2
        ));
    }

    // Nodes.
    for n in &layout.nodes {
        let x = n.x - NODE_W / 2.0;
        let y = n.y;
        s.push_str(&format!(
            "<g><rect x=\"{:.1}\" y=\"{:.1}\" width=\"{:.0}\" height=\"{:.0}\" rx=\"8\" \
             fill=\"{}\" stroke=\"{}\" stroke-width=\"1.5\"/>\
             <text x=\"{:.1}\" y=\"{:.1}\" font-size=\"12\" font-family=\"system-ui,sans-serif\" \
             fill=\"#1b1f24\" text-anchor=\"middle\" dominant-baseline=\"middle\">{}</text></g>",
            x,
            y,
            NODE_W,
            NODE_H,
            fill(&n.decision),
            stroke(&n.decision),
            n.x,
            y + NODE_H / 2.0,
            escape(&n.label),
        ));
    }

    s.push_str("</svg>");
    s
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DecisionRow;

    fn row(
        name: &str,
        uid: &str,
        target: Option<&str>,
        inputs: &[&str],
        decision: &str,
    ) -> DecisionRow {
        let input_tables = serde_json::json!(
            inputs
                .iter()
                .map(|n| serde_json::json!({ "name": n, "last_modified_epoch": 0 }))
                .collect::<Vec<_>>()
        );
        DecisionRow {
            id: 1,
            node_name: Some(name.into()),
            node_unique_id: Some(uid.into()),
            node_fqn: None,
            resource_type: Some("model".into()),
            decision: decision.into(),
            is_stale: false,
            decision_description: None,
            target_table: target.map(|s| s.into()),
            node_body_hash: None,
            clone_source: None,
            execution_runtime_ms: None,
            input_tables,
            clone_sqls: None,
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn empty_input_yields_none() {
        assert!(build(&[]).is_none());
    }

    #[test]
    fn infers_edge_from_input_to_producer() {
        // stg produces "db.sch.stg"; mart consumes it -> edge stg -> mart.
        let decisions = vec![
            row(
                "stg",
                "model.p.stg",
                Some("\"DB\".\"S\".\"STG\""),
                &[],
                "build",
            ),
            row(
                "mart",
                "model.p.mart",
                Some("\"DB\".\"S\".\"MART\""),
                &["\"DB\".\"S\".\"STG\""],
                "skip",
            ),
        ];
        let layout = build(&decisions).expect("layout");
        assert_eq!(layout.nodes.len(), 2);
        assert_eq!(layout.edges.len(), 1, "one upstream->downstream edge");
    }

    #[test]
    fn self_reference_is_not_an_edge() {
        // A node whose own target table appears in its inputs must not self-edge.
        let decisions = vec![row(
            "inc",
            "model.p.inc",
            Some("\"DB\".\"S\".\"INC\""),
            &["\"DB\".\"S\".\"INC\""],
            "build",
        )];
        let layout = build(&decisions).expect("layout");
        assert_eq!(layout.nodes.len(), 1);
        assert_eq!(layout.edges.len(), 0);
    }

    #[test]
    fn svg_is_accessible_and_contains_nodes() {
        let decisions = vec![row(
            "stg",
            "model.p.stg",
            Some("\"DB\".\"S\".\"STG\""),
            &[],
            "build",
        )];
        let layout = build(&decisions).unwrap();
        let svg = render_svg(&layout);
        assert!(svg.contains("role=\"img\""));
        assert!(svg.contains("<title id=\"lin-title\">"));
        assert!(svg.contains("<desc id=\"lin-desc\">"));
        assert!(svg.contains("<rect"));
        assert!(svg.contains(">stg<"));
    }
}
