//! Machine notation serializer — deterministic canonical form
//!
//! Converts a `Machine` to its machine notation string representation.
//! The output is deterministic: the same graph always produces the same string.
//!
//! The canonical form is line-based (one statement per line, no brackets).
//!
//! ## Alias Generation
//!
//! Aliases are derived from the cap URN's `op=` tag value. If no `op=` tag
//! exists, aliases are generated as `edge_0`, `edge_1`, etc. Duplicate
//! aliases from identical op tags are disambiguated with numeric suffixes.
//!
//! ## Node Name Generation
//!
//! Node names are generated deterministically from topological position.
//! The first root source is `n0`, etc. Intermediate nodes get names based
//! on their topological order.
//!
//! ## Canonical Ordering
//!
//! Edges are sorted by (cap_urn canonical string, sources canonical, target canonical)
//! for stable output. Headers are emitted first (sorted by alias), then wirings
//! in the same edge order.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;

use crate::planner::live_cap_graph::{Strand, StrandStepType};
use crate::urn::media_urn::MediaUrn;

use super::error::MachineAbstractionError;
use super::graph::{MachineEdge, Machine};

/// Serialization format for machine notation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotationFormat {
    /// Line-based: one statement per line, no brackets.
    /// ```text
    /// extract cap:in="media:pdf";op=extract;out="media:txt;textable"
    /// doc -> extract -> text
    /// ```
    LineBased,
    /// Bracketed: each statement wrapped in `[...]`.
    /// ```text
    /// [extract cap:in="media:pdf";op=extract;out="media:txt;textable"]
    /// [doc -> extract -> text]
    /// ```
    Bracketed,
}

impl Machine {
    /// Convert a `Strand` (resolved linear path) into a `Machine`.
    ///
    /// The conversion:
    /// - Each `Cap` step becomes a `MachineEdge` with a single source.
    /// - `ForEach` steps set `is_loop: true` on the next Cap edge.
    /// - `Collect` steps are elided (implicit in transitions).
    ///
    /// ## Source URN chaining
    ///
    /// Each cap step carries its cap-declared `from_spec`, which is
    /// a pattern the cap's input conforms to — NOT necessarily the
    /// exact URN of the preceding cap's output. For example,
    /// `Disbind` may produce `media:page;textable` while
    /// `MakeDecision` declares its input as `media:textable`. The
    /// planner links them because `media:page;textable` conforms
    /// to `media:textable`, but using `from_spec` verbatim as the
    /// edge's source URN produces a machine with disconnected
    /// strands (each cap has a different media URN as source/
    /// target, so the serializer's node-naming emits two parallel
    /// chains instead of one).
    ///
    /// To preserve linear topology through the serializer, the
    /// source URN of each cap edge is set to the PRECEDING cap
    /// step's target URN (which is the exact URN flowing into the
    /// next cap at runtime). The very first cap uses the strand's
    /// `source_spec` as its source.
    pub fn from_path(path: &Strand) -> Result<Self, MachineAbstractionError> {
        let mut edges = Vec::new();
        let mut pending_loop = false;
        // Track the URN of the most recent cap's output so the
        // next cap's source URN can be pinned to it — this is
        // what wires successive caps into a single linear strand
        // in the serializer's node-naming pass.
        let mut prev_target: Option<MediaUrn> = None;

        for step in &path.steps {
            match &step.step_type {
                StrandStepType::Cap { cap_urn, .. } => {
                    let source = prev_target
                        .clone()
                        .unwrap_or_else(|| path.source_spec.clone());
                    edges.push(MachineEdge {
                        sources: vec![source],
                        cap_urn: cap_urn.clone(),
                        target: step.to_spec.clone(),
                        is_loop: pending_loop,
                    });
                    pending_loop = false;
                    prev_target = Some(step.to_spec.clone());
                }
                StrandStepType::ForEach { .. } => {
                    pending_loop = true;
                }
                StrandStepType::Collect { .. } => {
                    // Elided — cardinality transitions are implicit
                }
            }
        }

        if edges.is_empty() {
            return Err(MachineAbstractionError::NoCapabilitySteps);
        }

        Ok(Self::new(edges))
    }

    /// Serialize this machine graph to canonical bracketed machine notation.
    ///
    /// The output is deterministic: same graph → same string. This is the
    /// primary serialization format for accessibility identifiers and
    /// comparison. One-line, each statement wrapped in `[...]`.
    pub fn to_machine_notation(&self) -> String {
        self.to_machine_notation_formatted(NotationFormat::Bracketed)
    }

    /// Serialize to multi-line machine notation (one statement per line).
    /// Uses the same format as `to_machine_notation()` (bracketed) but
    /// with newlines between statements.
    pub fn to_machine_notation_multiline(&self) -> String {
        if self.edges().is_empty() {
            return String::new();
        }

        let (aliases, node_names, edge_order) = self.build_serialization_maps();
        let mut output = String::new();

        let mut sorted_aliases: Vec<(&String, &(usize, String))> = aliases.iter().collect();
        sorted_aliases.sort_by_key(|(alias, _)| *alias);

        for (alias, (edge_idx, _)) in &sorted_aliases {
            let edge = &self.edges()[*edge_idx];
            writeln!(output, "[{} {}]", alias, edge.cap_urn).unwrap();
        }

        for edge_idx in &edge_order {
            let edge = &self.edges()[*edge_idx];
            let (alias, _) = aliases.iter().find(|(_, (idx, _))| idx == edge_idx).unwrap();

            let sources: Vec<&String> = edge.sources.iter().map(|s| {
                let key = s.to_string();
                node_names.get(&key).unwrap()
            }).collect();

            let target_key = edge.target.to_string();
            let target_name = node_names.get(&target_key).unwrap();

            let loop_prefix = if edge.is_loop { "LOOP " } else { "" };

            if sources.len() == 1 {
                writeln!(output, "[{} -> {}{} -> {}]", sources[0], loop_prefix, alias, target_name).unwrap();
            } else {
                let group = sources.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ");
                writeln!(output, "[({}) -> {}{} -> {}]", group, loop_prefix, alias, target_name).unwrap();
            }
        }

        if output.ends_with('\n') {
            output.pop();
        }

        output
    }

    /// Serialize this machine graph to machine notation in the specified format.
    ///
    /// The output is deterministic: same graph + same format → same string.
    pub fn to_machine_notation_formatted(&self, format: NotationFormat) -> String {
        if self.edges().is_empty() {
            return String::new();
        }

        let (aliases, node_names, edge_order) = self.build_serialization_maps();
        let mut output = String::new();

        let (open, close, sep) = match format {
            NotationFormat::Bracketed => ("[", "]", ""),
            NotationFormat::LineBased => ("", "", "\n"),
        };

        // Emit headers in alias-sorted order
        let mut sorted_aliases: Vec<(&String, &(usize, String))> = aliases.iter().collect();
        sorted_aliases.sort_by_key(|(alias, _)| *alias);

        for (alias, (edge_idx, _cap_str)) in &sorted_aliases {
            let edge = &self.edges()[*edge_idx];
            write!(output, "{}{} {}{}{}", open, alias, edge.cap_urn, close, sep).unwrap();
        }

        // Emit wirings in edge order
        for edge_idx in &edge_order {
            let edge = &self.edges()[*edge_idx];
            let (alias, _) = aliases.iter().find(|(_, (idx, _))| idx == edge_idx).unwrap();

            let sources: Vec<&String> = edge.sources.iter().map(|s| {
                let key = s.to_string();
                node_names.get(&key).unwrap()
            }).collect();

            let target_key = edge.target.to_string();
            let target_name = node_names.get(&target_key).unwrap();

            let loop_prefix = if edge.is_loop { "LOOP " } else { "" };

            if sources.len() == 1 {
                write!(output, "{}{} -> {}{} -> {}{}{}", open, sources[0], loop_prefix, alias, target_name, close, sep).unwrap();
            } else {
                let group = sources.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", ");
                write!(output, "{}({}) -> {}{} -> {}{}{}", open, group, loop_prefix, alias, target_name, close, sep).unwrap();
            }
        }

        // Remove trailing separator
        if format == NotationFormat::LineBased && output.ends_with('\n') {
            output.pop();
        }

        output
    }

    /// Topological sort of the edges by data-flow dependency.
    ///
    /// Edge A precedes edge B iff some URN in B.sources equals
    /// A.target. Ties (edges at the same topological "level") are
    /// broken lexicographically by cap URN so two semantically
    /// equivalent machines produce byte-identical output
    /// regardless of the order edges were added.
    ///
    /// The input is the `Machine`'s raw edge vector. The output
    /// is a permutation of `0..edges.len()` ordered such that
    /// every edge's predecessors appear before it.
    ///
    /// If the graph has a cycle (which a well-formed `Machine`
    /// should never have — all DAGs by contract), any edges that
    /// can't be ordered fall at the end in cap-URN order. This
    /// is a fail-visible fallback: the serialized notation will
    /// parse but round-trip to a non-equivalent graph, signaling
    /// the caller that their `Machine` had a structural problem.
    fn topological_edge_order(edges: &[MachineEdge]) -> Vec<usize> {
        use std::collections::VecDeque;

        let n = edges.len();
        if n == 0 {
            return Vec::new();
        }

        // Build the predecessor relation: for each edge B, which
        // other edges A have A.target equal to one of B.sources?
        // Because `MediaUrn::is_equivalent` is the canonical
        // identity check, we compare via that — not by raw
        // string equality on `.to_string()`.
        let mut predecessors: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut successors: Vec<Vec<usize>> = vec![Vec::new(); n];
        let mut indegree: Vec<usize> = vec![0; n];

        for (b_idx, b) in edges.iter().enumerate() {
            for (a_idx, a) in edges.iter().enumerate() {
                if a_idx == b_idx {
                    continue;
                }
                let matches = b.sources.iter().any(|src| {
                    match a.target.is_equivalent(src) {
                        Ok(eq) => eq,
                        Err(_) => false,
                    }
                });
                if matches {
                    predecessors[b_idx].push(a_idx);
                    successors[a_idx].push(b_idx);
                    indegree[b_idx] += 1;
                }
            }
        }

        // Kahn's algorithm: repeatedly remove a zero-indegree
        // edge, breaking ties by cap URN string. Edges whose
        // source URN is consumed by another edge's target wait
        // their turn.
        let mut result: Vec<usize> = Vec::with_capacity(n);
        let mut ready: VecDeque<usize> = edges
            .iter()
            .enumerate()
            .filter_map(|(i, _)| if indegree[i] == 0 { Some(i) } else { None })
            .collect();

        // Maintain `ready` sorted by cap URN so the first pop is
        // deterministic. Sort it once before the loop and
        // re-sort on each insertion (the set is small — a
        // machine with more than a few dozen edges is unusual).
        fn sort_ready(ready: &mut VecDeque<usize>, edges: &[MachineEdge]) {
            let mut v: Vec<usize> = ready.drain(..).collect();
            v.sort_by(|a, b| {
                let ea = &edges[*a];
                let eb = &edges[*b];
                let cap_cmp = ea.cap_urn.to_string().cmp(&eb.cap_urn.to_string());
                if cap_cmp != std::cmp::Ordering::Equal {
                    return cap_cmp;
                }
                let src_a: Vec<String> = ea.sources.iter().map(|s| s.to_string()).collect();
                let src_b: Vec<String> = eb.sources.iter().map(|s| s.to_string()).collect();
                let src_cmp = src_a.cmp(&src_b);
                if src_cmp != std::cmp::Ordering::Equal {
                    return src_cmp;
                }
                ea.target.to_string().cmp(&eb.target.to_string())
            });
            ready.extend(v);
        }

        sort_ready(&mut ready, edges);

        while let Some(idx) = ready.pop_front() {
            result.push(idx);
            for &succ in &successors[idx] {
                indegree[succ] -= 1;
                if indegree[succ] == 0 {
                    ready.push_back(succ);
                }
            }
            sort_ready(&mut ready, edges);
        }

        // If the DAG has a cycle, some edges will never reach
        // zero in-degree. Append them in cap-URN order so the
        // output is still deterministic (and the cycle is
        // visible to whoever inspects the notation).
        if result.len() < n {
            let mut remaining: Vec<usize> = (0..n)
                .filter(|i| !result.contains(i))
                .collect();
            remaining.sort_by(|a, b| {
                edges[*a].cap_urn.to_string().cmp(&edges[*b].cap_urn.to_string())
            });
            result.extend(remaining);
        }

        result
    }

    /// Build the alias map, node name map, and edge ordering for serialization.
    ///
    /// Returns:
    /// - `aliases`: alias → (edge_index, cap_urn_string)
    /// - `node_names`: media_urn_canonical_string → node_name
    /// - `edge_order`: edge indices in canonical order
    ///
    /// Edge ordering is a **topological sort** of the data-flow
    /// DAG: an edge A precedes an edge B if A.target matches any
    /// URN in B.sources. This produces a linear reading order
    /// when the machine IS linear (upstream caps come first),
    /// and respects partial order in fan-out / fan-in. Ties at
    /// the same topological level are broken lexicographically
    /// by cap URN for determinism — the existing
    /// `reordered_edges_produce_same_notation` contract still
    /// holds because the underlying predecessor relation is a
    /// property of the edge set, not the construction order.
    fn build_serialization_maps(&self) -> (BTreeMap<String, (usize, String)>, HashMap<String, String>, Vec<usize>) {
        // Step 1: Topological sort with deterministic tiebreaker.
        let edge_order = Self::topological_edge_order(self.edges());

        // Step 2: Generate aliases from op= tag
        let mut aliases: BTreeMap<String, (usize, String)> = BTreeMap::new();
        let mut alias_counts: HashMap<String, usize> = HashMap::new();

        for &idx in &edge_order {
            let edge = &self.edges()[idx];
            let base_alias = edge.cap_urn.get_tag("op")
                .map(|s| s.clone())
                .unwrap_or_else(|| format!("edge_{}", idx));

            let count = alias_counts.entry(base_alias.clone()).or_insert(0);
            let alias = if *count == 0 {
                base_alias.clone()
            } else {
                format!("{}_{}", base_alias, count)
            };
            *count += 1;

            let cap_str = edge.cap_urn.to_string();
            aliases.insert(alias, (idx, cap_str));
        }

        // Step 3: Generate node names
        // Collect all unique media URNs, assign names in order of first appearance
        let mut node_names: HashMap<String, String> = HashMap::new();
        let mut node_counter = 0usize;

        for &idx in &edge_order {
            let edge = &self.edges()[idx];
            for src in &edge.sources {
                let key = src.to_string();
                if !node_names.contains_key(&key) {
                    node_names.insert(key, format!("n{}", node_counter));
                    node_counter += 1;
                }
            }
            let target_key = edge.target.to_string();
            if !node_names.contains_key(&target_key) {
                node_names.insert(target_key, format!("n{}", node_counter));
                node_counter += 1;
            }
        }

        (aliases, node_names, edge_order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::urn::cap_urn::CapUrn;

    fn media(s: &str) -> MediaUrn {
        MediaUrn::from_string(s).unwrap()
    }

    fn cap(s: &str) -> CapUrn {
        CapUrn::from_string(s).unwrap()
    }

    fn edge(sources: &[&str], cap_str: &str, target: &str, is_loop: bool) -> MachineEdge {
        MachineEdge {
            sources: sources.iter().map(|s| media(s)).collect(),
            cap_urn: cap(cap_str),
            target: media(target),
            is_loop,
        }
    }

    // =========================================================================
    // Basic serialization
    // =========================================================================

    #[test]
    fn serialize_single_edge() {
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = g.to_machine_notation();
        assert!(notation.contains("[extract "));
        assert!(notation.contains("-> extract ->"));
        assert!(notation.contains("[n0 ->"));
        assert!(notation.contains("-> n1]"));
    }

    #[test]
    fn serialize_two_edge_chain() {
        let g = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let notation = g.to_machine_notation();
        // Should have 2 headers and 2 wirings
        let bracket_count = notation.matches('[').count();
        assert_eq!(bracket_count, 4); // 2 headers + 2 wirings
    }

    #[test]
    fn serialize_empty_graph() {
        let g = Machine::empty();
        assert_eq!(g.to_machine_notation(), "");
    }

    // =========================================================================
    // Round-trip: serialize → parse → compare
    // =========================================================================

    #[test]
    fn roundtrip_single_edge() {
        let original = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = original.to_machine_notation();
        let reparsed = Machine::from_string(&notation).unwrap();
        assert!(
            original.is_equivalent(&reparsed),
            "Round-trip failed:\n  original: {:?}\n  notation: {}\n  reparsed: {:?}",
            original, notation, reparsed
        );
    }

    #[test]
    fn roundtrip_two_edge_chain() {
        let original = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let notation = original.to_machine_notation();
        let reparsed = Machine::from_string(&notation).unwrap();
        assert!(
            original.is_equivalent(&reparsed),
            "Round-trip failed:\n  notation: {}\n  original edges: {}\n  reparsed edges: {}",
            notation, original.edge_count(), reparsed.edge_count()
        );
    }

    #[test]
    fn roundtrip_fan_out() {
        let original = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract_metadata;out=\"media:file-metadata;record;textable\"",
                "media:file-metadata;record;textable",
                false,
            ),
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract_outline;out=\"media:document-outline;record;textable\"",
                "media:document-outline;record;textable",
                false,
            ),
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=generate_thumbnail;out=\"media:image;png;thumbnail\"",
                "media:image;png;thumbnail",
                false,
            ),
        ]);
        let notation = original.to_machine_notation();
        let reparsed = Machine::from_string(&notation).unwrap();
        assert!(
            original.is_equivalent(&reparsed),
            "Fan-out round-trip failed:\n  notation: {}",
            notation
        );
    }

    #[test]
    fn roundtrip_loop_edge() {
        let original = Machine::new(vec![edge(
            &["media:disbound-page;textable"],
            "cap:in=\"media:disbound-page;textable\";op=page_to_text;out=\"media:txt;textable\"",
            "media:txt;textable",
            true,
        )]);
        let notation = original.to_machine_notation();
        let reparsed = Machine::from_string(&notation).unwrap();
        assert!(original.is_equivalent(&reparsed));
        assert!(reparsed.edges()[0].is_loop);
    }

    // =========================================================================
    // Determinism
    // =========================================================================

    #[test]
    fn serialization_is_deterministic() {
        let g = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let n1 = g.to_machine_notation();
        let n2 = g.to_machine_notation();
        assert_eq!(n1, n2, "Serialization must be deterministic");
    }

    #[test]
    fn reordered_edges_produce_same_notation() {
        let g1 = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        // Same edges, reversed order
        let g2 = Machine::new(vec![
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
        ]);
        assert_eq!(
            g1.to_machine_notation(),
            g2.to_machine_notation(),
            "Same graph with reordered edges must produce identical notation"
        );
    }

    // =========================================================================
    // Multi-line format
    // =========================================================================

    #[test]
    fn multiline_format() {
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let multi = g.to_machine_notation_multiline();
        assert!(multi.contains('\n'), "Multi-line format must contain newlines");

        // Should still round-trip
        let reparsed = Machine::from_string(&multi).unwrap();
        assert!(g.is_equivalent(&reparsed));
    }

    // =========================================================================
    // Alias generation
    // =========================================================================

    #[test]
    fn alias_from_op_tag() {
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = g.to_machine_notation();
        // Should use "extract" as alias (from op= tag)
        assert!(notation.contains("[extract "), "Expected 'extract' alias, got: {}", notation);
    }

    #[test]
    fn alias_fallback_without_op_tag() {
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = g.to_machine_notation();
        // Should use fallback alias "edge_N"
        assert!(notation.contains("edge_"), "Expected fallback alias, got: {}", notation);
    }

    #[test]
    fn duplicate_op_tags_disambiguated() {
        let g = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:json;record;textable\"",
                "media:json;record;textable",
                false,
            ),
        ]);
        let notation = g.to_machine_notation();
        // Should have "extract" and "extract_1"
        assert!(notation.contains("extract_1") || notation.contains("extract_2"),
            "Duplicate ops must be disambiguated: {}", notation);
    }

    // =========================================================================
    // Line-based format
    // =========================================================================

    #[test]
    fn line_based_format_single_edge() {
        let g = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = g.to_machine_notation_formatted(NotationFormat::LineBased);
        // No brackets
        assert!(!notation.contains('['), "Line-based format must not contain brackets: {}", notation);
        assert!(!notation.contains(']'), "Line-based format must not contain brackets: {}", notation);
        // Contains content
        assert!(notation.contains("extract cap:"));
        assert!(notation.contains("-> extract ->"));
    }

    #[test]
    fn line_based_roundtrip_single_edge() {
        let original = Machine::new(vec![edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
            "media:txt;textable",
            false,
        )]);
        let notation = original.to_machine_notation_formatted(NotationFormat::LineBased);
        let reparsed = Machine::from_string(&notation).unwrap();
        assert!(
            original.is_equivalent(&reparsed),
            "Line-based round-trip failed:\n  notation: {}\n  original: {:?}\n  reparsed: {:?}",
            notation, original, reparsed
        );
    }

    #[test]
    fn line_based_roundtrip_two_edge_chain() {
        let original = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let notation = original.to_machine_notation_formatted(NotationFormat::LineBased);
        let reparsed = Machine::from_string(&notation).unwrap();
        assert!(
            original.is_equivalent(&reparsed),
            "Line-based round-trip failed:\n  notation: {}",
            notation
        );
    }

    #[test]
    fn line_based_roundtrip_loop() {
        let original = Machine::new(vec![edge(
            &["media:disbound-page;textable"],
            "cap:in=\"media:disbound-page;textable\";op=page_to_text;out=\"media:txt;textable\"",
            "media:txt;textable",
            true,
        )]);
        let notation = original.to_machine_notation_formatted(NotationFormat::LineBased);
        let reparsed = Machine::from_string(&notation).unwrap();
        assert!(original.is_equivalent(&reparsed));
        assert!(reparsed.edges()[0].is_loop);
    }

    #[test]
    fn line_based_deterministic() {
        let g = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let n1 = g.to_machine_notation_formatted(NotationFormat::LineBased);
        let n2 = g.to_machine_notation_formatted(NotationFormat::LineBased);
        assert_eq!(n1, n2);
    }

    #[test]
    fn line_based_and_bracketed_parse_to_same_graph() {
        let g = Machine::new(vec![
            edge(
                &["media:pdf"],
                "cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\"",
                "media:txt;textable",
                false,
            ),
            edge(
                &["media:txt;textable"],
                "cap:in=\"media:txt;textable\";op=embed;out=\"media:embedding-vector;record;textable\"",
                "media:embedding-vector;record;textable",
                false,
            ),
        ]);
        let bracketed = g.to_machine_notation_formatted(NotationFormat::Bracketed);
        let line_based = g.to_machine_notation_formatted(NotationFormat::LineBased);

        let g_bracketed = Machine::from_string(&bracketed).unwrap();
        let g_line_based = Machine::from_string(&line_based).unwrap();
        assert!(
            g_bracketed.is_equivalent(&g_line_based),
            "Bracketed and line-based must parse to equivalent graphs"
        );
    }

    // =========================================================================
    // from_path conversion
    // =========================================================================

    #[test]
    fn from_path_simple() {
        use crate::planner::live_cap_graph::{StrandStep, StrandStepType};

        let path = Strand {
            steps: vec![StrandStep {
                step_type: StrandStepType::Cap {
                    cap_urn: cap("cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\""),
                    title: "Extract Text".to_string(),
                    specificity: 5,
                    input_is_sequence: false,
                    output_is_sequence: false,
                },
                from_spec: media("media:pdf"),
                to_spec: media("media:txt;textable"),
            }],
            source_spec: media("media:pdf"),
            target_spec: media("media:txt;textable"),
            total_steps: 1,
            cap_step_count: 1,
            description: "Extract Text".to_string(),
        };

        let graph = Machine::from_path(&path).unwrap();
        assert_eq!(graph.edge_count(), 1);
        assert!(!graph.edges()[0].is_loop);
    }

    #[test]
    fn from_path_with_foreach() {
        use crate::planner::live_cap_graph::{StrandStep, StrandStepType};

        let path = Strand {
            steps: vec![
                StrandStep {
                    step_type: StrandStepType::ForEach {
                        media_spec: media("media:disbound-page;textable"),
                    },
                    from_spec: media("media:disbound-page;textable"),
                    to_spec: media("media:disbound-page;textable"),
                },
                StrandStep {
                    step_type: StrandStepType::Cap {
                        cap_urn: cap("cap:in=\"media:disbound-page;textable\";op=page_to_text;out=\"media:txt;textable\""),
                        title: "Page to Text".to_string(),
                        specificity: 4,
                        input_is_sequence: false,
                        output_is_sequence: false,
                    },
                    from_spec: media("media:disbound-page;textable"),
                    to_spec: media("media:txt;textable"),
                },
                StrandStep {
                    step_type: StrandStepType::Collect {
                        media_spec: media("media:txt;textable"),
                    },
                    from_spec: media("media:txt;textable"),
                    to_spec: media("media:txt;textable"),
                },
            ],
            source_spec: media("media:disbound-page;textable"),
            target_spec: media("media:txt;textable"),
            total_steps: 3,
            cap_step_count: 1,
            description: "ForEach → Page to Text → Collect".to_string(),
        };

        let graph = Machine::from_path(&path).unwrap();
        // ForEach + Cap + Collect → 1 edge with is_loop=true
        assert_eq!(graph.edge_count(), 1);
        assert!(graph.edges()[0].is_loop, "ForEach step must set is_loop on next Cap edge");
    }

    #[test]
    fn from_path_collect_elided() {
        use crate::planner::live_cap_graph::{StrandStep, StrandStepType};

        let path = Strand {
            steps: vec![
                StrandStep {
                    step_type: StrandStepType::Cap {
                        cap_urn: cap("cap:in=\"media:pdf\";op=extract;out=\"media:txt;textable\""),
                        title: "Extract".to_string(),
                        specificity: 5,
                    input_is_sequence: false,
                    output_is_sequence: false,
                    },
                    from_spec: media("media:pdf"),
                    to_spec: media("media:txt;textable"),
                },
                StrandStep {
                    step_type: StrandStepType::Collect {
                        media_spec: media("media:txt;textable"),
                    },
                    from_spec: media("media:txt;textable"),
                    to_spec: media("media:txt;textable"),
                },
            ],
            source_spec: media("media:pdf"),
            target_spec: media("media:txt;textable"),
            total_steps: 2,
            cap_step_count: 1,
            description: "Extract → Collect".to_string(),
        };

        let graph = Machine::from_path(&path).unwrap();
        // Collect is elided — only the Cap edge remains
        assert_eq!(graph.edge_count(), 1);
        assert!(!graph.edges()[0].is_loop);
    }

    #[test]
    fn from_path_structural_only_fails() {
        use crate::planner::live_cap_graph::{StrandStep, StrandStepType};

        let path = Strand {
            steps: vec![StrandStep {
                step_type: StrandStepType::ForEach {
                    media_spec: media("media:decision;json;record;textable"),
                },
                from_spec: media("media:decision;json;record;textable"),
                to_spec: media("media:decision;json;record;textable"),
            }],
            source_spec: media("media:decision;json;record;textable"),
            target_spec: media("media:decision;json;record;textable"),
            total_steps: 1,
            cap_step_count: 0,
            description: "ForEach".to_string(),
        };

        assert!(matches!(
            Machine::from_path(&path),
            Err(MachineAbstractionError::NoCapabilitySteps)
        ));
    }

    // =========================================================================
    // Serializer respects topology, not URN string equality
    // =========================================================================

    /// Real-world case that triggered the topological-sort fix:
    /// `[Disbind, ForEach, make_decision]`. Disbind outputs
    /// `media:page;textable`; make_decision declares its input
    /// as `media:textable`. The URNs differ by string, but the
    /// planner links them via conformance because ForEach unwraps
    /// the sequence item. The serializer must produce a **single
    /// linear strand** n0 -> disbind -> n1, n1 -> LOOP make_decision -> n2
    /// — not two disconnected strands.
    #[test]
    fn from_path_chains_disbind_foreach_make_decision_into_single_strand() {
        use crate::planner::live_cap_graph::{StrandStep, StrandStepType};

        let path = Strand {
            steps: vec![
                StrandStep {
                    step_type: StrandStepType::Cap {
                        cap_urn: cap("cap:in=\"media:pdf\";op=disbind;out=\"media:page;textable\""),
                        title: "Disbind".to_string(),
                        specificity: 5,
                        input_is_sequence: false,
                        output_is_sequence: true,
                    },
                    from_spec: media("media:pdf"),
                    to_spec: media("media:page;textable"),
                },
                StrandStep {
                    step_type: StrandStepType::ForEach {
                        media_spec: media("media:page;textable"),
                    },
                    from_spec: media("media:page;textable"),
                    to_spec: media("media:page;textable"),
                },
                StrandStep {
                    step_type: StrandStepType::Cap {
                        // Note: cap's declared from_spec is
                        // `media:textable`, NOT
                        // `media:page;textable`. The from_path
                        // chaining fix must override this with
                        // the preceding cap's target URN so the
                        // serializer sees a single chain.
                        cap_urn: cap("cap:constrained;in=\"media:textable\";op=make_decision;out=\"media:decision;json;record;textable\""),
                        title: "Make a Decision".to_string(),
                        specificity: 4,
                        input_is_sequence: false,
                        output_is_sequence: false,
                    },
                    from_spec: media("media:textable"),
                    to_spec: media("media:decision;json;record;textable"),
                },
            ],
            source_spec: media("media:pdf"),
            target_spec: media("media:decision;json;record;textable"),
            total_steps: 3,
            cap_step_count: 2,
            description: "Disbind → ForEach → Make a Decision".to_string(),
        };

        let graph = Machine::from_path(&path).unwrap();
        assert_eq!(graph.edge_count(), 2);

        // First edge: disbind, sources=[media:pdf], target=media:page;textable, not looped
        let disbind = &graph.edges()[0];
        assert_eq!(disbind.sources.len(), 1);
        assert_eq!(disbind.sources[0].to_string(), media("media:pdf").to_string());
        assert_eq!(disbind.target.to_string(), media("media:page;textable").to_string());
        assert!(!disbind.is_loop);

        // Second edge: make_decision, sources MUST be the
        // preceding cap's target (media:page;textable), NOT the
        // cap's declared from_spec (media:textable). This is the
        // chaining fix that allows a single linear strand.
        let make_decision = &graph.edges()[1];
        assert_eq!(make_decision.sources.len(), 1);
        assert_eq!(
            make_decision.sources[0].to_string(),
            media("media:page;textable").to_string(),
            "make_decision's source must be pinned to disbind's target (ForEach chaining)"
        );
        assert_eq!(
            make_decision.target.to_string(),
            media("media:decision;json;record;textable").to_string()
        );
        assert!(make_decision.is_loop, "make_decision must be LOOP (inside ForEach)");

        // Serialize and assert the notation is a single connected strand.
        let notation = graph.to_machine_notation_multiline();
        assert!(
            notation.contains("[n0 -> disbind -> n1]"),
            "Expected 'n0 -> disbind -> n1' in notation:\n{}",
            notation
        );
        assert!(
            notation.contains("[n1 -> LOOP make_decision -> n2]"),
            "Expected 'n1 -> LOOP make_decision -> n2' in notation:\n{}",
            notation
        );
    }

    /// The reorder-determinism contract must still hold even
    /// when edges are chained through the ForEach fix. Two
    /// Machines built from the same edge set in different orders
    /// must serialize identically under the topological sort.
    #[test]
    fn reordered_chained_edges_produce_same_notation() {
        let disbind_edge = edge(
            &["media:pdf"],
            "cap:in=\"media:pdf\";op=disbind;out=\"media:page;textable\"",
            "media:page;textable",
            false,
        );
        let make_decision_edge = edge(
            &["media:page;textable"],
            "cap:constrained;in=\"media:textable\";op=make_decision;out=\"media:decision;json;record;textable\"",
            "media:decision;json;record;textable",
            true,
        );

        let g1 = Machine::new(vec![disbind_edge.clone(), make_decision_edge.clone()]);
        let g2 = Machine::new(vec![make_decision_edge, disbind_edge]);
        assert_eq!(
            g1.to_machine_notation(),
            g2.to_machine_notation(),
            "Topological sort must produce identical output regardless of input edge order"
        );
    }

    /// Topological sort respects partial order — a downstream
    /// edge whose sources match an upstream edge's target must
    /// come second, even if its cap URN sorts first
    /// alphabetically. This test pins that the topological
    /// predecessor relation dominates the cap-URN tiebreaker.
    #[test]
    fn topological_sort_dominates_alphabetical_cap_urn() {
        // "aaa_first" sorts before "zzz_second" alphabetically,
        // but "zzz_second" produces the URN that "aaa_first"
        // consumes, so topologically zzz_second must come first.
        let g = Machine::new(vec![
            edge(
                &["media:middle"],
                "cap:in=\"media:middle\";op=aaa_first;out=\"media:end\"",
                "media:end",
                false,
            ),
            edge(
                &["media:start"],
                "cap:in=\"media:start\";op=zzz_second;out=\"media:middle\"",
                "media:middle",
                false,
            ),
        ]);
        let notation = g.to_machine_notation_multiline();
        // In the wiring section, zzz_second must appear before
        // aaa_first because it's the topological predecessor.
        let zzz_pos = notation.find("-> zzz_second ->").expect("zzz_second wiring missing");
        let aaa_pos = notation.find("-> aaa_first ->").expect("aaa_first wiring missing");
        assert!(
            zzz_pos < aaa_pos,
            "zzz_second (upstream) must appear before aaa_first (downstream) in:\n{}",
            notation
        );
    }
}
