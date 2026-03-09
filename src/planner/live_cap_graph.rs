//! LiveCapGraph — Precomputed capability graph for path finding
//!
//! This module provides a live, incrementally-updated graph of capabilities
//! for efficient path finding and reachability queries. Unlike CapPlanBuilder
//! which rebuilds the graph for each query, LiveCapGraph maintains a persistent
//! graph structure that is updated when capabilities change.
//!
//! ## Design Principles
//!
//! 1. **Typed URNs**: Store MediaUrn and CapUrn directly, not strings.
//!    This avoids reparsing and provides order-theoretic methods.
//!
//! 2. **Exact matching**: For target matching, use `is_equivalent()` not `conforms_to()`.
//!    This ensures "media:X" does NOT match paths ending in "media:X;list".
//!
//! 3. **Conformance for traversal**: Use `conforms_to()` only for graph traversal
//!    (can this output feed into that input?).
//!
//! 4. **Deterministic ordering**: Results are sorted by (path_length, specificity, urn).

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::cap::registry::CapRegistry;
use crate::urn::cap_urn::CapUrn;
use crate::urn::media_urn::MediaUrn;
use crate::Cap;

// =============================================================================
// DATA STRUCTURES
// =============================================================================

/// An edge in the live capability graph.
///
/// Each edge represents a capability that transforms from one media type to another.
/// URNs are stored as typed values, not strings, for efficient order-theoretic operations.
#[derive(Debug, Clone)]
pub struct LiveCapEdge {
    /// Input media type (what the cap consumes)
    pub from_spec: MediaUrn,
    /// Output media type (what the cap produces)
    pub to_spec: MediaUrn,
    /// The capability URN
    pub cap_urn: CapUrn,
    /// Human-readable title for display
    pub cap_title: String,
    /// Specificity score (number of non-wildcard tags)
    pub specificity: usize,
}

/// Precomputed graph of capabilities for path finding.
///
/// This graph is designed to be:
/// - Updated incrementally when caps change
/// - Queried efficiently for reachability and path finding
/// - Deterministic in its results
#[derive(Debug)]
pub struct LiveCapGraph {
    /// All edges in the graph
    edges: Vec<LiveCapEdge>,
    /// Index: from_spec (canonical string) → edge indices
    /// Uses canonical string as key because MediaUrn doesn't implement Hash
    outgoing: HashMap<String, Vec<usize>>,
    /// Index: to_spec (canonical string) → edge indices
    incoming: HashMap<String, Vec<usize>>,
    /// All unique media URN nodes (canonical strings)
    nodes: HashSet<String>,
    /// Cap URN (canonical string) → edge indices for removal
    cap_to_edges: HashMap<String, Vec<usize>>,
}

/// Information about a reachable target from a source media type.
#[derive(Debug, Clone)]
pub struct ReachableTargetInfo {
    /// The target media URN
    pub media_spec: MediaUrn,
    /// Human-readable display name (from media registry)
    pub display_name: String,
    /// Minimum number of steps to reach this target
    pub min_path_length: i32,
    /// Number of distinct paths to this target
    pub path_count: i32,
}

/// Information about a single step in a capability chain path.
#[derive(Debug, Clone)]
pub struct CapChainStepInfo {
    /// The capability URN for this step
    pub cap_urn: CapUrn,
    /// Input media type for this step
    pub from_spec: MediaUrn,
    /// Output media type for this step
    pub to_spec: MediaUrn,
    /// Human-readable title
    pub title: String,
    /// Specificity of this cap (for ordering)
    pub specificity: usize,
}

/// Information about a complete capability chain path.
#[derive(Debug, Clone)]
pub struct CapChainPathInfo {
    /// Steps in the path, in order
    pub steps: Vec<CapChainStepInfo>,
    /// Source media URN
    pub source_spec: MediaUrn,
    /// Target media URN
    pub target_spec: MediaUrn,
    /// Total number of steps
    pub total_steps: i32,
    /// Human-readable description
    pub description: String,
}

// =============================================================================
// IMPLEMENTATION
// =============================================================================

impl LiveCapGraph {
    /// Create a new empty capability graph.
    pub fn new() -> Self {
        Self {
            edges: Vec::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            nodes: HashSet::new(),
            cap_to_edges: HashMap::new(),
        }
    }

    /// Clear the graph completely.
    pub fn clear(&mut self) {
        self.edges.clear();
        self.outgoing.clear();
        self.incoming.clear();
        self.nodes.clear();
        self.cap_to_edges.clear();
    }

    /// Rebuild the graph from a list of capabilities.
    ///
    /// This completely replaces the current graph contents.
    /// Call this when the set of available capabilities changes.
    pub fn sync_from_caps(&mut self, caps: &[Cap]) {
        self.clear();

        for cap in caps {
            self.add_cap(cap);
        }

        tracing::debug!(
            edge_count = self.edges.len(),
            node_count = self.nodes.len(),
            "[LiveCapGraph] Synced from {} caps",
            caps.len()
        );
    }

    /// Rebuild the graph from a list of cap URN strings using the registry.
    ///
    /// This is the primary method for RelaySwitch integration. Given the list of
    /// available cap URN strings (from plugins), it looks up the Cap definitions
    /// from the registry and builds the graph.
    ///
    /// Only caps that exist in the registry are added. Caps not found in the registry
    /// are logged as errors - this indicates a mismatch between plugin capabilities
    /// and registered cap definitions that must be fixed.
    pub async fn sync_from_cap_urns(&mut self, cap_urns: &[String], registry: &Arc<CapRegistry>) {
        self.clear();

        // Get all cached caps from registry
        let all_caps = match registry.get_cached_caps().await {
            Ok(caps) => caps,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    "[LiveCapGraph] Failed to get cached caps from registry"
                );
                return;
            }
        };

        let mut matched_count = 0;

        for cap_urn_str in cap_urns {
            // Parse the cap URN
            let cap_urn = match CapUrn::from_string(cap_urn_str) {
                Ok(u) => u,
                Err(e) => {
                    tracing::error!(
                        cap_urn = cap_urn_str,
                        error = %e,
                        "[LiveCapGraph] Plugin reported invalid cap URN - this is a bug in the plugin"
                    );
                    continue;
                }
            };

            // Find matching Cap in registry using is_dispatchable
            // A registry cap matches if the plugin's cap can dispatch it
            let matching_cap = all_caps.iter().find(|registry_cap| {
                cap_urn.is_dispatchable(&registry_cap.urn)
            });

            match matching_cap {
                Some(cap) => {
                    self.add_cap(cap);
                    matched_count += 1;
                }
                None => {
                    // Cap not in registry - this is an error condition.
                    // All caps must be registered. Log and skip.
                    tracing::error!(
                        cap_urn = %cap_urn,
                        "[LiveCapGraph] Cap URN not found in registry - plugin provides unregistered capability"
                    );
                }
            }
        }

        tracing::info!(
            edge_count = self.edges.len(),
            node_count = self.nodes.len(),
            matched_count,
            total_urns = cap_urns.len(),
            "[LiveCapGraph] Synced from cap URNs"
        );
    }

    /// Add a capability as an edge in the graph.
    pub fn add_cap(&mut self, cap: &Cap) {
        let in_spec_str = cap.urn.in_spec();
        let out_spec_str = cap.urn.out_spec();

        // Skip caps with empty specs
        if in_spec_str.is_empty() || out_spec_str.is_empty() {
            return;
        }

        // Skip identity caps (passthrough caps that don't transform anything)
        // These are is_equivalent to the CAP_IDENTITY constant
        if cap.urn.is_equivalent(&crate::standard::caps::identity_urn()) {
            return;
        }

        // Parse media URNs
        let from_spec = match MediaUrn::from_string(in_spec_str) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(
                    cap_urn = %cap.urn,
                    in_spec = in_spec_str,
                    error = %e,
                    "[LiveCapGraph] Failed to parse in_spec, skipping cap"
                );
                return;
            }
        };

        let to_spec = match MediaUrn::from_string(out_spec_str) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(
                    cap_urn = %cap.urn,
                    out_spec = out_spec_str,
                    error = %e,
                    "[LiveCapGraph] Failed to parse out_spec, skipping cap"
                );
                return;
            }
        };

        let from_canonical = from_spec.to_string();
        let to_canonical = to_spec.to_string();
        let cap_canonical = cap.urn.to_string();

        // Create edge
        let edge_idx = self.edges.len();
        let edge = LiveCapEdge {
            from_spec,
            to_spec,
            cap_urn: cap.urn.clone(),
            cap_title: cap.title.clone(),
            specificity: cap.urn.specificity(),
        };
        self.edges.push(edge);

        // Update indices
        self.outgoing.entry(from_canonical.clone()).or_default().push(edge_idx);
        self.incoming.entry(to_canonical.clone()).or_default().push(edge_idx);
        self.nodes.insert(from_canonical);
        self.nodes.insert(to_canonical);
        self.cap_to_edges.entry(cap_canonical).or_default().push(edge_idx);
    }

    /// Get all edges originating from a source media URN.
    ///
    /// Uses `conforms_to()` matching: returns edges where the source
    /// conforms to the edge's from_spec requirement.
    fn get_outgoing_edges(&self, source: &MediaUrn) -> Vec<&LiveCapEdge> {
        self.edges
            .iter()
            .filter(|edge| {
                source.conforms_to(&edge.from_spec)
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Get statistics about the graph.
    pub fn stats(&self) -> (usize, usize) {
        (self.nodes.len(), self.edges.len())
    }

    // =========================================================================
    // REACHABLE TARGETS (BFS)
    // =========================================================================

    /// Find all reachable targets from a source media URN.
    ///
    /// Uses BFS to explore the graph up to max_depth steps.
    /// Returns targets sorted by (min_path_length, display_name).
    pub fn get_reachable_targets(
        &self,
        source: &MediaUrn,
        max_depth: usize,
    ) -> Vec<ReachableTargetInfo> {
        let mut results: HashMap<String, ReachableTargetInfo> = HashMap::new();
        let mut visited: HashSet<String> = HashSet::new();
        let mut queue: VecDeque<(MediaUrn, usize)> = VecDeque::new();

        let source_canonical = source.to_string();
        queue.push_back((source.clone(), 0));
        visited.insert(source_canonical);

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }

            for edge in self.get_outgoing_edges(&current) {
                let new_depth = depth + 1;
                let output_canonical = edge.to_spec.to_string();

                // Record this target
                let entry = results.entry(output_canonical.clone()).or_insert_with(|| {
                    ReachableTargetInfo {
                        media_spec: edge.to_spec.clone(),
                        display_name: output_canonical.clone(), // Will be enriched by caller
                        min_path_length: new_depth as i32,
                        path_count: 0,
                    }
                });
                entry.path_count += 1;

                // Continue BFS if not visited
                if !visited.contains(&output_canonical) {
                    visited.insert(output_canonical);
                    queue.push_back((edge.to_spec.clone(), new_depth));
                }
            }
        }

        // Sort by (min_path_length, display_name)
        let mut targets: Vec<_> = results.into_values().collect();
        targets.sort_by(|a, b| {
            a.min_path_length.cmp(&b.min_path_length)
                .then_with(|| a.display_name.cmp(&b.display_name))
        });

        targets
    }

    // =========================================================================
    // PATH FINDING (DFS with exact target matching)
    // =========================================================================

    /// Find all paths from source to target media URN.
    ///
    /// **Critical**: Uses `is_equivalent()` for target matching, NOT `conforms_to()`.
    /// This ensures exact matching: "media:X" will NOT match "media:X;list".
    ///
    /// Returns paths sorted by (total_steps, total_specificity desc, cap_urns).
    pub fn find_paths_to_exact_target(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        max_depth: usize,
        max_paths: usize,
    ) -> Vec<CapChainPathInfo> {
        // Check if source already satisfies target
        if source.is_equivalent(target).unwrap_or(false) {
            return vec![];
        }

        let mut all_paths: Vec<CapChainPathInfo> = Vec::new();
        let mut current_path: Vec<CapChainStepInfo> = Vec::new();
        let mut visited: HashSet<String> = HashSet::new();

        self.dfs_find_paths(
            source,
            target,
            source,
            &mut current_path,
            &mut visited,
            &mut all_paths,
            max_depth,
            max_paths,
        );

        // Sort paths deterministically
        all_paths.sort_by(|a, b| Self::compare_paths(a, b));

        all_paths
    }

    /// DFS helper for path finding.
    fn dfs_find_paths(
        &self,
        source: &MediaUrn,
        target: &MediaUrn,
        current: &MediaUrn,
        current_path: &mut Vec<CapChainStepInfo>,
        visited: &mut HashSet<String>,
        all_paths: &mut Vec<CapChainPathInfo>,
        max_depth: usize,
        max_paths: usize,
    ) {
        if all_paths.len() >= max_paths {
            return;
        }

        // Check if we've reached the EXACT target using is_equivalent()
        // is_equivalent: same tag set, order-independent
        if current.is_equivalent(target).unwrap_or(false) {
            let description = current_path
                .iter()
                .map(|s| s.title.clone())
                .collect::<Vec<_>>()
                .join(" → ");

            all_paths.push(CapChainPathInfo {
                steps: current_path.clone(),
                source_spec: source.clone(),
                target_spec: target.clone(),
                total_steps: current_path.len() as i32,
                description,
            });
            return;
        }

        if current_path.len() >= max_depth {
            return;
        }

        let current_canonical = current.to_string();
        visited.insert(current_canonical.clone());

        // Explore outgoing edges
        for edge in self.get_outgoing_edges(current) {
            let next_canonical = edge.to_spec.to_string();

            if !visited.contains(&next_canonical) {
                current_path.push(CapChainStepInfo {
                    cap_urn: edge.cap_urn.clone(),
                    from_spec: edge.from_spec.clone(),
                    to_spec: edge.to_spec.clone(),
                    title: edge.cap_title.clone(),
                    specificity: edge.specificity,
                });

                self.dfs_find_paths(
                    source,
                    target,
                    &edge.to_spec,
                    current_path,
                    visited,
                    all_paths,
                    max_depth,
                    max_paths,
                );

                current_path.pop();
            }
        }

        visited.remove(&current_canonical);
    }

    /// Compare two paths for deterministic ordering.
    ///
    /// Sort by:
    /// 1. total_steps (ascending - shorter paths first)
    /// 2. total specificity (descending - more specific first)
    /// 3. cap URNs lexicographically (for tie-breaking stability)
    fn compare_paths(a: &CapChainPathInfo, b: &CapChainPathInfo) -> Ordering {
        a.total_steps.cmp(&b.total_steps)
            .then_with(|| {
                // Higher specificity first
                let spec_a: usize = a.steps.iter().map(|s| s.specificity).sum();
                let spec_b: usize = b.steps.iter().map(|s| s.specificity).sum();
                spec_b.cmp(&spec_a)
            })
            .then_with(|| {
                // Lexicographic by cap URNs (only for tie-breaking)
                let urns_a: Vec<String> = a.steps.iter().map(|s| s.cap_urn.to_string()).collect();
                let urns_b: Vec<String> = b.steps.iter().map(|s| s.cap_urn.to_string()).collect();
                urns_a.cmp(&urns_b)
            })
    }
}

impl Default for LiveCapGraph {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cap::definition::Cap;
    use crate::urn::cap_urn::CapUrn;

    fn make_test_cap(in_spec: &str, out_spec: &str, op: &str, title: &str) -> Cap {
        let cap_urn = CapUrn::from_string(&format!(
            "cap:in={};out={};op={}",
            in_spec, out_spec, op
        )).unwrap();

        Cap {
            urn: cap_urn,
            title: title.to_string(),
            cap_description: None,
            metadata: Default::default(),
            command: "test".to_string(),
            media_specs: vec![],
            output: None,
            args: vec![],
        }
    }

    #[test]
    fn test_add_cap_and_basic_traversal() {
        let mut graph = LiveCapGraph::new();

        let cap = make_test_cap("media:pdf", "media:text;textable", "extract_text", "Extract Text");
        graph.add_cap(&cap);

        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.nodes.len(), 2);

        let source = MediaUrn::from_string("media:pdf").unwrap();
        let targets = graph.get_reachable_targets(&source, 5);

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].min_path_length, 1);
    }

    #[test]
    fn test_exact_vs_conformance_matching() {
        let mut graph = LiveCapGraph::new();

        // Add cap: pdf -> decision
        let cap1 = make_test_cap(
            "media:pdf",
            "media:decision;bool;textable",
            "analyze",
            "Analyze PDF"
        );
        graph.add_cap(&cap1);

        // Add cap: pdf -> decision;list
        let cap2 = make_test_cap(
            "media:pdf",
            "media:decision;bool;textable;list",
            "analyze_multi",
            "Analyze PDF Multi"
        );
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:pdf").unwrap();

        // Query for EXACT target: decision (singular)
        let target_singular = MediaUrn::from_string("media:decision;bool;textable").unwrap();
        let paths_singular = graph.find_paths_to_exact_target(&source, &target_singular, 5, 10);

        // Should find exactly 1 path (not both!)
        assert_eq!(paths_singular.len(), 1);
        assert_eq!(paths_singular[0].steps[0].title, "Analyze PDF");

        // Query for EXACT target: decision;list (plural)
        let target_plural = MediaUrn::from_string("media:decision;bool;textable;list").unwrap();
        let paths_plural = graph.find_paths_to_exact_target(&source, &target_plural, 5, 10);

        // Should find exactly 1 path (not both!)
        assert_eq!(paths_plural.len(), 1);
        assert_eq!(paths_plural[0].steps[0].title, "Analyze PDF Multi");
    }

    #[test]
    fn test_multi_step_path() {
        let mut graph = LiveCapGraph::new();

        // pdf -> text
        let cap1 = make_test_cap("media:pdf", "media:text;textable", "extract", "Extract");
        // text -> summary
        let cap2 = make_test_cap("media:text;textable", "media:summary;textable", "summarize", "Summarize");

        graph.add_cap(&cap1);
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:pdf").unwrap();
        let target = MediaUrn::from_string("media:summary;textable").unwrap();

        let paths = graph.find_paths_to_exact_target(&source, &target, 5, 10);

        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].total_steps, 2);
        assert_eq!(paths[0].steps[0].title, "Extract");
        assert_eq!(paths[0].steps[1].title, "Summarize");
    }

    #[test]
    fn test_deterministic_ordering() {
        let mut graph = LiveCapGraph::new();

        // Two paths to the same target with different specificities
        let cap1 = make_test_cap("media:pdf", "media:text;textable", "extract_a", "Extract A");
        let cap2 = make_test_cap("media:pdf", "media:text;textable", "extract_b", "Extract B");

        graph.add_cap(&cap1);
        graph.add_cap(&cap2);

        let source = MediaUrn::from_string("media:pdf").unwrap();
        let target = MediaUrn::from_string("media:text;textable").unwrap();

        // Run multiple times - should always get the same order
        let paths1 = graph.find_paths_to_exact_target(&source, &target, 5, 10);
        let paths2 = graph.find_paths_to_exact_target(&source, &target, 5, 10);

        assert_eq!(paths1.len(), paths2.len());
        for (p1, p2) in paths1.iter().zip(paths2.iter()) {
            assert_eq!(p1.steps[0].cap_urn.to_string(), p2.steps[0].cap_urn.to_string());
        }
    }

    #[test]
    fn test_sync_from_caps() {
        let mut graph = LiveCapGraph::new();

        let caps = vec![
            make_test_cap("media:pdf", "media:text;textable", "op1", "Op1"),
            make_test_cap("media:text;textable", "media:summary;textable", "op2", "Op2"),
        ];

        graph.sync_from_caps(&caps);

        assert_eq!(graph.edges.len(), 2);
        assert_eq!(graph.nodes.len(), 3);

        // Sync again with different caps - should replace
        let new_caps = vec![
            make_test_cap("media:image", "media:text;textable", "ocr", "OCR"),
        ];

        graph.sync_from_caps(&new_caps);

        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.nodes.len(), 2);
    }
}
