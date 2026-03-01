//! DAG validation for orchestration graphs
//!
//! Implements cycle detection using Kahn's algorithm for topological sort.

use super::types::{ParseOrchestrationError, ResolvedEdge};
use std::collections::HashMap;

/// Validate that the graph is a DAG (no cycles)
pub fn validate_dag(
    nodes: &HashMap<String, String>,
    edges: &[ResolvedEdge],
) -> Result<(), ParseOrchestrationError> {
    // Build adjacency list
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut in_degree: HashMap<&str, usize> = HashMap::new();

    // Initialize all nodes
    for node in nodes.keys() {
        in_degree.insert(node.as_str(), 0);
        adj.insert(node.as_str(), Vec::new());
    }

    // Build graph
    for edge in edges {
        adj.entry(edge.from.as_str())
            .or_insert_with(Vec::new)
            .push(edge.to.as_str());
        *in_degree.entry(edge.to.as_str()).or_insert(0) += 1;
    }

    // Kahn's algorithm for topological sort
    let mut queue: Vec<&str> = in_degree
        .iter()
        .filter_map(|(node, &deg)| if deg == 0 { Some(*node) } else { None })
        .collect();

    let mut sorted_count = 0;

    while let Some(node) = queue.pop() {
        sorted_count += 1;

        if let Some(neighbors) = adj.get(node) {
            for &neighbor in neighbors {
                if let Some(degree) = in_degree.get_mut(neighbor) {
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push(neighbor);
                    }
                }
            }
        }
    }

    // If we couldn't sort all nodes, there's a cycle
    if sorted_count != nodes.len() {
        let cycle_nodes: Vec<String> = in_degree
            .iter()
            .filter_map(|(node, &deg)| {
                if deg > 0 {
                    Some(node.to_string())
                } else {
                    None
                }
            })
            .collect();

        return Err(ParseOrchestrationError::NotADag { cycle_nodes });
    }

    Ok(())
}
