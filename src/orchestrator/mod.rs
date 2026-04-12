//! Orchestrator: Machine Notation Parser with CapDag Orchestration
//!
//! This module parses machine notation through `Machine::from_string`,
//! looks up the resolved caps in the registry, and produces a
//! validated executable DAG IR (`ResolvedGraph`).
//!
//! # Example
//!
//! ```ignore
//! use capdag::orchestrator::parse_machine_to_cap_dag;
//! use capdag::CapRegistry;
//!
//! let route = r#"
//!     [extract cap:in="media:pdf;bytes";op=extract;out="media:txt;textable"]
//!     [A -> extract -> B]
//! "#;
//!
//! let registry = CapRegistry::new().await?;
//! let graph = parse_machine_to_cap_dag(route, &registry).await?;
//! ```

pub mod types;
pub mod parser;
pub mod executor;
pub mod plan_converter;
pub mod cbor_util;

// Re-export key types
pub use types::{
    ParseOrchestrationError,
    ResolvedEdge,
    ResolvedGraph,
};

pub use parser::parse_machine_to_cap_dag;

pub use plan_converter::plan_to_resolved_graph;

pub use cbor_util::{split_cbor_array, assemble_cbor_array, split_cbor_sequence, assemble_cbor_sequence, CborUtilError};

pub use executor::{
    ExecutionError,
    NodeData,
    EdgeGroup,
    CartridgeManager,
    ExecutionContext,
    CapProgressFn,
    ProgressMapper,
    map_progress,
    execute_dag,
};
