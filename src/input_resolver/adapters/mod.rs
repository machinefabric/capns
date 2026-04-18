//! Media Adapter Registry
//!
//! This module provides the `MediaAdapterRegistry` which tracks cartridge-provided
//! content inspection adapters and matches file extensions to registered adapters.

mod registry;

pub use registry::MediaAdapterRegistry;
