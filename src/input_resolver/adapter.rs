//! Adapter types for file type detection
//!
//! This module defines the result types used by the adapter system and the
//! `CartridgeAdapterInvoker` trait for invoking cartridge content-inspection
//! adapters over the Bifaci protocol.

use crate::input_resolver::{ContentStructure, InputResolverError};
use async_trait::async_trait;
use std::path::Path;

/// Result of adapter detection — a selected media URN and its structure
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterResult {
    /// The selected media URN
    pub media_urn: String,

    /// The detected content structure
    pub content_structure: ContentStructure,
}

/// Trait for invoking the adapter-selection cap on a specific cartridge.
///
/// The implementation lives on the host side (machfab) where it has access
/// to the cartridge process/relay infrastructure. capdag defines the trait;
/// the host implements it.
#[async_trait]
pub trait CartridgeAdapterInvoker: Send + Sync {
    /// Invoke adapter-selection cap on a specific cartridge by ID.
    ///
    /// The cartridge_id is the `InstalledCartridgeIdentity.id` string that
    /// uniquely identifies the cartridge across reconnections.
    ///
    /// Returns:
    /// - `Ok(None)` for empty END frame (no match — cartridge doesn't handle this file)
    /// - `Ok(Some(media_urns))` for a successful detection with one or more media URNs
    /// - `Err(...)` for protocol errors, invalid responses, or infrastructure failures
    ///
    /// Invalid responses (stream output that isn't valid `{"media_urns": [...]}`) are
    /// runtime errors — the implementation must fail hard, not return None.
    async fn invoke_adapter_selection(
        &self,
        cartridge_id: &str,
        file_path: &Path,
    ) -> Result<Option<Vec<String>>, InputResolverError>;
}
