//! MediaAdapter trait — pluggable file type detection
//!
//! Each adapter is responsible for:
//! 1. Matching: Does this adapter handle this file? (by extension or magic bytes)
//! 2. Detection: Given file content, produce media URN with correct list/record markers

use std::path::Path;
use crate::input_resolver::ContentStructure;

/// Result of checking if an adapter matches a file
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterMatch {
    /// Adapter matches by extension (no content inspection needed for base type)
    ByExtension,

    /// Adapter matches by magic bytes (content was inspected)
    ByMagicBytes,

    /// Adapter does not match this file
    NoMatch,
}

impl AdapterMatch {
    pub fn matches(&self) -> bool {
        !matches!(self, AdapterMatch::NoMatch)
    }
}

/// Result of adapter detection
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterResult {
    /// The detected media URN (with appropriate markers)
    pub media_urn: String,

    /// The detected content structure
    pub content_structure: ContentStructure,
}

impl AdapterResult {
    /// Create a scalar opaque result (most common for binary files)
    pub fn scalar_opaque(media_urn: impl Into<String>) -> Self {
        AdapterResult {
            media_urn: media_urn.into(),
            content_structure: ContentStructure::ScalarOpaque,
        }
    }

    /// Create a scalar record result
    pub fn scalar_record(media_urn: impl Into<String>) -> Self {
        AdapterResult {
            media_urn: media_urn.into(),
            content_structure: ContentStructure::ScalarRecord,
        }
    }

    /// Create a list opaque result
    pub fn list_opaque(media_urn: impl Into<String>) -> Self {
        AdapterResult {
            media_urn: media_urn.into(),
            content_structure: ContentStructure::ListOpaque,
        }
    }

    /// Create a list record result
    pub fn list_record(media_urn: impl Into<String>) -> Self {
        AdapterResult {
            media_urn: media_urn.into(),
            content_structure: ContentStructure::ListRecord,
        }
    }
}

/// Trait for media type adapters
///
/// Each adapter handles one or more file types and is responsible for:
/// 1. Detecting if it applies to a file (by extension or magic bytes)
/// 2. Inspecting content to determine the correct media URN and structure markers
pub trait MediaAdapter: Send + Sync {
    /// Unique name for this adapter (for debugging/logging)
    fn name(&self) -> &'static str;

    /// File extensions this adapter handles (lowercase, without dot)
    /// Return empty slice if this adapter uses magic bytes only
    fn extensions(&self) -> &'static [&'static str];

    /// Magic bytes patterns this adapter recognizes
    /// Each pattern is (offset, bytes) - checks if file[offset..] starts with bytes
    /// Return empty slice if this adapter uses extension only
    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[]
    }

    /// Check if this adapter matches the given file
    ///
    /// - `path`: File path (for extension checking)
    /// - `content_prefix`: First N bytes of file content (for magic byte checking)
    fn matches(&self, path: &Path, content_prefix: &[u8]) -> AdapterMatch {
        // Check extension first (cheaper)
        if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            let ext_lower = ext.to_lowercase();
            if self.extensions().iter().any(|&e| e == ext_lower) {
                return AdapterMatch::ByExtension;
            }
        }

        // Check magic bytes
        for (magic, offset) in self.magic_bytes() {
            if content_prefix.len() >= offset + magic.len() {
                if &content_prefix[*offset..][..magic.len()] == *magic {
                    return AdapterMatch::ByMagicBytes;
                }
            }
        }

        AdapterMatch::NoMatch
    }

    /// Detect media type and structure from file content
    ///
    /// This is called after `matches()` returns true.
    ///
    /// - `path`: File path
    /// - `content`: Full file content (or first N bytes for large files)
    ///
    /// Returns the detected media URN with appropriate list/record markers
    fn detect(&self, path: &Path, content: &[u8]) -> AdapterResult;

    /// Whether this adapter requires content inspection to determine structure
    ///
    /// If false, the adapter can determine everything from extension alone.
    /// If true, content must be read and passed to `detect()`.
    fn requires_content_inspection(&self) -> bool {
        false
    }

    /// Priority for adapter matching (higher = checked first)
    /// Default is 0. Use higher values for more specific adapters.
    fn priority(&self) -> i32 {
        0
    }
}

/// Helper to build media URN with markers
pub fn build_media_urn(base: &str, list: bool, record: bool, textable: bool) -> String {
    let mut parts = vec![format!("media:{}", base)];

    if list {
        parts.push("list".to_string());
    }
    if record {
        parts.push("record".to_string());
    }
    if textable {
        parts.push("textable".to_string());
    }

    parts.join(";")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_media_urn_basic() {
        assert_eq!(build_media_urn("pdf", false, false, false), "media:pdf");
    }

    #[test]
    fn test_build_media_urn_with_list() {
        assert_eq!(build_media_urn("json", true, false, true), "media:json;list;textable");
    }

    #[test]
    fn test_build_media_urn_with_record() {
        assert_eq!(build_media_urn("json", false, true, true), "media:json;record;textable");
    }

    #[test]
    fn test_build_media_urn_with_both() {
        assert_eq!(build_media_urn("csv", true, true, true), "media:csv;list;record;textable");
    }
}
