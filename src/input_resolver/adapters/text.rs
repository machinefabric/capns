//! Plain text content inspection adapter
//!
//! Only PlainTextAdapter requires content inspection to determine
//! if text is single-line (scalar) or multi-line (list).
//! Other text formats (Markdown, Log, etc.) have fixed structures
//! defined in their TOML specs.

use std::path::Path;

use crate::input_resolver::adapter::{AdapterResult, MediaAdapter};
use crate::input_resolver::ContentStructure;

/// Plain text adapter — inspects content for structure
///
/// Determines if a .txt file is:
/// - Single line → ScalarOpaque (media:txt;textable)
/// - Multi-line → ListOpaque (media:txt;list;textable)
pub struct PlainTextAdapter;

impl MediaAdapter for PlainTextAdapter {
    fn name(&self) -> &'static str {
        "txt"
    }

    fn extensions(&self) -> &'static [&'static str] {
        // Empty - extensions handled by MediaUrnRegistry
        &[]
    }

    fn requires_content_inspection(&self) -> bool {
        true
    }

    fn detect(&self, _path: &Path, content: &[u8]) -> AdapterResult {
        detect_plain_text_structure(content)
    }
}

/// Detect plain text structure from content
fn detect_plain_text_structure(content: &[u8]) -> AdapterResult {
    let text = match std::str::from_utf8(content) {
        Ok(s) => s,
        Err(_) => {
            // Not valid UTF-8, treat as binary
            return AdapterResult::scalar_opaque("media:");
        }
    };

    // Count newlines
    let line_count = text.lines().count();

    if line_count <= 1 {
        // Single line → scalar
        AdapterResult::scalar_opaque("media:txt;textable")
    } else {
        // Multi-line → list of lines
        AdapterResult::list_opaque("media:txt;list;textable")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_plain_text_single_line() {
        let adapter = PlainTextAdapter;
        let path = PathBuf::from("note.txt");
        let content = b"just a single line";

        let result = adapter.detect(&path, content);
        assert_eq!(result.content_structure, ContentStructure::ScalarOpaque);
        assert_eq!(result.media_urn, "media:txt;textable");
    }

    #[test]
    fn test_plain_text_multi_line() {
        let adapter = PlainTextAdapter;
        let path = PathBuf::from("note.txt");
        let content = b"line one\nline two\nline three";

        let result = adapter.detect(&path, content);
        assert_eq!(result.content_structure, ContentStructure::ListOpaque);
        assert_eq!(result.media_urn, "media:txt;list;textable");
    }

    #[test]
    fn test_plain_text_empty() {
        let adapter = PlainTextAdapter;
        let path = PathBuf::from("empty.txt");
        let content = b"";

        let result = adapter.detect(&path, content);
        // Empty file has 0 lines, which is <= 1
        assert_eq!(result.content_structure, ContentStructure::ScalarOpaque);
    }

    #[test]
    fn test_plain_text_binary() {
        let adapter = PlainTextAdapter;
        let path = PathBuf::from("data.txt");
        let content = &[0xFF, 0xFE, 0x00, 0x01]; // Invalid UTF-8

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:");
    }
}
