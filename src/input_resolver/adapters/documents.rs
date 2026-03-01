//! Document adapters — PDF, EPUB, Office documents, etc.

use std::path::Path;
use crate::input_resolver::adapter::{MediaAdapter, AdapterResult};

/// PDF document adapter
pub struct PdfAdapter;

impl MediaAdapter for PdfAdapter {
    fn name(&self) -> &'static str { "pdf" }

    fn extensions(&self) -> &'static [&'static str] {
        &["pdf"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"%PDF", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:pdf")
    }
}

/// EPUB ebook adapter
pub struct EpubAdapter;

impl MediaAdapter for EpubAdapter {
    fn name(&self) -> &'static str { "epub" }

    fn extensions(&self) -> &'static [&'static str] {
        &["epub"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        // EPUB is a ZIP file with "mimetypeapplication/epub+zip" at offset 30
        &[
            (b"PK\x03\x04", 0), // ZIP magic
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:epub")
    }
}

/// Kindle MOBI/AZW adapter
pub struct MobiAdapter;

impl MediaAdapter for MobiAdapter {
    fn name(&self) -> &'static str { "mobi" }

    fn extensions(&self) -> &'static [&'static str] {
        &["mobi", "azw", "azw3", "kf8", "kfx"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:mobi")
    }
}

/// DjVu document adapter
pub struct DjvuAdapter;

impl MediaAdapter for DjvuAdapter {
    fn name(&self) -> &'static str { "djvu" }

    fn extensions(&self) -> &'static [&'static str] {
        &["djvu", "djv"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"AT&TFORM", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:djvu")
    }
}

/// Microsoft Office adapter (doc, docx, xls, xlsx, ppt, pptx)
pub struct MsOfficeAdapter;

impl MediaAdapter for MsOfficeAdapter {
    fn name(&self) -> &'static str { "msoffice" }

    fn extensions(&self) -> &'static [&'static str] {
        &[
            // Legacy formats
            "doc", "xls", "ppt",
            // Modern formats (OOXML)
            "docx", "xlsx", "pptx",
            // Templates
            "dotx", "xltx", "potx",
            // Macro-enabled
            "docm", "xlsm", "pptm",
        ]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            // OLE Compound Document (legacy Office)
            (b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1", 0),
            // ZIP (OOXML)
            (b"PK\x03\x04", 0),
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        // Determine specific type from extension
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "doc" => "media:doc",
            "docx" | "dotx" | "docm" => "media:docx",
            "xls" => "media:xls",
            "xlsx" | "xltx" | "xlsm" => "media:xlsx",
            "ppt" => "media:ppt",
            "pptx" | "potx" | "pptm" => "media:pptx",
            _ => "media:msoffice",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// OpenDocument adapter (odt, ods, odp, etc.)
pub struct OpenDocumentAdapter;

impl MediaAdapter for OpenDocumentAdapter {
    fn name(&self) -> &'static str { "opendocument" }

    fn extensions(&self) -> &'static [&'static str] {
        &[
            "odt", "ods", "odp", "odg", "odf",
            "ott", "ots", "otp", "otg",
        ]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            // ZIP file (ODF is ZIP-based)
            (b"PK\x03\x04", 0),
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "odt" | "ott" => "media:odt",
            "ods" | "ots" => "media:ods",
            "odp" | "otp" => "media:odp",
            "odg" | "otg" => "media:odg",
            _ => "media:odf",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Apple iWork adapter (pages, numbers, keynote)
pub struct AppleIWorkAdapter;

impl MediaAdapter for AppleIWorkAdapter {
    fn name(&self) -> &'static str { "iwork" }

    fn extensions(&self) -> &'static [&'static str] {
        &["pages", "numbers", "keynote", "key"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "pages" => "media:pages",
            "numbers" => "media:numbers",
            "keynote" | "key" => "media:keynote",
            _ => "media:iwork",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Rich Text Format adapter
pub struct RtfAdapter;

impl MediaAdapter for RtfAdapter {
    fn name(&self) -> &'static str { "rtf" }

    fn extensions(&self) -> &'static [&'static str] {
        &["rtf"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"{\\rtf", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:rtf;textable")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use crate::input_resolver::adapter::AdapterMatch;

    // TEST1080: PDF extension mapping
    #[test]
    fn test1080_pdf_extension() {
        let adapter = PdfAdapter;
        let path = PathBuf::from("document.pdf");
        assert!(adapter.matches(&path, &[]).matches());

        let result = adapter.detect(&path, b"%PDF-1.4");
        assert_eq!(result.media_urn, "media:pdf");
        assert_eq!(result.content_structure, crate::input_resolver::ContentStructure::ScalarOpaque);
    }

    #[test]
    fn test_pdf_magic_bytes() {
        let adapter = PdfAdapter;
        let path = PathBuf::from("document");
        let content = b"%PDF-1.4 some content";

        let match_result = adapter.matches(&path, content);
        assert_eq!(match_result, AdapterMatch::ByMagicBytes);
    }

    #[test]
    fn test_docx_mapping() {
        let adapter = MsOfficeAdapter;
        let path = PathBuf::from("report.docx");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:docx");
    }

    #[test]
    fn test_xlsx_mapping() {
        let adapter = MsOfficeAdapter;
        let path = PathBuf::from("spreadsheet.xlsx");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:xlsx");
    }
}
