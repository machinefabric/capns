//! Image adapters — PNG, JPEG, GIF, WebP, etc.

use std::path::Path;
use crate::input_resolver::adapter::{MediaAdapter, AdapterResult};

/// PNG image adapter
pub struct PngAdapter;

impl MediaAdapter for PngAdapter {
    fn name(&self) -> &'static str { "png" }

    fn extensions(&self) -> &'static [&'static str] {
        &["png"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"\x89PNG\r\n\x1a\n", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:png;image")
    }
}

/// JPEG image adapter
pub struct JpegAdapter;

impl MediaAdapter for JpegAdapter {
    fn name(&self) -> &'static str { "jpeg" }

    fn extensions(&self) -> &'static [&'static str] {
        &["jpg", "jpeg", "jpe", "jif", "jfif"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"\xFF\xD8\xFF", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:jpeg;image")
    }
}

/// GIF image adapter
pub struct GifAdapter;

impl MediaAdapter for GifAdapter {
    fn name(&self) -> &'static str { "gif" }

    fn extensions(&self) -> &'static [&'static str] {
        &["gif"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"GIF87a", 0),
            (b"GIF89a", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:gif;image")
    }
}

/// WebP image adapter
pub struct WebpAdapter;

impl MediaAdapter for WebpAdapter {
    fn name(&self) -> &'static str { "webp" }

    fn extensions(&self) -> &'static [&'static str] {
        &["webp"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            // RIFF....WEBP
            (b"RIFF", 0),
        ]
    }

    fn detect(&self, _path: &Path, content: &[u8]) -> AdapterResult {
        // Verify WEBP signature at offset 8
        if content.len() >= 12 && &content[8..12] == b"WEBP" {
            AdapterResult::scalar_opaque("media:webp;image")
        } else {
            // RIFF but not WEBP - still return webp if extension matched
            AdapterResult::scalar_opaque("media:webp;image")
        }
    }
}

/// AVIF image adapter
pub struct AvifAdapter;

impl MediaAdapter for AvifAdapter {
    fn name(&self) -> &'static str { "avif" }

    fn extensions(&self) -> &'static [&'static str] {
        &["avif"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            // ftyp box with avif brand
            (b"\x00\x00\x00", 0), // Size bytes vary
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:avif;image")
    }
}

/// HEIC/HEIF image adapter
pub struct HeicAdapter;

impl MediaAdapter for HeicAdapter {
    fn name(&self) -> &'static str { "heic" }

    fn extensions(&self) -> &'static [&'static str] {
        &["heic", "heif", "hif"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:heic;image")
    }
}

/// TIFF image adapter
pub struct TiffAdapter;

impl MediaAdapter for TiffAdapter {
    fn name(&self) -> &'static str { "tiff" }

    fn extensions(&self) -> &'static [&'static str] {
        &["tiff", "tif"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"II*\x00", 0),  // Little-endian
            (b"MM\x00*", 0),  // Big-endian
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:tiff;image")
    }
}

/// BMP image adapter
pub struct BmpAdapter;

impl MediaAdapter for BmpAdapter {
    fn name(&self) -> &'static str { "bmp" }

    fn extensions(&self) -> &'static [&'static str] {
        &["bmp", "dib"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"BM", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:bmp;image")
    }
}

/// ICO/CUR icon adapter
pub struct IcoAdapter;

impl MediaAdapter for IcoAdapter {
    fn name(&self) -> &'static str { "ico" }

    fn extensions(&self) -> &'static [&'static str] {
        &["ico", "cur", "icns"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"\x00\x00\x01\x00", 0), // ICO
            (b"\x00\x00\x02\x00", 0), // CUR
            (b"icns", 0),             // ICNS (macOS)
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "icns" => "media:icns;image",
            _ => "media:ico;image",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// SVG vector image adapter
pub struct SvgAdapter;

impl MediaAdapter for SvgAdapter {
    fn name(&self) -> &'static str { "svg" }

    fn extensions(&self) -> &'static [&'static str] {
        &["svg", "svgz"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        // SVG is XML-based, textable
        AdapterResult::scalar_opaque("media:svg;image;textable")
    }
}

/// Photoshop PSD adapter
pub struct PsdAdapter;

impl MediaAdapter for PsdAdapter {
    fn name(&self) -> &'static str { "psd" }

    fn extensions(&self) -> &'static [&'static str] {
        &["psd", "psb"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"8BPS", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:psd;image")
    }
}

/// Camera RAW image adapter
pub struct RawImageAdapter;

impl MediaAdapter for RawImageAdapter {
    fn name(&self) -> &'static str { "raw" }

    fn extensions(&self) -> &'static [&'static str] {
        &[
            "raw", "cr2", "cr3", "nef", "nrw", "arw", "srf", "sr2",
            "dng", "orf", "rw2", "pef", "raf", "3fr", "erf", "kdc",
            "dcr", "mrw", "x3f",
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:raw;image")
    }
}

/// EPS/AI vector adapter
pub struct EpsAdapter;

impl MediaAdapter for EpsAdapter {
    fn name(&self) -> &'static str { "eps" }

    fn extensions(&self) -> &'static [&'static str] {
        &["eps", "epsf", "epsi", "ai"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"%!PS-Adobe", 0),
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "ai" => "media:ai;image",
            _ => "media:eps;image",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// HDR/EXR high dynamic range adapter
pub struct HdrAdapter;

impl MediaAdapter for HdrAdapter {
    fn name(&self) -> &'static str { "hdr" }

    fn extensions(&self) -> &'static [&'static str] {
        &["hdr", "exr"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"#?RADIANCE", 0),  // HDR
            (b"\x76\x2f\x31\x01", 0), // EXR
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "exr" => "media:exr;image",
            _ => "media:hdr;image",
        };

        AdapterResult::scalar_opaque(media)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use crate::input_resolver::ContentStructure;

    // TEST1081: PNG extension mapping
    #[test]
    fn test1081_png_extension() {
        let adapter = PngAdapter;
        let path = PathBuf::from("image.png");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:png;image");
        assert_eq!(result.content_structure, ContentStructure::ScalarOpaque);
    }

    #[test]
    fn test_png_magic_bytes() {
        let adapter = PngAdapter;
        let path = PathBuf::from("image");
        let content = b"\x89PNG\r\n\x1a\n";

        assert!(adapter.matches(&path, content).matches());
    }

    #[test]
    fn test_jpeg_variants() {
        let adapter = JpegAdapter;

        for ext in &["jpg", "jpeg", "jpe", "jfif"] {
            let path = PathBuf::from(format!("photo.{}", ext));
            assert!(adapter.matches(&path, &[]).matches());
        }
    }

    #[test]
    fn test_svg_is_textable() {
        let adapter = SvgAdapter;
        let path = PathBuf::from("icon.svg");

        let result = adapter.detect(&path, &[]);
        assert!(result.media_urn.contains("textable"));
    }
}
