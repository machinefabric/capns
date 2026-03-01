//! Archive adapters — ZIP, TAR, GZIP, etc.
//!
//! All archives are treated as scalar opaque.

use std::path::Path;
use crate::input_resolver::adapter::{MediaAdapter, AdapterResult};

/// ZIP archive adapter
pub struct ZipAdapter;

impl MediaAdapter for ZipAdapter {
    fn name(&self) -> &'static str { "zip" }

    fn extensions(&self) -> &'static [&'static str] {
        &["zip", "zipx"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"PK\x03\x04", 0),
            (b"PK\x05\x06", 0), // Empty archive
            (b"PK\x07\x08", 0), // Spanned archive
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:zip;archive")
    }
}

/// TAR archive adapter
pub struct TarAdapter;

impl MediaAdapter for TarAdapter {
    fn name(&self) -> &'static str { "tar" }

    fn extensions(&self) -> &'static [&'static str] {
        &["tar"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            // "ustar" at offset 257
            (b"ustar", 257),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:tar;archive")
    }
}

/// GZIP adapter
pub struct GzipAdapter;

impl MediaAdapter for GzipAdapter {
    fn name(&self) -> &'static str { "gzip" }

    fn extensions(&self) -> &'static [&'static str] {
        &["gz", "gzip", "tgz", "tar.gz"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"\x1f\x8b", 0),
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        // Check for .tar.gz or .tgz
        if filename.ends_with(".tar.gz") || filename.ends_with(".tgz") {
            AdapterResult::scalar_opaque("media:targz;archive")
        } else {
            AdapterResult::scalar_opaque("media:gzip;archive")
        }
    }
}

/// Bzip2 adapter
pub struct Bzip2Adapter;

impl MediaAdapter for Bzip2Adapter {
    fn name(&self) -> &'static str { "bzip2" }

    fn extensions(&self) -> &'static [&'static str] {
        &["bz2", "bzip2", "tbz2", "tar.bz2"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"BZh", 0),
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        if filename.ends_with(".tar.bz2") || filename.ends_with(".tbz2") {
            AdapterResult::scalar_opaque("media:tarbz2;archive")
        } else {
            AdapterResult::scalar_opaque("media:bzip2;archive")
        }
    }
}

/// XZ adapter
pub struct XzAdapter;

impl MediaAdapter for XzAdapter {
    fn name(&self) -> &'static str { "xz" }

    fn extensions(&self) -> &'static [&'static str] {
        &["xz", "lzma", "txz", "tar.xz"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"\xFD7zXZ\x00", 0),
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        if filename.ends_with(".tar.xz") || filename.ends_with(".txz") {
            AdapterResult::scalar_opaque("media:tarxz;archive")
        } else {
            AdapterResult::scalar_opaque("media:xz;archive")
        }
    }
}

/// Zstandard adapter
pub struct ZstdAdapter;

impl MediaAdapter for ZstdAdapter {
    fn name(&self) -> &'static str { "zstd" }

    fn extensions(&self) -> &'static [&'static str] {
        &["zst", "zstd"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"\x28\xB5\x2F\xFD", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:zstd;archive")
    }
}

/// 7-Zip adapter
pub struct SevenZipAdapter;

impl MediaAdapter for SevenZipAdapter {
    fn name(&self) -> &'static str { "7z" }

    fn extensions(&self) -> &'static [&'static str] {
        &["7z"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"7z\xBC\xAF\x27\x1C", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:7z;archive")
    }
}

/// RAR adapter
pub struct RarAdapter;

impl MediaAdapter for RarAdapter {
    fn name(&self) -> &'static str { "rar" }

    fn extensions(&self) -> &'static [&'static str] {
        &["rar"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"Rar!\x1A\x07\x00", 0), // RAR 4
            (b"Rar!\x1A\x07\x01\x00", 0), // RAR 5
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:rar;archive")
    }
}

/// Java JAR/WAR/EAR adapter
pub struct JarAdapter;

impl MediaAdapter for JarAdapter {
    fn name(&self) -> &'static str { "jar" }

    fn extensions(&self) -> &'static [&'static str] {
        &["jar", "war", "ear", "aar"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "war" => "media:war;archive",
            "ear" => "media:ear;archive",
            "aar" => "media:aar;archive",
            _ => "media:jar;archive",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// APK/IPA mobile app adapter
pub struct MobileAppAdapter;

impl MediaAdapter for MobileAppAdapter {
    fn name(&self) -> &'static str { "mobileapp" }

    fn extensions(&self) -> &'static [&'static str] {
        &["apk", "ipa", "aab"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "ipa" => "media:ipa;archive",
            "aab" => "media:aab;archive",
            _ => "media:apk;archive",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Disk image adapter
pub struct DiskImageAdapter;

impl MediaAdapter for DiskImageAdapter {
    fn name(&self) -> &'static str { "diskimage" }

    fn extensions(&self) -> &'static [&'static str] {
        &["dmg", "iso", "img", "vhd", "vhdx", "vmdk", "qcow2"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = format!("media:{};archive", ext);
        AdapterResult::scalar_opaque(media)
    }
}

/// Package manager adapter (deb, rpm, pkg)
pub struct PackageAdapter;

impl MediaAdapter for PackageAdapter {
    fn name(&self) -> &'static str { "package" }

    fn extensions(&self) -> &'static [&'static str] {
        &["deb", "rpm", "pkg", "msi", "appx", "msix", "snap", "flatpak"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = format!("media:{};archive", ext);
        AdapterResult::scalar_opaque(media)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use crate::input_resolver::ContentStructure;

    #[test]
    fn test_zip_extension() {
        let adapter = ZipAdapter;
        let path = PathBuf::from("archive.zip");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:zip;archive");
        assert_eq!(result.content_structure, ContentStructure::ScalarOpaque);
    }

    #[test]
    fn test_zip_magic() {
        let adapter = ZipAdapter;
        let path = PathBuf::from("unknown");
        let content = b"PK\x03\x04";

        assert!(adapter.matches(&path, content).matches());
    }

    #[test]
    fn test_tar_gz_detection() {
        let adapter = GzipAdapter;
        let path = PathBuf::from("archive.tar.gz");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:targz;archive");
    }

    #[test]
    fn test_tgz_detection() {
        let adapter = GzipAdapter;
        let path = PathBuf::from("archive.tgz");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:targz;archive");
    }

    #[test]
    fn test_7z_magic() {
        let adapter = SevenZipAdapter;
        let path = PathBuf::from("unknown");
        let content = b"7z\xBC\xAF\x27\x1C";

        assert!(adapter.matches(&path, content).matches());
    }
}
