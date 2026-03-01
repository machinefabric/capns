//! Video adapters — MP4, WebM, MKV, etc.

use std::path::Path;
use crate::input_resolver::adapter::{MediaAdapter, AdapterResult};

/// MP4 video adapter
pub struct Mp4Adapter;

impl MediaAdapter for Mp4Adapter {
    fn name(&self) -> &'static str { "mp4" }

    fn extensions(&self) -> &'static [&'static str] {
        &["mp4", "m4v", "f4v"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            // ftyp box variations
            (b"ftyp", 4),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:mp4;video")
    }
}

/// WebM video adapter
pub struct WebmAdapter;

impl MediaAdapter for WebmAdapter {
    fn name(&self) -> &'static str { "webm" }

    fn extensions(&self) -> &'static [&'static str] {
        &["webm"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            // EBML header
            (b"\x1A\x45\xDF\xA3", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:webm;video")
    }
}

/// Matroska video adapter
pub struct MkvAdapter;

impl MediaAdapter for MkvAdapter {
    fn name(&self) -> &'static str { "mkv" }

    fn extensions(&self) -> &'static [&'static str] {
        &["mkv", "mk3d", "mka", "mks"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            // EBML header (same as WebM)
            (b"\x1A\x45\xDF\xA3", 0),
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "mka" => "media:mka;audio",
            _ => "media:mkv;video",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// QuickTime MOV adapter
pub struct MovAdapter;

impl MediaAdapter for MovAdapter {
    fn name(&self) -> &'static str { "mov" }

    fn extensions(&self) -> &'static [&'static str] {
        &["mov", "qt"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"moov", 4),
            (b"mdat", 4),
            (b"wide", 4),
            (b"free", 4),
            (b"ftyp", 4),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:mov;video")
    }
}

/// AVI video adapter
pub struct AviAdapter;

impl MediaAdapter for AviAdapter {
    fn name(&self) -> &'static str { "avi" }

    fn extensions(&self) -> &'static [&'static str] {
        &["avi"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"RIFF", 0),
        ]
    }

    fn detect(&self, _path: &Path, content: &[u8]) -> AdapterResult {
        // Verify AVI signature at offset 8
        if content.len() >= 12 && &content[8..12] == b"AVI " {
            AdapterResult::scalar_opaque("media:avi;video")
        } else {
            AdapterResult::scalar_opaque("media:avi;video")
        }
    }
}

/// MPEG video adapter
pub struct MpegAdapter;

impl MediaAdapter for MpegAdapter {
    fn name(&self) -> &'static str { "mpeg" }

    fn extensions(&self) -> &'static [&'static str] {
        &["mpeg", "mpg", "mpe", "mpv", "m1v", "m2v", "mp2", "mpv2", "vob"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            // MPEG Program Stream
            (b"\x00\x00\x01\xBA", 0),
            // MPEG Video
            (b"\x00\x00\x01\xB3", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:mpeg;video")
    }
}

/// Windows Media Video adapter
pub struct WmvAdapter;

impl MediaAdapter for WmvAdapter {
    fn name(&self) -> &'static str { "wmv" }

    fn extensions(&self) -> &'static [&'static str] {
        &["wmv"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            // ASF header GUID
            (b"\x30\x26\xB2\x75\x8E\x66\xCF\x11", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:wmv;video")
    }
}

/// Flash Video adapter
pub struct FlvAdapter;

impl MediaAdapter for FlvAdapter {
    fn name(&self) -> &'static str { "flv" }

    fn extensions(&self) -> &'static [&'static str] {
        &["flv", "f4v"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"FLV", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:flv;video")
    }
}

/// 3GP mobile video adapter
pub struct ThreeGpAdapter;

impl MediaAdapter for ThreeGpAdapter {
    fn name(&self) -> &'static str { "3gp" }

    fn extensions(&self) -> &'static [&'static str] {
        &["3gp", "3g2", "3gpp", "3gpp2"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:3gp;video")
    }
}

/// Ogg Video adapter
pub struct OgvAdapter;

impl MediaAdapter for OgvAdapter {
    fn name(&self) -> &'static str { "ogv" }

    fn extensions(&self) -> &'static [&'static str] {
        &["ogv"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"OggS", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:ogv;video")
    }
}

/// MPEG Transport Stream adapter
pub struct TsAdapter;

impl MediaAdapter for TsAdapter {
    fn name(&self) -> &'static str { "ts" }

    fn extensions(&self) -> &'static [&'static str] {
        &["ts", "mts", "m2ts"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            // TS sync byte
            (b"\x47", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:ts;video")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use crate::input_resolver::ContentStructure;

    // TEST1083: MP4 extension mapping
    #[test]
    fn test1083_mp4_extension() {
        let adapter = Mp4Adapter;
        let path = PathBuf::from("video.mp4");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:mp4;video");
        assert_eq!(result.content_structure, ContentStructure::ScalarOpaque);
    }

    #[test]
    fn test_mov_extension() {
        let adapter = MovAdapter;
        let path = PathBuf::from("clip.mov");

        assert!(adapter.matches(&path, &[]).matches());
    }

    #[test]
    fn test_webm_extension() {
        let adapter = WebmAdapter;
        let path = PathBuf::from("animation.webm");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:webm;video");
    }
}
