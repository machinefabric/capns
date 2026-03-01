//! Audio adapters — WAV, MP3, FLAC, etc.

use std::path::Path;
use crate::input_resolver::adapter::{MediaAdapter, AdapterResult};

/// WAV audio adapter
pub struct WavAdapter;

impl MediaAdapter for WavAdapter {
    fn name(&self) -> &'static str { "wav" }

    fn extensions(&self) -> &'static [&'static str] {
        &["wav", "wave"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"RIFF", 0),
        ]
    }

    fn detect(&self, _path: &Path, content: &[u8]) -> AdapterResult {
        // Verify WAVE signature at offset 8
        if content.len() >= 12 && &content[8..12] == b"WAVE" {
            AdapterResult::scalar_opaque("media:wav;audio")
        } else {
            AdapterResult::scalar_opaque("media:wav;audio")
        }
    }
}

/// MP3 audio adapter
pub struct Mp3Adapter;

impl MediaAdapter for Mp3Adapter {
    fn name(&self) -> &'static str { "mp3" }

    fn extensions(&self) -> &'static [&'static str] {
        &["mp3"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"ID3", 0),           // ID3v2 tag
            (b"\xFF\xFB", 0),      // MPEG Audio frame sync
            (b"\xFF\xFA", 0),
            (b"\xFF\xF3", 0),
            (b"\xFF\xF2", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:mp3;audio")
    }
}

/// FLAC audio adapter
pub struct FlacAdapter;

impl MediaAdapter for FlacAdapter {
    fn name(&self) -> &'static str { "flac" }

    fn extensions(&self) -> &'static [&'static str] {
        &["flac"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"fLaC", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:flac;audio")
    }
}

/// AAC audio adapter
pub struct AacAdapter;

impl MediaAdapter for AacAdapter {
    fn name(&self) -> &'static str { "aac" }

    fn extensions(&self) -> &'static [&'static str] {
        &["aac", "m4a", "m4b", "m4p", "m4r"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "m4a" | "m4b" | "m4p" | "m4r" => "media:m4a;audio",
            _ => "media:aac;audio",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Ogg Vorbis/Opus adapter
pub struct OggAdapter;

impl MediaAdapter for OggAdapter {
    fn name(&self) -> &'static str { "ogg" }

    fn extensions(&self) -> &'static [&'static str] {
        &["ogg", "oga", "ogx", "spx"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"OggS", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:ogg;audio")
    }
}

/// Opus audio adapter
pub struct OpusAdapter;

impl MediaAdapter for OpusAdapter {
    fn name(&self) -> &'static str { "opus" }

    fn extensions(&self) -> &'static [&'static str] {
        &["opus"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:opus;audio")
    }

    fn priority(&self) -> i32 {
        // Higher priority than OggAdapter since Opus uses Ogg container
        1
    }
}

/// AIFF audio adapter
pub struct AiffAdapter;

impl MediaAdapter for AiffAdapter {
    fn name(&self) -> &'static str { "aiff" }

    fn extensions(&self) -> &'static [&'static str] {
        &["aiff", "aif", "aifc"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"FORM", 0),
        ]
    }

    fn detect(&self, _path: &Path, content: &[u8]) -> AdapterResult {
        // Verify AIFF signature at offset 8
        if content.len() >= 12 {
            if &content[8..12] == b"AIFF" || &content[8..12] == b"AIFC" {
                return AdapterResult::scalar_opaque("media:aiff;audio");
            }
        }
        AdapterResult::scalar_opaque("media:aiff;audio")
    }
}

/// MIDI audio adapter
pub struct MidiAdapter;

impl MediaAdapter for MidiAdapter {
    fn name(&self) -> &'static str { "midi" }

    fn extensions(&self) -> &'static [&'static str] {
        &["mid", "midi", "kar"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"MThd", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:midi;audio")
    }
}

/// Windows Media Audio adapter
pub struct WmaAdapter;

impl MediaAdapter for WmaAdapter {
    fn name(&self) -> &'static str { "wma" }

    fn extensions(&self) -> &'static [&'static str] {
        &["wma", "asf"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            // ASF header GUID
            (b"\x30\x26\xB2\x75\x8E\x66\xCF\x11", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:wma;audio")
    }
}

/// Core Audio Format adapter
pub struct CafAdapter;

impl MediaAdapter for CafAdapter {
    fn name(&self) -> &'static str { "caf" }

    fn extensions(&self) -> &'static [&'static str] {
        &["caf"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"caff", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:caf;audio")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use crate::input_resolver::ContentStructure;

    // TEST1082: MP3 extension mapping
    #[test]
    fn test1082_mp3_extension() {
        let adapter = Mp3Adapter;
        let path = PathBuf::from("song.mp3");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:mp3;audio");
        assert_eq!(result.content_structure, ContentStructure::ScalarOpaque);
    }

    #[test]
    fn test_wav_magic() {
        let adapter = WavAdapter;
        let path = PathBuf::from("audio");
        let content = b"RIFF\x00\x00\x00\x00WAVEfmt ";

        assert!(adapter.matches(&path, content).matches());
    }

    #[test]
    fn test_flac_magic() {
        let adapter = FlacAdapter;
        let path = PathBuf::from("music");
        let content = b"fLaC\x00\x00\x00\x22";

        assert!(adapter.matches(&path, content).matches());
    }
}
