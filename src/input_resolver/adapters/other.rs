//! Other file type adapters — fonts, 3D models, ML models, databases, etc.

use std::path::Path;
use crate::input_resolver::adapter::{MediaAdapter, AdapterResult};
use crate::input_resolver::ContentStructure;

/// Font adapter (TTF, OTF, WOFF, etc.)
pub struct FontAdapter;

impl MediaAdapter for FontAdapter {
    fn name(&self) -> &'static str { "font" }

    fn extensions(&self) -> &'static [&'static str] {
        &["ttf", "otf", "woff", "woff2", "eot", "ttc"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"\x00\x01\x00\x00", 0), // TrueType
            (b"OTTO", 0),             // OpenType
            (b"wOFF", 0),             // WOFF
            (b"wOF2", 0),             // WOFF2
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = format!("media:{};font", ext);
        AdapterResult::scalar_opaque(media)
    }
}

/// 3D model adapter
pub struct Model3DAdapter;

impl MediaAdapter for Model3DAdapter {
    fn name(&self) -> &'static str { "3d" }

    fn extensions(&self) -> &'static [&'static str] {
        &[
            "obj", "stl", "fbx", "gltf", "glb", "dae", "blend", "3ds",
            "ply", "step", "stp", "iges", "igs", "dwg", "dxf", "usdz", "usd"
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let (media, structure) = match ext.as_str() {
            "gltf" => ("media:gltf;model;textable", ContentStructure::ScalarRecord),
            "dxf" => ("media:dxf;cad;textable", ContentStructure::ScalarOpaque),
            "step" | "stp" => ("media:step;cad", ContentStructure::ScalarOpaque),
            "iges" | "igs" => ("media:iges;cad", ContentStructure::ScalarOpaque),
            "dwg" => ("media:dwg;cad", ContentStructure::ScalarOpaque),
            _ => {
                let m = format!("media:{};model", ext);
                return AdapterResult::scalar_opaque(m);
            }
        };

        AdapterResult {
            media_urn: media.to_string(),
            content_structure: structure,
        }
    }
}

/// ML model adapter
pub struct MlModelAdapter;

impl MediaAdapter for MlModelAdapter {
    fn name(&self) -> &'static str { "mlmodel" }

    fn extensions(&self) -> &'static [&'static str] {
        &[
            "gguf", "ggml", "safetensors", "pt", "pth", "onnx",
            "mlmodel", "mlpackage", "h5", "hdf5", "pb", "tflite",
            "caffemodel", "npy", "npz", "pkl", "joblib"
        ]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"GGUF", 0),  // GGUF format
            (b"lmgg", 0),  // GGML format (reversed)
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "gguf" => "media:gguf;model",
            "ggml" => "media:ggml;model",
            "safetensors" => "media:safetensors;model",
            "pt" | "pth" => "media:pytorch;model",
            "onnx" => "media:onnx;model",
            "mlmodel" | "mlpackage" => "media:coreml;model",
            "h5" | "hdf5" => "media:hdf5",
            "tflite" => "media:tflite;model",
            "npy" => "media:numpy",
            "npz" => "media:numpy;archive",
            "pkl" | "joblib" => "media:pickle",
            _ => "media:mlmodel",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Database adapter
pub struct DatabaseAdapter;

impl MediaAdapter for DatabaseAdapter {
    fn name(&self) -> &'static str { "database" }

    fn extensions(&self) -> &'static [&'static str] {
        &["sqlite", "sqlite3", "db", "mdb", "accdb"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"SQLite format 3", 0),
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "sqlite" | "sqlite3" | "db" => "media:sqlite",
            "mdb" | "accdb" => "media:access",
            _ => "media:database",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Columnar data adapter (Parquet, Arrow, etc.)
pub struct ColumnarDataAdapter;

impl MediaAdapter for ColumnarDataAdapter {
    fn name(&self) -> &'static str { "columnar" }

    fn extensions(&self) -> &'static [&'static str] {
        &["parquet", "arrow", "feather", "avro", "orc"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"PAR1", 0),         // Parquet
            (b"ARROW1", 0),       // Arrow IPC
            (b"Obj\x01", 0),      // Avro
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "parquet" => "media:parquet;list;record",
            "arrow" | "feather" => "media:arrow;list;record",
            "avro" => "media:avro;list;record",
            "orc" => "media:orc;list;record",
            _ => "media:columnar;list;record",
        };

        AdapterResult::list_record(media)
    }
}

/// Certificate adapter
pub struct CertificateAdapter;

impl MediaAdapter for CertificateAdapter {
    fn name(&self) -> &'static str { "cert" }

    fn extensions(&self) -> &'static [&'static str] {
        &["pem", "crt", "cer", "der", "key", "csr", "p12", "pfx", "p7b", "p7c"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "pem" => "media:pem;textable",
            "crt" | "cer" | "der" => "media:cert",
            "key" => "media:key",
            "csr" => "media:csr",
            "p12" | "pfx" => "media:pkcs12",
            "p7b" | "p7c" => "media:pkcs7",
            _ => "media:cert",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// GPG/PGP adapter
pub struct GpgAdapter;

impl MediaAdapter for GpgAdapter {
    fn name(&self) -> &'static str { "gpg" }

    fn extensions(&self) -> &'static [&'static str] {
        &["gpg", "pgp", "asc", "sig", "pub"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"-----BEGIN PGP", 0),
            (b"-----BEGIN GPG", 0),
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "asc" => "media:gpg;textable",
            "sig" => "media:sig",
            "pub" => "media:pubkey;textable",
            _ => "media:gpg",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Geospatial adapter
pub struct GeoAdapter;

impl MediaAdapter for GeoAdapter {
    fn name(&self) -> &'static str { "geo" }

    fn extensions(&self) -> &'static [&'static str] {
        &["kml", "kmz", "gpx", "shp", "dbf", "shx", "prj"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "kml" => "media:kml;textable",
            "kmz" => "media:kmz",
            "gpx" => "media:gpx;textable",
            "shp" | "dbf" | "shx" | "prj" => "media:shapefile",
            _ => "media:geo",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Subtitle adapter
pub struct SubtitleAdapter;

impl MediaAdapter for SubtitleAdapter {
    fn name(&self) -> &'static str { "subtitle" }

    fn extensions(&self) -> &'static [&'static str] {
        &["srt", "vtt", "ass", "ssa", "sub"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        // Subtitles are lists of timed entries (records)
        let media = format!("media:{};list;record;textable", ext);
        AdapterResult::list_record(media)
    }
}

/// Email/Calendar adapter
pub struct EmailAdapter;

impl MediaAdapter for EmailAdapter {
    fn name(&self) -> &'static str { "email" }

    fn extensions(&self) -> &'static [&'static str] {
        &["eml", "msg", "mbox", "ics", "vcf", "vcard"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let (media, structure) = match ext.as_str() {
            "eml" => ("media:eml;record;textable", ContentStructure::ScalarRecord),
            "msg" => ("media:msg", ContentStructure::ScalarOpaque),
            "mbox" => ("media:mbox;list;record;textable", ContentStructure::ListRecord),
            "ics" => ("media:ics;list;record;textable", ContentStructure::ListRecord),
            "vcf" | "vcard" => ("media:vcf;list;record;textable", ContentStructure::ListRecord),
            _ => ("media:email", ContentStructure::ScalarOpaque),
        };

        AdapterResult {
            media_urn: media.to_string(),
            content_structure: structure,
        }
    }
}

/// Jupyter notebook adapter
pub struct JupyterAdapter;

impl MediaAdapter for JupyterAdapter {
    fn name(&self) -> &'static str { "jupyter" }

    fn extensions(&self) -> &'static [&'static str] {
        &["ipynb"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        // Jupyter notebooks are JSON with a specific structure
        AdapterResult::scalar_record("media:jupyter;record;textable")
    }
}

/// Binary serialization adapter (protobuf, msgpack, cbor, bson)
pub struct BinarySerializationAdapter;

impl MediaAdapter for BinarySerializationAdapter {
    fn name(&self) -> &'static str { "binserialization" }

    fn extensions(&self) -> &'static [&'static str] {
        &["protobuf", "pb", "msgpack", "cbor", "bson"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "protobuf" | "pb" => "media:protobuf",
            "msgpack" => "media:msgpack",
            "cbor" => "media:cbor",
            "bson" => "media:bson",
            _ => "media:binary",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Scientific data adapter
pub struct ScientificAdapter;

impl MediaAdapter for ScientificAdapter {
    fn name(&self) -> &'static str { "scientific" }

    fn extensions(&self) -> &'static [&'static str] {
        &["nc", "nc4", "fits", "mat", "sav", "rdata", "rds"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "nc" | "nc4" => "media:netcdf",
            "fits" => "media:fits",
            "mat" => "media:matlab",
            "rdata" | "rds" => "media:rdata",
            _ => "media:scientific",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// WebAssembly adapter
pub struct WasmAdapter;

impl MediaAdapter for WasmAdapter {
    fn name(&self) -> &'static str { "wasm" }

    fn extensions(&self) -> &'static [&'static str] {
        &["wasm", "wat"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"\x00asm", 0),
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "wat" => "media:wat;textable;code",
            _ => "media:wasm",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Fallback adapter for unknown file types
pub struct FallbackAdapter;

impl MediaAdapter for FallbackAdapter {
    fn name(&self) -> &'static str { "fallback" }

    fn extensions(&self) -> &'static [&'static str] {
        &[] // Matches nothing by extension
    }

    fn matches(&self, _path: &Path, _content_prefix: &[u8]) -> crate::input_resolver::adapter::AdapterMatch {
        // Always matches as fallback
        crate::input_resolver::adapter::AdapterMatch::ByExtension
    }

    fn priority(&self) -> i32 {
        // Lowest priority - only matches if nothing else does
        i32::MIN
    }

    fn detect(&self, path: &Path, content: &[u8]) -> AdapterResult {
        // Try to detect if it's text or binary
        let is_text = content.iter().take(1024).all(|&b| {
            b == b'\n' || b == b'\r' || b == b'\t' || (b >= 0x20 && b < 0x7f) || b >= 0x80
        });

        if is_text {
            // Check for common text patterns
            let text = std::str::from_utf8(content).unwrap_or("");

            // Check if it looks like JSON
            let trimmed = text.trim();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                return super::data::JsonAdapter.detect(path, content);
            }

            // Check if it looks like XML
            if trimmed.starts_with('<') {
                return super::data::XmlAdapter.detect(path, content);
            }

            // Generic textable
            AdapterResult::scalar_opaque("media:textable")
        } else {
            // Binary file with unknown extension
            if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                AdapterResult::scalar_opaque(format!("media:ext={}", ext.to_lowercase()))
            } else {
                AdapterResult::scalar_opaque("media:")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // TEST1089: Unknown extension fallback
    #[test]
    fn test1089_unknown_extension() {
        let adapter = FallbackAdapter;
        let path = PathBuf::from("file.xyz");
        let content = b"some binary content \x00\x01\x02";

        let result = adapter.detect(&path, content);
        assert_eq!(result.media_urn, "media:ext=xyz");
        assert_eq!(result.content_structure, ContentStructure::ScalarOpaque);
    }

    #[test]
    fn test_fallback_text_detection() {
        let adapter = FallbackAdapter;
        let path = PathBuf::from("file.unknown");
        let content = b"This is plain text content\nWith multiple lines\n";

        let result = adapter.detect(&path, content);
        assert!(result.media_urn.contains("textable"));
    }

    #[test]
    fn test_fallback_json_detection() {
        let adapter = FallbackAdapter;
        let path = PathBuf::from("file.unknown");
        let content = br#"{"key": "value"}"#;

        let result = adapter.detect(&path, content);
        assert!(result.media_urn.contains("json"));
    }

    #[test]
    fn test_sqlite_magic() {
        let adapter = DatabaseAdapter;
        let path = PathBuf::from("data");
        let content = b"SQLite format 3\x00";

        assert!(adapter.matches(&path, content).matches());
    }

    #[test]
    fn test_parquet_is_list_record() {
        let adapter = ColumnarDataAdapter;
        let path = PathBuf::from("data.parquet");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.content_structure, ContentStructure::ListRecord);
    }

    #[test]
    fn test_subtitle_is_list_record() {
        let adapter = SubtitleAdapter;
        let path = PathBuf::from("captions.srt");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.content_structure, ContentStructure::ListRecord);
    }
}
