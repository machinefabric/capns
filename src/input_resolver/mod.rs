//! InputResolver — Unified input resolution with pluggable media adapters
//!
//! This module resolves mixed file/directory/glob inputs into a flat list of files
//! with detected media types, cardinality, and structure markers.
//!
//! ## Architecture
//!
//! ```text
//! InputResolver
//!   ├── PathResolver: expands files/dirs/globs → Vec<PathBuf>
//!   ├── OsFileFilter: excludes OS artifacts (.DS_Store, etc.)
//!   └── MediaAdapterRegistry
//!         ├── PdfAdapter
//!         ├── JsonAdapter (inspects content)
//!         ├── CsvAdapter (inspects content)
//!         ├── YamlAdapter (inspects content)
//!         ├── ... (one adapter per file type)
//!         └── FallbackAdapter (unknown types)
//! ```
//!
//! Each adapter implements `MediaAdapter` and is responsible for:
//! 1. Matching: Does this adapter handle this file? (by extension or magic bytes)
//! 2. Detection: Given file content, produce media URN with correct list/record markers
//!
//! ## Usage
//!
//! ```ignore
//! use capdag::input_resolver::{resolve_paths, InputItem};
//!
//! // Resolve mixed inputs
//! let result = resolve_paths(&["/path/to/file.pdf", "/path/to/dir/", "*.json"])?;
//! for file in result.files {
//!     println!("{}: {} ({:?})", file.path.display(), file.media_urn, file.content_structure);
//! }
//! ```

mod types;
mod os_filter;
mod path_resolver;
mod adapter;
mod adapters;
mod resolver;

pub use types::{
    InputItem,
    ContentStructure,
    ResolvedFile,
    ResolvedInputSet,
    InputResolverError,
};

pub use adapter::{
    MediaAdapter,
    AdapterMatch,
    AdapterResult,
};

pub use resolver::{
    resolve_input,
    resolve_inputs,
    resolve_paths,
    detect_file,
};

// Re-export adapter registry for extensibility
pub use adapters::MediaAdapterRegistry;
