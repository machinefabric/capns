//! Core types for InputResolver

use std::path::PathBuf;
use std::fmt;

use crate::planner::InputCardinality;

/// A single input specification from the user
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputItem {
    /// A single file path
    File(PathBuf),
    /// A directory path (resolve recursively)
    Directory(PathBuf),
    /// A glob pattern (e.g., "*.pdf", "/tmp/**/*.json")
    Glob(String),
}

impl InputItem {
    /// Create from a string, auto-detecting the type
    pub fn from_string(s: &str) -> Self {
        // Check for glob metacharacters
        if s.contains('*') || s.contains('?') || s.contains('[') {
            return InputItem::Glob(s.to_string());
        }

        let path = PathBuf::from(s);

        // If path exists, check if it's a directory
        if path.is_dir() {
            InputItem::Directory(path)
        } else {
            // Assume file (existence checked during resolution)
            InputItem::File(path)
        }
    }
}

/// The detected internal structure of file content
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContentStructure {
    /// Single opaque value (no list, no record markers)
    /// Examples: PDF, PNG, single string
    ScalarOpaque,

    /// Single structured record (no list, has record marker)
    /// Examples: JSON object, TOML file, config file
    ScalarRecord,

    /// List of opaque values (has list, no record markers)
    /// Examples: array of strings, multi-line text, JSON array of primitives
    ListOpaque,

    /// List of records (has list and record markers)
    /// Examples: CSV with headers, NDJSON of objects, JSON array of objects
    ListRecord,
}

impl ContentStructure {
    /// Returns true if this structure has the `list` marker
    pub fn is_list(&self) -> bool {
        matches!(self, ContentStructure::ListOpaque | ContentStructure::ListRecord)
    }

    /// Returns true if this structure has the `record` marker
    pub fn is_record(&self) -> bool {
        matches!(self, ContentStructure::ScalarRecord | ContentStructure::ListRecord)
    }

    /// Convert to InputCardinality (for compatibility with planner)
    pub fn to_cardinality(&self) -> InputCardinality {
        if self.is_list() {
            InputCardinality::Sequence
        } else {
            InputCardinality::Single
        }
    }
}

impl fmt::Display for ContentStructure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContentStructure::ScalarOpaque => write!(f, "scalar/opaque"),
            ContentStructure::ScalarRecord => write!(f, "scalar/record"),
            ContentStructure::ListOpaque => write!(f, "list/opaque"),
            ContentStructure::ListRecord => write!(f, "list/record"),
        }
    }
}

/// A single resolved file with detected media information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFile {
    /// Absolute path to the file
    pub path: PathBuf,

    /// Detected media URN (includes list/record markers if applicable)
    pub media_urn: String,

    /// File size in bytes
    pub size_bytes: u64,

    /// Content structure detected from inspection
    pub content_structure: ContentStructure,
}

/// The complete result of input resolution
#[derive(Debug, Clone)]
pub struct ResolvedInputSet {
    /// All resolved files
    pub files: Vec<ResolvedFile>,

    /// Aggregate cardinality (Single if 1 scalar file, Sequence otherwise)
    pub cardinality: InputCardinality,

    /// Common base media type (if files share a type), or None if heterogeneous
    pub common_media: Option<String>,
}

impl ResolvedInputSet {
    /// Create a new ResolvedInputSet from files
    pub fn new(files: Vec<ResolvedFile>) -> Self {
        let cardinality = Self::compute_cardinality(&files);
        let common_media = Self::compute_common_media(&files);

        ResolvedInputSet {
            files,
            cardinality,
            common_media,
        }
    }

    fn compute_cardinality(files: &[ResolvedFile]) -> InputCardinality {
        match files.len() {
            0 => InputCardinality::Single, // Edge case, should be error
            1 => {
                // Single file: cardinality depends on content structure
                if files[0].content_structure.is_list() {
                    InputCardinality::Sequence
                } else {
                    InputCardinality::Single
                }
            }
            _ => InputCardinality::Sequence, // Multiple files always sequence
        }
    }

    fn compute_common_media(files: &[ResolvedFile]) -> Option<String> {
        if files.is_empty() {
            return None;
        }

        // Extract base media type (before any markers)
        let first_base = Self::extract_base_media(&files[0].media_urn);

        for file in files.iter().skip(1) {
            let base = Self::extract_base_media(&file.media_urn);
            if base != first_base {
                return None;
            }
        }

        Some(first_base)
    }

    /// Extract base media type from URN (e.g., "media:json;record;textable" -> "json")
    fn extract_base_media(urn: &str) -> String {
        // Strip "media:" prefix
        let without_prefix = urn.strip_prefix("media:").unwrap_or(urn);

        // Take first segment (before any semicolon)
        without_prefix
            .split(';')
            .next()
            .unwrap_or("")
            .to_string()
    }

    /// Returns true if all files have the same base media type
    pub fn is_homogeneous(&self) -> bool {
        self.common_media.is_some()
    }

    /// Returns the number of files
    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Returns true if no files
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

/// Errors that can occur during input resolution
#[derive(Debug)]
pub enum InputResolverError {
    /// Path does not exist
    NotFound(PathBuf),

    /// Permission denied when accessing path
    PermissionDenied(PathBuf),

    /// Invalid glob pattern
    InvalidGlob {
        pattern: String,
        reason: String,
    },

    /// IO error during resolution
    IoError {
        path: PathBuf,
        error: std::io::Error,
    },

    /// Content inspection failed
    InspectionFailed {
        path: PathBuf,
        reason: String,
    },

    /// Empty input (no paths provided)
    EmptyInput,

    /// All paths resolved to zero files
    NoFilesResolved,

    /// Symlink cycle detected
    SymlinkCycle {
        path: PathBuf,
    },
}

impl fmt::Display for InputResolverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputResolverError::NotFound(path) => {
                write!(f, "Path not found: {}", path.display())
            }
            InputResolverError::PermissionDenied(path) => {
                write!(f, "Permission denied: {}", path.display())
            }
            InputResolverError::InvalidGlob { pattern, reason } => {
                write!(f, "Invalid glob pattern '{}': {}", pattern, reason)
            }
            InputResolverError::IoError { path, error } => {
                write!(f, "IO error at {}: {}", path.display(), error)
            }
            InputResolverError::InspectionFailed { path, reason } => {
                write!(f, "Content inspection failed for {}: {}", path.display(), reason)
            }
            InputResolverError::EmptyInput => {
                write!(f, "No input paths provided")
            }
            InputResolverError::NoFilesResolved => {
                write!(f, "No files found after resolving all inputs")
            }
            InputResolverError::SymlinkCycle { path } => {
                write!(f, "Symlink cycle detected at: {}", path.display())
            }
        }
    }
}

impl std::error::Error for InputResolverError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            InputResolverError::IoError { error, .. } => Some(error),
            _ => None,
        }
    }
}
