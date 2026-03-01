# InputResolver Design Document

This document specifies the **InputResolver** module: a unified system for resolving mixed file/directory/glob inputs into a flat list of files with detected media types, cardinality, and structure markers.

## Overview

InputResolver takes heterogeneous input (files, directories, globs, or any mix) and produces a uniform `ResolvedInputSet` that captures:
1. The list of resolved file paths
2. For each file: detected media URN with appropriate `list` and `record` markers
3. Aggregate cardinality information for the entire input set

This module lives in `capdag` (Rust) and is mirrored in `capdag-objc` (Swift/ObjC).

---

## Core Types

### `InputItem`

A single input specification from the user.

```rust
pub enum InputItem {
    /// A single file path
    File(PathBuf),
    /// A directory path (resolve recursively)
    Directory(PathBuf),
    /// A glob pattern (e.g., "*.pdf", "/tmp/**/*.json")
    Glob(String),
}
```

### `ResolvedFile`

A single resolved file with detected media information.

```rust
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
```

### `ContentStructure`

The detected internal structure of file content.

```rust
pub enum ContentStructure {
    /// Single opaque value (no list, no record markers)
    ScalarOpaque,
    /// Single structured record (no list, has record marker)
    ScalarRecord,
    /// List of opaque values (has list, no record markers)
    ListOpaque,
    /// List of records (has list and record markers)
    ListRecord,
}
```

### `ResolvedInputSet`

The complete result of input resolution.

```rust
pub struct ResolvedInputSet {
    /// All resolved files
    pub files: Vec<ResolvedFile>,
    /// Aggregate cardinality (Single if 1 file, Sequence if >1)
    pub cardinality: InputCardinality,
    /// Common base media URN (if files share a type), or None if heterogeneous
    pub common_media: Option<String>,
}
```

---

## Resolution Rules

### 1. Path Expansion

| Input Type | Behavior |
|------------|----------|
| File path | Verify exists, add to results |
| Directory | Recursive enumeration with OS file filtering |
| Glob pattern | Expand using glob crate, files only |
| Mixed inputs | Process each, flatten results, deduplicate by path |

### 2. OS File Filtering

These files are **always excluded** (OS artifacts, not user content):

| Pattern | Reason |
|---------|--------|
| `.DS_Store` | macOS folder metadata |
| `Thumbs.db` | Windows thumbnail cache |
| `desktop.ini` | Windows folder settings |
| `.Spotlight-V100/` | macOS Spotlight index |
| `.Trashes/` | macOS trash |
| `._*` | macOS resource forks |
| `.fseventsd/` | macOS filesystem events |
| `.TemporaryItems/` | macOS temp files |
| `~$*` | Office lock files |
| `*.tmp`, `*.temp` | Temporary files |
| `.git/`, `.svn/`, `.hg/` | Version control dirs |
| `__MACOSX/` | macOS archive artifacts |
| `.localized` | macOS localization |
| `Icon\r` | macOS custom folder icons |
| `ehthumbs.db` | Windows thumbnail cache |
| `ehthumbs_vista.db` | Windows Vista thumbnails |

Note: We do NOT filter by content extension (`.json`, `.log`, `.md`, etc.) - all user content files are valid inputs.

---

## Comprehensive File Type Catalog

### Documents

| Extension(s) | Base Media URN | Structure | Inspection | Notes |
|--------------|----------------|-----------|------------|-------|
| `.pdf` | `media:pdf` | ScalarOpaque | No | Binary document |
| `.epub` | `media:epub` | ScalarOpaque | No | Binary ebook |
| `.mobi`, `.azw`, `.azw3` | `media:mobi` | ScalarOpaque | No | Kindle formats |
| `.djvu` | `media:djvu` | ScalarOpaque | No | DjVu document |
| `.doc` | `media:doc` | ScalarOpaque | No | Legacy Word |
| `.docx` | `media:docx` | ScalarOpaque | No | Word document |
| `.odt` | `media:odt` | ScalarOpaque | No | OpenDocument text |
| `.rtf` | `media:rtf;textable` | ScalarOpaque | No | Rich text |
| `.xls` | `media:xls` | ScalarOpaque | No | Legacy Excel |
| `.xlsx` | `media:xlsx` | ScalarOpaque | No | Excel spreadsheet |
| `.ods` | `media:ods` | ScalarOpaque | No | OpenDocument spreadsheet |
| `.ppt` | `media:ppt` | ScalarOpaque | No | Legacy PowerPoint |
| `.pptx` | `media:pptx` | ScalarOpaque | No | PowerPoint |
| `.odp` | `media:odp` | ScalarOpaque | No | OpenDocument presentation |
| `.pages` | `media:pages` | ScalarOpaque | No | Apple Pages |
| `.numbers` | `media:numbers` | ScalarOpaque | No | Apple Numbers |
| `.keynote` | `media:keynote` | ScalarOpaque | No | Apple Keynote |
| `.xps` | `media:xps` | ScalarOpaque | No | XML Paper Specification |
| `.oxps` | `media:oxps` | ScalarOpaque | No | Open XPS |

### Images

| Extension(s) | Base Media URN | Structure | Inspection | Notes |
|--------------|----------------|-----------|------------|-------|
| `.png` | `media:png;image` | ScalarOpaque | No | Lossless raster |
| `.jpg`, `.jpeg` | `media:jpeg;image` | ScalarOpaque | No | Lossy raster |
| `.gif` | `media:gif;image` | ScalarOpaque | No | Animated/static |
| `.webp` | `media:webp;image` | ScalarOpaque | No | Modern web format |
| `.avif` | `media:avif;image` | ScalarOpaque | No | AV1 image |
| `.heic`, `.heif` | `media:heic;image` | ScalarOpaque | No | Apple HEIF |
| `.tiff`, `.tif` | `media:tiff;image` | ScalarOpaque | No | High-quality raster |
| `.bmp` | `media:bmp;image` | ScalarOpaque | No | Bitmap |
| `.ico` | `media:ico;image` | ScalarOpaque | No | Windows icon |
| `.icns` | `media:icns;image` | ScalarOpaque | No | macOS icon |
| `.svg` | `media:svg;image;textable` | ScalarOpaque | No | Vector (XML-based) |
| `.eps` | `media:eps;image` | ScalarOpaque | No | Encapsulated PostScript |
| `.ai` | `media:ai;image` | ScalarOpaque | No | Adobe Illustrator |
| `.psd` | `media:psd;image` | ScalarOpaque | No | Photoshop |
| `.xcf` | `media:xcf;image` | ScalarOpaque | No | GIMP |
| `.raw`, `.cr2`, `.nef`, `.arw`, `.dng` | `media:raw;image` | ScalarOpaque | No | Camera RAW formats |
| `.exr` | `media:exr;image` | ScalarOpaque | No | OpenEXR HDR |
| `.hdr` | `media:hdr;image` | ScalarOpaque | No | Radiance HDR |

### Audio

| Extension(s) | Base Media URN | Structure | Inspection | Notes |
|--------------|----------------|-----------|------------|-------|
| `.wav` | `media:wav;audio` | ScalarOpaque | No | Uncompressed PCM |
| `.mp3` | `media:mp3;audio` | ScalarOpaque | No | MPEG Layer 3 |
| `.flac` | `media:flac;audio` | ScalarOpaque | No | Lossless |
| `.aac` | `media:aac;audio` | ScalarOpaque | No | Advanced Audio |
| `.m4a` | `media:m4a;audio` | ScalarOpaque | No | MPEG-4 Audio |
| `.ogg`, `.oga` | `media:ogg;audio` | ScalarOpaque | No | Ogg Vorbis |
| `.opus` | `media:opus;audio` | ScalarOpaque | No | Opus codec |
| `.wma` | `media:wma;audio` | ScalarOpaque | No | Windows Media |
| `.aiff`, `.aif` | `media:aiff;audio` | ScalarOpaque | No | Apple uncompressed |
| `.mid`, `.midi` | `media:midi;audio` | ScalarOpaque | No | MIDI sequence |
| `.caf` | `media:caf;audio` | ScalarOpaque | No | Core Audio Format |

### Video

| Extension(s) | Base Media URN | Structure | Inspection | Notes |
|--------------|----------------|-----------|------------|-------|
| `.mp4`, `.m4v` | `media:mp4;video` | ScalarOpaque | No | MPEG-4 |
| `.webm` | `media:webm;video` | ScalarOpaque | No | WebM/VP9 |
| `.mkv` | `media:mkv;video` | ScalarOpaque | No | Matroska |
| `.mov` | `media:mov;video` | ScalarOpaque | No | QuickTime |
| `.avi` | `media:avi;video` | ScalarOpaque | No | AVI container |
| `.wmv` | `media:wmv;video` | ScalarOpaque | No | Windows Media |
| `.flv` | `media:flv;video` | ScalarOpaque | No | Flash Video |
| `.mpeg`, `.mpg` | `media:mpeg;video` | ScalarOpaque | No | MPEG |
| `.3gp`, `.3g2` | `media:3gp;video` | ScalarOpaque | No | Mobile video |
| `.ogv` | `media:ogv;video` | ScalarOpaque | No | Ogg Video |
| `.ts`, `.mts`, `.m2ts` | `media:ts;video` | ScalarOpaque | No | MPEG-TS |

### Plain Text

| Extension(s) | Base Media URN | Structure | Inspection | Notes |
|--------------|----------------|-----------|------------|-------|
| `.txt` | `media:txt;textable` | Inspect | Yes | May be multi-line list |
| `.text` | `media:txt;textable` | Inspect | Yes | Same as .txt |
| `.log` | `media:log;textable` | ListOpaque | No | Always multi-line |
| `.out` | `media:log;textable` | ListOpaque | No | Program output |

### Markup & Documentation

| Extension(s) | Base Media URN | Structure | Inspection | Notes |
|--------------|----------------|-----------|------------|-------|
| `.md`, `.markdown` | `media:md;textable` | ScalarOpaque | No | Markdown |
| `.rst` | `media:rst;textable` | ScalarOpaque | No | reStructuredText |
| `.adoc`, `.asciidoc` | `media:asciidoc;textable` | ScalarOpaque | No | AsciiDoc |
| `.tex`, `.latex` | `media:tex;textable` | ScalarOpaque | No | LaTeX |
| `.org` | `media:org;textable` | ScalarOpaque | No | Org-mode |
| `.wiki` | `media:wiki;textable` | ScalarOpaque | No | Wiki markup |
| `.pod` | `media:pod;textable` | ScalarOpaque | No | Perl POD |
| `.man` | `media:man;textable` | ScalarOpaque | No | Man page |

### Web

| Extension(s) | Base Media URN | Structure | Inspection | Notes |
|--------------|----------------|-----------|------------|-------|
| `.html`, `.htm` | `media:html;textable` | ScalarOpaque | No | HTML document |
| `.xhtml` | `media:xhtml;textable` | ScalarOpaque | No | XHTML |
| `.css` | `media:css;textable` | ScalarOpaque | No | Stylesheet |
| `.scss`, `.sass` | `media:scss;textable` | ScalarOpaque | No | Sass |
| `.less` | `media:less;textable` | ScalarOpaque | No | Less CSS |

### Data Interchange (Requires Content Inspection)

| Extension(s) | Base Media URN | Possible Structures | Inspection Rules |
|--------------|----------------|---------------------|------------------|
| `.json` | `media:json;textable` | All four | See JSON detection |
| `.ndjson`, `.jsonl` | `media:ndjson;textable` | ListOpaque or ListRecord | See NDJSON detection |
| `.csv` | `media:csv;textable` | ListOpaque or ListRecord | See CSV detection |
| `.tsv` | `media:tsv;textable` | ListOpaque or ListRecord | See TSV detection |
| `.psv` | `media:psv;textable` | ListOpaque or ListRecord | Pipe-separated |

### Configuration (Always Record)

| Extension(s) | Base Media URN | Structure | Inspection | Notes |
|--------------|----------------|-----------|------------|-------|
| `.yaml`, `.yml` | `media:yaml;textable` | Inspect | Yes | See YAML detection |
| `.toml` | `media:toml;record;textable` | ScalarRecord | No | Always record |
| `.ini` | `media:ini;record;textable` | ScalarRecord | No | Always record |
| `.cfg`, `.conf` | `media:conf;record;textable` | ScalarRecord | No | Config file |
| `.properties` | `media:properties;record;textable` | ScalarRecord | No | Java properties |
| `.env` | `media:env;record;textable` | ScalarRecord | No | Environment vars |
| `.plist` | `media:plist;record` | ScalarRecord | No | Apple plist (binary or XML) |
| `.xml` | `media:xml;textable` | Inspect | Yes | See XML detection |

### Source Code (All Textable, Opaque)

| Extension(s) | Base Media URN | Structure | Notes |
|--------------|----------------|-----------|-------|
| `.rs` | `media:rust;textable;code` | ScalarOpaque | Rust |
| `.py` | `media:python;textable;code` | ScalarOpaque | Python |
| `.pyw` | `media:python;textable;code` | ScalarOpaque | Python (Windows) |
| `.pyi` | `media:python;textable;code` | ScalarOpaque | Python stubs |
| `.js` | `media:javascript;textable;code` | ScalarOpaque | JavaScript |
| `.mjs` | `media:javascript;textable;code` | ScalarOpaque | ES Module |
| `.cjs` | `media:javascript;textable;code` | ScalarOpaque | CommonJS |
| `.ts` | `media:typescript;textable;code` | ScalarOpaque | TypeScript |
| `.tsx` | `media:tsx;textable;code` | ScalarOpaque | TypeScript JSX |
| `.jsx` | `media:jsx;textable;code` | ScalarOpaque | JavaScript JSX |
| `.go` | `media:go;textable;code` | ScalarOpaque | Go |
| `.java` | `media:java;textable;code` | ScalarOpaque | Java |
| `.kt`, `.kts` | `media:kotlin;textable;code` | ScalarOpaque | Kotlin |
| `.scala` | `media:scala;textable;code` | ScalarOpaque | Scala |
| `.c` | `media:c;textable;code` | ScalarOpaque | C |
| `.h` | `media:c-header;textable;code` | ScalarOpaque | C header |
| `.cpp`, `.cc`, `.cxx` | `media:cpp;textable;code` | ScalarOpaque | C++ |
| `.hpp`, `.hh`, `.hxx` | `media:cpp-header;textable;code` | ScalarOpaque | C++ header |
| `.m` | `media:objc;textable;code` | ScalarOpaque | Objective-C |
| `.mm` | `media:objcpp;textable;code` | ScalarOpaque | Objective-C++ |
| `.swift` | `media:swift;textable;code` | ScalarOpaque | Swift |
| `.rb` | `media:ruby;textable;code` | ScalarOpaque | Ruby |
| `.php` | `media:php;textable;code` | ScalarOpaque | PHP |
| `.pl`, `.pm` | `media:perl;textable;code` | ScalarOpaque | Perl |
| `.lua` | `media:lua;textable;code` | ScalarOpaque | Lua |
| `.sh`, `.bash` | `media:shell;textable;code` | ScalarOpaque | Shell script |
| `.zsh` | `media:zsh;textable;code` | ScalarOpaque | Zsh script |
| `.fish` | `media:fish;textable;code` | ScalarOpaque | Fish script |
| `.ps1` | `media:powershell;textable;code` | ScalarOpaque | PowerShell |
| `.bat`, `.cmd` | `media:batch;textable;code` | ScalarOpaque | Windows batch |
| `.sql` | `media:sql;textable;code` | ScalarOpaque | SQL |
| `.r`, `.R` | `media:r;textable;code` | ScalarOpaque | R |
| `.jl` | `media:julia;textable;code` | ScalarOpaque | Julia |
| `.ex`, `.exs` | `media:elixir;textable;code` | ScalarOpaque | Elixir |
| `.erl`, `.hrl` | `media:erlang;textable;code` | ScalarOpaque | Erlang |
| `.hs` | `media:haskell;textable;code` | ScalarOpaque | Haskell |
| `.ml`, `.mli` | `media:ocaml;textable;code` | ScalarOpaque | OCaml |
| `.fs`, `.fsi`, `.fsx` | `media:fsharp;textable;code` | ScalarOpaque | F# |
| `.clj`, `.cljs`, `.cljc` | `media:clojure;textable;code` | ScalarOpaque | Clojure |
| `.lisp`, `.cl` | `media:lisp;textable;code` | ScalarOpaque | Common Lisp |
| `.scm`, `.ss` | `media:scheme;textable;code` | ScalarOpaque | Scheme |
| `.rkt` | `media:racket;textable;code` | ScalarOpaque | Racket |
| `.nim` | `media:nim;textable;code` | ScalarOpaque | Nim |
| `.zig` | `media:zig;textable;code` | ScalarOpaque | Zig |
| `.v` | `media:v;textable;code` | ScalarOpaque | V or Verilog |
| `.d` | `media:d;textable;code` | ScalarOpaque | D |
| `.dart` | `media:dart;textable;code` | ScalarOpaque | Dart |
| `.cr` | `media:crystal;textable;code` | ScalarOpaque | Crystal |
| `.groovy` | `media:groovy;textable;code` | ScalarOpaque | Groovy |
| `.vb`, `.vbs` | `media:vb;textable;code` | ScalarOpaque | Visual Basic |
| `.cs` | `media:csharp;textable;code` | ScalarOpaque | C# |
| `.vue` | `media:vue;textable;code` | ScalarOpaque | Vue SFC |
| `.svelte` | `media:svelte;textable;code` | ScalarOpaque | Svelte |
| `.astro` | `media:astro;textable;code` | ScalarOpaque | Astro |
| `.wasm` | `media:wasm` | ScalarOpaque | WebAssembly (binary) |
| `.wat` | `media:wat;textable;code` | ScalarOpaque | WebAssembly text |

### Build & Config Files

| Extension(s) | Base Media URN | Structure | Notes |
|--------------|----------------|-----------|-------|
| `Makefile`, `makefile`, `GNUmakefile` | `media:makefile;textable` | ScalarOpaque | Make |
| `CMakeLists.txt` | `media:cmake;textable` | ScalarOpaque | CMake |
| `.cmake` | `media:cmake;textable` | ScalarOpaque | CMake module |
| `Dockerfile` | `media:dockerfile;textable` | ScalarOpaque | Docker |
| `.dockerignore` | `media:dockerignore;textable` | ListOpaque | Docker ignore |
| `.gitignore` | `media:gitignore;textable` | ListOpaque | Git ignore |
| `.gitattributes` | `media:gitattributes;textable` | ListOpaque | Git attributes |
| `.editorconfig` | `media:editorconfig;textable` | ScalarRecord | Editor config |
| `.prettierrc` | `media:prettierrc;textable` | ScalarRecord | Prettier |
| `.eslintrc` | `media:eslintrc;textable` | ScalarRecord | ESLint |
| `package.json` | `media:json;record;textable` | ScalarRecord | NPM manifest |
| `Cargo.toml` | `media:toml;record;textable` | ScalarRecord | Rust manifest |
| `go.mod` | `media:gomod;textable` | ScalarOpaque | Go module |
| `go.sum` | `media:gosum;textable` | ListOpaque | Go checksums |
| `Gemfile` | `media:ruby;textable` | ScalarOpaque | Ruby bundler |
| `requirements.txt` | `media:requirements;textable` | ListOpaque | Python deps |
| `pyproject.toml` | `media:toml;record;textable` | ScalarRecord | Python project |
| `setup.py` | `media:python;textable;code` | ScalarOpaque | Python setup |
| `Package.swift` | `media:swift;textable;code` | ScalarOpaque | Swift package |
| `build.gradle`, `build.gradle.kts` | `media:gradle;textable` | ScalarOpaque | Gradle |
| `pom.xml` | `media:xml;textable` | ScalarRecord | Maven POM |

### Archives (Binary, Opaque)

| Extension(s) | Base Media URN | Structure | Notes |
|--------------|----------------|-----------|-------|
| `.zip` | `media:zip;archive` | ScalarOpaque | ZIP archive |
| `.tar` | `media:tar;archive` | ScalarOpaque | Tape archive |
| `.gz`, `.gzip` | `media:gzip;archive` | ScalarOpaque | Gzip |
| `.bz2` | `media:bzip2;archive` | ScalarOpaque | Bzip2 |
| `.xz` | `media:xz;archive` | ScalarOpaque | XZ |
| `.lz`, `.lzma` | `media:lzma;archive` | ScalarOpaque | LZMA |
| `.zst`, `.zstd` | `media:zstd;archive` | ScalarOpaque | Zstandard |
| `.7z` | `media:7z;archive` | ScalarOpaque | 7-Zip |
| `.rar` | `media:rar;archive` | ScalarOpaque | RAR |
| `.tar.gz`, `.tgz` | `media:targz;archive` | ScalarOpaque | Tarball gzip |
| `.tar.bz2`, `.tbz2` | `media:tarbz2;archive` | ScalarOpaque | Tarball bzip2 |
| `.tar.xz`, `.txz` | `media:tarxz;archive` | ScalarOpaque | Tarball xz |
| `.jar` | `media:jar;archive` | ScalarOpaque | Java archive |
| `.war` | `media:war;archive` | ScalarOpaque | Web archive |
| `.ear` | `media:ear;archive` | ScalarOpaque | Enterprise archive |
| `.apk` | `media:apk;archive` | ScalarOpaque | Android package |
| `.ipa` | `media:ipa;archive` | ScalarOpaque | iOS package |
| `.dmg` | `media:dmg;archive` | ScalarOpaque | macOS disk image |
| `.iso` | `media:iso;archive` | ScalarOpaque | ISO image |
| `.deb` | `media:deb;archive` | ScalarOpaque | Debian package |
| `.rpm` | `media:rpm;archive` | ScalarOpaque | RPM package |
| `.pkg` | `media:pkg;archive` | ScalarOpaque | macOS installer |

### Fonts

| Extension(s) | Base Media URN | Structure | Notes |
|--------------|----------------|-----------|-------|
| `.ttf` | `media:ttf;font` | ScalarOpaque | TrueType |
| `.otf` | `media:otf;font` | ScalarOpaque | OpenType |
| `.woff` | `media:woff;font` | ScalarOpaque | Web Open Font |
| `.woff2` | `media:woff2;font` | ScalarOpaque | WOFF2 |
| `.eot` | `media:eot;font` | ScalarOpaque | Embedded OpenType |

### 3D & CAD

| Extension(s) | Base Media URN | Structure | Notes |
|--------------|----------------|-----------|-------|
| `.obj` | `media:obj;model` | ScalarOpaque | Wavefront OBJ |
| `.stl` | `media:stl;model` | ScalarOpaque | Stereolithography |
| `.fbx` | `media:fbx;model` | ScalarOpaque | Filmbox |
| `.gltf` | `media:gltf;model;textable` | ScalarRecord | GL Transmission (JSON) |
| `.glb` | `media:glb;model` | ScalarOpaque | GL Binary |
| `.dae` | `media:collada;model;textable` | ScalarOpaque | COLLADA |
| `.blend` | `media:blend;model` | ScalarOpaque | Blender |
| `.3ds` | `media:3ds;model` | ScalarOpaque | 3D Studio |
| `.ply` | `media:ply;model` | ScalarOpaque | Polygon File |
| `.step`, `.stp` | `media:step;cad` | ScalarOpaque | STEP CAD |
| `.iges`, `.igs` | `media:iges;cad` | ScalarOpaque | IGES CAD |
| `.dwg` | `media:dwg;cad` | ScalarOpaque | AutoCAD |
| `.dxf` | `media:dxf;cad;textable` | ScalarOpaque | Drawing Exchange |

### Database & Binary Data

| Extension(s) | Base Media URN | Structure | Notes |
|--------------|----------------|-----------|-------|
| `.sqlite`, `.sqlite3`, `.db` | `media:sqlite` | ScalarOpaque | SQLite database |
| `.mdb`, `.accdb` | `media:access` | ScalarOpaque | MS Access |
| `.parquet` | `media:parquet` | ListRecord | Columnar data |
| `.arrow`, `.feather` | `media:arrow` | ListRecord | Arrow IPC |
| `.avro` | `media:avro` | ListRecord | Avro serialization |
| `.orc` | `media:orc` | ListRecord | ORC columnar |
| `.protobuf`, `.pb` | `media:protobuf` | ScalarOpaque | Protocol Buffers |
| `.msgpack` | `media:msgpack` | ScalarOpaque | MessagePack |
| `.cbor` | `media:cbor` | ScalarOpaque | CBOR |
| `.bson` | `media:bson` | ScalarOpaque | BSON |

### ML & Scientific

| Extension(s) | Base Media URN | Structure | Notes |
|--------------|----------------|-----------|-------|
| `.gguf` | `media:gguf;model` | ScalarOpaque | GGUF model |
| `.ggml` | `media:ggml;model` | ScalarOpaque | GGML model |
| `.safetensors` | `media:safetensors;model` | ScalarOpaque | SafeTensors |
| `.pt`, `.pth` | `media:pytorch;model` | ScalarOpaque | PyTorch |
| `.onnx` | `media:onnx;model` | ScalarOpaque | ONNX |
| `.mlmodel` | `media:coreml;model` | ScalarOpaque | Core ML |
| `.mlpackage` | `media:coreml;model` | ScalarOpaque | Core ML package |
| `.h5`, `.hdf5` | `media:hdf5` | ScalarOpaque | HDF5 |
| `.npy` | `media:numpy` | ScalarOpaque | NumPy array |
| `.npz` | `media:numpy;archive` | ScalarOpaque | NumPy archive |
| `.mat` | `media:matlab` | ScalarOpaque | MATLAB |
| `.nc`, `.nc4` | `media:netcdf` | ScalarOpaque | NetCDF |
| `.fits` | `media:fits` | ScalarOpaque | FITS astronomical |
| `.ipynb` | `media:jupyter;record;textable` | ScalarRecord | Jupyter notebook |

### Certificates & Security

| Extension(s) | Base Media URN | Structure | Notes |
|--------------|----------------|-----------|-------|
| `.pem` | `media:pem;textable` | ScalarOpaque | PEM certificate |
| `.crt`, `.cer` | `media:cert` | ScalarOpaque | Certificate |
| `.key` | `media:key` | ScalarOpaque | Private key |
| `.csr` | `media:csr` | ScalarOpaque | Certificate request |
| `.p12`, `.pfx` | `media:pkcs12` | ScalarOpaque | PKCS#12 |
| `.p7b`, `.p7c` | `media:pkcs7` | ScalarOpaque | PKCS#7 |
| `.gpg`, `.pgp` | `media:gpg` | ScalarOpaque | GPG encrypted |
| `.asc` | `media:gpg;textable` | ScalarOpaque | ASCII armored GPG |
| `.sig` | `media:sig` | ScalarOpaque | Signature file |
| `.pub` | `media:pubkey;textable` | ScalarOpaque | Public key |

### Geospatial

| Extension(s) | Base Media URN | Structure | Notes |
|--------------|----------------|-----------|-------|
| `.geojson` | `media:geojson;record;textable` | ScalarRecord | GeoJSON |
| `.kml` | `media:kml;textable` | ScalarOpaque | Keyhole Markup |
| `.kmz` | `media:kmz` | ScalarOpaque | Compressed KML |
| `.gpx` | `media:gpx;textable` | ScalarOpaque | GPS Exchange |
| `.shp` | `media:shapefile` | ScalarOpaque | Shapefile |
| `.topojson` | `media:topojson;record;textable` | ScalarRecord | TopoJSON |

### Subtitles & Captions

| Extension(s) | Base Media URN | Structure | Notes |
|--------------|----------------|-----------|-------|
| `.srt` | `media:srt;textable` | ListRecord | SubRip |
| `.vtt` | `media:vtt;textable` | ListRecord | WebVTT |
| `.ass`, `.ssa` | `media:ass;textable` | ListRecord | SubStation Alpha |
| `.sub` | `media:sub;textable` | ListRecord | MicroDVD |

### Graph & Diagram

| Extension(s) | Base Media URN | Structure | Notes |
|--------------|----------------|-----------|-------|
| `.dot`, `.gv` | `media:dot;textable` | ScalarOpaque | Graphviz DOT |
| `.mermaid` | `media:mermaid;textable` | ScalarOpaque | Mermaid diagram |
| `.puml`, `.plantuml` | `media:plantuml;textable` | ScalarOpaque | PlantUML |
| `.drawio` | `media:drawio;textable` | ScalarOpaque | Draw.io (XML) |

### Email & Calendar

| Extension(s) | Base Media URN | Structure | Notes |
|--------------|----------------|-----------|-------|
| `.eml` | `media:eml;textable` | ScalarRecord | Email message |
| `.msg` | `media:msg` | ScalarOpaque | Outlook message |
| `.mbox` | `media:mbox;textable` | ListRecord | Mail archive |
| `.ics` | `media:ics;textable` | ListRecord | iCalendar |
| `.vcf` | `media:vcf;textable` | ListRecord | vCard |

---

## Content Inspection Rules

For files requiring content inspection, read the first N bytes (64KB default) and apply these rules:

### JSON Detection (`.json` or unknown extension)

```
Read first non-whitespace character:
  '[' → Parse first element:
    - If object '{}': → list;record
    - If primitive: → list (opaque)
    - If empty array: → list (opaque)
  '{' → record (no list)
  primitive (string/number/bool/null) → opaque (no list, no record)
  Parse error → opaque
```

| Content Pattern | Media URN | Structure |
|-----------------|-----------|-----------|
| `{"key": "value"}` | `media:json;record;textable` | ScalarRecord |
| `[{"a":1}, {"b":2}]` | `media:json;list;record;textable` | ListRecord |
| `["a", "b", "c"]` | `media:json;list;textable` | ListOpaque |
| `[1, 2, 3]` | `media:json;list;textable` | ListOpaque |
| `"just a string"` | `media:json;textable` | ScalarOpaque |
| `42` | `media:json;textable` | ScalarOpaque |
| `null` | `media:json;textable` | ScalarOpaque |
| `true` | `media:json;textable` | ScalarOpaque |
| `[]` | `media:json;list;textable` | ListOpaque |
| `[{}]` | `media:json;list;record;textable` | ListRecord |

### NDJSON Detection (`.ndjson`, `.jsonl`)

```
Read first N non-empty lines (10 max):
  - Each line must parse as valid JSON
  - If ANY line is object '{}': → list;record
  - Otherwise: → list (opaque)

Note: Always has `list` marker (multiple JSON values)
```

| Content Pattern | Media URN | Structure |
|-----------------|-----------|-----------|
| `{"a":1}\n{"b":2}` | `media:ndjson;list;record;textable` | ListRecord |
| `"a"\n"b"\n"c"` | `media:ndjson;list;textable` | ListOpaque |
| `1\n2\n3` | `media:ndjson;list;textable` | ListOpaque |
| `{"x":1}` (single line .ndjson) | `media:ndjson;list;record;textable` | ListRecord |

### CSV Detection (`.csv`)

```
Parse first row as header:
  - 1 column: → list (opaque per row)
  - >1 columns: → list;record (each row is record)

Note: Always has `list` marker
```

| Content Pattern | Media URN | Structure |
|-----------------|-----------|-----------|
| `name,age\nAlice,30` | `media:csv;list;record;textable` | ListRecord |
| `value\n1\n2\n3` | `media:csv;list;textable` | ListOpaque |
| Empty or header-only | `media:csv;list;textable` | ListOpaque |

### TSV Detection (`.tsv`)

Same rules as CSV but tab-delimited.

### YAML Detection (`.yaml`, `.yml`)

```
Parse document:
  Mapping (object) at root → record
  Sequence at root:
    - Inspect first element
    - If mapping: → list;record
    - Otherwise: → list (opaque)
  Scalar at root → opaque

Multi-document (---):
  - Multiple documents → list
  - Apply same element rules
```

| Content Pattern | Media URN | Structure |
|-----------------|-----------|-----------|
| `key: value` | `media:yaml;record;textable` | ScalarRecord |
| `- item1\n- item2` | `media:yaml;list;textable` | ListOpaque |
| `- name: a\n- name: b` | `media:yaml;list;record;textable` | ListRecord |
| `just text` | `media:yaml;textable` | ScalarOpaque |
| `---\na: 1\n---\nb: 2` | `media:yaml;list;record;textable` | ListRecord |

### XML Detection (`.xml`)

```
Parse document:
  - Root element with children that have consistent structure → record
  - Root element is container of repeated elements → list;record
  - Otherwise → opaque

Heuristics for list detection:
  - Root has >1 child with same tag name → list
  - Root has single child with repeated grandchildren → list
```

| Content Pattern | Media URN | Structure |
|-----------------|-----------|-----------|
| `<root><item/><item/></root>` | `media:xml;list;record;textable` | ListRecord |
| `<config><key>val</key></config>` | `media:xml;record;textable` | ScalarRecord |
| `<note>text</note>` | `media:xml;textable` | ScalarOpaque |

### Plain Text Heuristics (`.txt`, no extension)

For ambiguous text files:

```
1. Try JSON parse of entire content → if succeeds, treat as JSON
2. Try parsing each line as JSON → if all succeed, treat as NDJSON
3. Check for CSV pattern (consistent comma/tab count) → treat as CSV/TSV
4. Count newlines:
   - 0 or 1 newline → scalar opaque
   - Multiple newlines → list opaque (lines as items)
```

---

## Aggregate Cardinality

The `ResolvedInputSet.cardinality` reflects the **input set as a whole**:

| Scenario | Cardinality | Notes |
|----------|-------------|-------|
| 1 file, scalar content | `Single` | |
| 1 file, list content | `Sequence` | List from within file |
| N files (N>1), any content | `Sequence` | N items |
| 1 directory → 1 file | `Single` | |
| 1 directory → N files | `Sequence` | |
| Mixed inputs → any files | `Sequence` | Multiple sources |
| 0 files (empty) | Error | NotFound |

---

## API

### Rust API (`capdag::input_resolver`)

```rust
use std::path::PathBuf;

/// Resolve a single input item
pub fn resolve_input(item: InputItem) -> Result<ResolvedInputSet, InputResolverError>;

/// Resolve multiple input items
pub fn resolve_inputs(items: Vec<InputItem>) -> Result<ResolvedInputSet, InputResolverError>;

/// Convenience: resolve from string paths (auto-detect file/dir/glob)
pub fn resolve_paths(paths: &[&str]) -> Result<ResolvedInputSet, InputResolverError>;

/// Detect content structure for a single file (exposed for testing)
pub fn detect_content_structure(path: &Path) -> Result<(String, ContentStructure), InputResolverError>;
```

### Swift/ObjC API (`capdag-objc`)

```swift
// CSInputResolver.h / CSInputResolver.swift

/// Resolve mixed inputs to a flat file list with media detection
public class CSInputResolver {
    /// Resolve paths (auto-detect file/dir/glob)
    public static func resolve(paths: [String]) throws -> CSResolvedInputSet

    /// Resolve a single directory recursively
    public static func resolveDirectory(_ path: String) throws -> CSResolvedInputSet

    /// Detect content structure for a single file
    public static func detectContentStructure(path: String) throws -> (mediaUrn: String, structure: CSContentStructure)
}
```

---

## Error Handling

```rust
pub enum InputResolverError {
    /// Path does not exist
    NotFound(PathBuf),
    /// Permission denied
    PermissionDenied(PathBuf),
    /// Invalid glob pattern
    InvalidGlob { pattern: String, reason: String },
    /// IO error during resolution
    IoError { path: PathBuf, error: std::io::Error },
    /// Content inspection failed
    InspectionFailed { path: PathBuf, reason: String },
    /// Empty input (no paths provided)
    EmptyInput,
    /// All paths resolved to zero files
    NoFilesResolved,
}
```

No fallbacks. All errors are explicit and must be handled.

---

## Test Case Index (TEST1000-TEST1099)

### Path Resolution (TEST1000-TEST1019)

| ID | Test | Input | Expected |
|----|------|-------|----------|
| TEST1000 | Single existing file | `/path/file.pdf` | 1 file |
| TEST1001 | Single non-existent file | `/path/missing.pdf` | Error::NotFound |
| TEST1002 | Empty directory | `/path/empty/` | Error::NoFilesResolved |
| TEST1003 | Directory with files | `/path/docs/` | N files (recursive) |
| TEST1004 | Directory with subdirs | `/path/nested/` | All files recursive |
| TEST1005 | Glob matching files | `/path/*.pdf` | Matching PDFs |
| TEST1006 | Glob matching nothing | `/path/*.xyz` | Error::NoFilesResolved |
| TEST1007 | Recursive glob | `/path/**/*.json` | All nested JSONs |
| TEST1008 | Mixed file + dir | `[file.pdf, dir/]` | Combined |
| TEST1009 | Mixed file + glob | `[file.pdf, *.txt]` | Combined |
| TEST1010 | Duplicate paths | `[file.pdf, file.pdf]` | Deduplicated to 1 |
| TEST1011 | Invalid glob syntax | `[unclosed` | Error::InvalidGlob |
| TEST1012 | Permission denied | `/root/secret` | Error::PermissionDenied |
| TEST1013 | Empty input array | `[]` | Error::EmptyInput |
| TEST1014 | Symlink to file | `link -> file.pdf` | Resolved file |
| TEST1015 | Symlink cycle | `a -> b -> a` | Error (cycle) |
| TEST1016 | Path with spaces | `/path/my file.pdf` | Works |
| TEST1017 | Path with unicode | `/path/文档.pdf` | Works |
| TEST1018 | Relative path | `./file.pdf` | Canonicalized |
| TEST1019 | Home expansion | `~/docs/file.pdf` | Expanded |

### OS File Filtering (TEST1020-TEST1029)

| ID | Test | Files in Dir | Expected Excluded |
|----|------|--------------|-------------------|
| TEST1020 | macOS .DS_Store | `.DS_Store, file.txt` | `.DS_Store` |
| TEST1021 | Windows Thumbs.db | `Thumbs.db, file.txt` | `Thumbs.db` |
| TEST1022 | macOS resource fork | `._file, file.txt` | `._file` |
| TEST1023 | Office lock file | `~$doc.docx, doc.docx` | `~$doc.docx` |
| TEST1024 | .git directory | `.git/*, src/` | Entire `.git/` |
| TEST1025 | __MACOSX archive | `__MACOSX/*, real/` | `__MACOSX/` |
| TEST1026 | Temp files | `file.tmp, file.txt` | `file.tmp` |
| TEST1027 | .localized | `.localized, file.txt` | `.localized` |
| TEST1028 | desktop.ini | `desktop.ini, file.txt` | `desktop.ini` |
| TEST1029 | Mixed OS artifacts | All above | All excluded |

### JSON Detection (TEST1030-TEST1044)

| ID | Test | Content | Expected URN | Structure |
|----|------|---------|--------------|-----------|
| TEST1030 | Empty object | `{}` | `media:json;record;textable` | ScalarRecord |
| TEST1031 | Simple object | `{"a":1}` | `media:json;record;textable` | ScalarRecord |
| TEST1032 | Nested object | `{"a":{"b":1}}` | `media:json;record;textable` | ScalarRecord |
| TEST1033 | Empty array | `[]` | `media:json;list;textable` | ListOpaque |
| TEST1034 | Array of primitives | `[1,2,3]` | `media:json;list;textable` | ListOpaque |
| TEST1035 | Array of strings | `["a","b"]` | `media:json;list;textable` | ListOpaque |
| TEST1036 | Array of objects | `[{"a":1}]` | `media:json;list;record;textable` | ListRecord |
| TEST1037 | Mixed array | `[1,{"a":1}]` | `media:json;list;record;textable` | ListRecord |
| TEST1038 | String primitive | `"hello"` | `media:json;textable` | ScalarOpaque |
| TEST1039 | Number primitive | `42` | `media:json;textable` | ScalarOpaque |
| TEST1040 | Boolean true | `true` | `media:json;textable` | ScalarOpaque |
| TEST1041 | Boolean false | `false` | `media:json;textable` | ScalarOpaque |
| TEST1042 | Null | `null` | `media:json;textable` | ScalarOpaque |
| TEST1043 | With whitespace | `  { "a" : 1 }  ` | `media:json;record;textable` | ScalarRecord |
| TEST1044 | Invalid JSON | `{invalid` | `media:json;textable` | ScalarOpaque |

### NDJSON Detection (TEST1045-TEST1054)

| ID | Test | Content | Expected URN | Structure |
|----|------|---------|--------------|-----------|
| TEST1045 | Objects only | `{"a":1}\n{"b":2}` | `media:ndjson;list;record;textable` | ListRecord |
| TEST1046 | Single object | `{"a":1}` | `media:ndjson;list;record;textable` | ListRecord |
| TEST1047 | Primitives only | `1\n2\n3` | `media:ndjson;list;textable` | ListOpaque |
| TEST1048 | Strings only | `"a"\n"b"` | `media:ndjson;list;textable` | ListOpaque |
| TEST1049 | Mixed with object | `1\n{"a":1}` | `media:ndjson;list;record;textable` | ListRecord |
| TEST1050 | Empty lines | `{"a":1}\n\n{"b":2}` | `media:ndjson;list;record;textable` | ListRecord |
| TEST1051 | Trailing newline | `{"a":1}\n` | `media:ndjson;list;record;textable` | ListRecord |
| TEST1052 | Arrays per line | `[1]\n[2]` | `media:ndjson;list;textable` | ListOpaque |
| TEST1053 | Nulls | `null\nnull` | `media:ndjson;list;textable` | ListOpaque |
| TEST1054 | Large file (10+ lines) | 100 objects | `media:ndjson;list;record;textable` | ListRecord |

### CSV Detection (TEST1055-TEST1064)

| ID | Test | Content | Expected URN | Structure |
|----|------|---------|--------------|-----------|
| TEST1055 | Multi-column with header | `a,b\n1,2` | `media:csv;list;record;textable` | ListRecord |
| TEST1056 | Single column | `value\n1\n2` | `media:csv;list;textable` | ListOpaque |
| TEST1057 | Header only | `a,b,c` | `media:csv;list;record;textable` | ListRecord |
| TEST1058 | Empty file | `` | `media:csv;list;textable` | ListOpaque |
| TEST1059 | Quoted fields | `"a,b",c\n"1,2",3` | `media:csv;list;record;textable` | ListRecord |
| TEST1060 | Many columns | 10 columns | `media:csv;list;record;textable` | ListRecord |
| TEST1061 | TSV multi-column | `a\tb\n1\t2` | `media:tsv;list;record;textable` | ListRecord |
| TEST1062 | TSV single column | `val\n1\n2` | `media:tsv;list;textable` | ListOpaque |
| TEST1063 | Escaped quotes | `"a""b"` | `media:csv;list;textable` | ListOpaque |
| TEST1064 | Newline in quoted | `"a\nb"` | `media:csv;list;textable` | ListOpaque |

### YAML Detection (TEST1065-TEST1074)

| ID | Test | Content | Expected URN | Structure |
|----|------|---------|--------------|-----------|
| TEST1065 | Simple mapping | `a: 1` | `media:yaml;record;textable` | ScalarRecord |
| TEST1066 | Nested mapping | `a:\n  b: 1` | `media:yaml;record;textable` | ScalarRecord |
| TEST1067 | Sequence of scalars | `- a\n- b` | `media:yaml;list;textable` | ListOpaque |
| TEST1068 | Sequence of mappings | `- a: 1\n- b: 2` | `media:yaml;list;record;textable` | ListRecord |
| TEST1069 | Scalar only | `hello` | `media:yaml;textable` | ScalarOpaque |
| TEST1070 | Multi-doc mappings | `---\na: 1\n---\nb: 2` | `media:yaml;list;record;textable` | ListRecord |
| TEST1071 | Multi-doc scalars | `---\nhello\n---\nworld` | `media:yaml;list;textable` | ListOpaque |
| TEST1072 | Flow mapping | `{a: 1, b: 2}` | `media:yaml;record;textable` | ScalarRecord |
| TEST1073 | Flow sequence | `[1, 2, 3]` | `media:yaml;list;textable` | ListOpaque |
| TEST1074 | Complex nested | mapping with arrays | `media:yaml;record;textable` | ScalarRecord |

### XML Detection (TEST1075-TEST1079)

| ID | Test | Content | Expected URN | Structure |
|----|------|---------|--------------|-----------|
| TEST1075 | Repeated children | `<r><i/><i/></r>` | `media:xml;list;record;textable` | ListRecord |
| TEST1076 | Config structure | `<cfg><k>v</k></cfg>` | `media:xml;record;textable` | ScalarRecord |
| TEST1077 | Simple element | `<note>text</note>` | `media:xml;textable` | ScalarOpaque |
| TEST1078 | With attributes | `<r a="1"><i/></r>` | `media:xml;list;record;textable` | ListRecord |
| TEST1079 | Empty root | `<root/>` | `media:xml;textable` | ScalarOpaque |

### Extension Mapping (TEST1080-TEST1089)

| ID | Test | Extension | Expected URN | Structure |
|----|------|-----------|--------------|-----------|
| TEST1080 | PDF | `.pdf` | `media:pdf` | ScalarOpaque |
| TEST1081 | PNG | `.png` | `media:png;image` | ScalarOpaque |
| TEST1082 | MP3 | `.mp3` | `media:mp3;audio` | ScalarOpaque |
| TEST1083 | MP4 | `.mp4` | `media:mp4;video` | ScalarOpaque |
| TEST1084 | Rust code | `.rs` | `media:rust;textable;code` | ScalarOpaque |
| TEST1085 | Python code | `.py` | `media:python;textable;code` | ScalarOpaque |
| TEST1086 | Markdown | `.md` | `media:md;textable` | ScalarOpaque |
| TEST1087 | TOML | `.toml` | `media:toml;record;textable` | ScalarRecord |
| TEST1088 | Log file | `.log` | `media:log;textable` | ListOpaque |
| TEST1089 | Unknown ext | `.xyz` | `media:` | ScalarOpaque |

### Aggregate Cardinality (TEST1090-TEST1099)

| ID | Test | Input | Expected Cardinality |
|----|------|-------|---------------------|
| TEST1090 | 1 file scalar | Single PDF | Single |
| TEST1091 | 1 file list content | Single CSV | Sequence |
| TEST1092 | 2 files | Two PDFs | Sequence |
| TEST1093 | 1 dir → 1 file | Dir with 1 PDF | Single |
| TEST1094 | 1 dir → 3 files | Dir with 3 files | Sequence |
| TEST1095 | Glob → 1 file | `*.pdf` matches 1 | Single |
| TEST1096 | Glob → 5 files | `*.pdf` matches 5 | Sequence |
| TEST1097 | Mixed → 2 files | file + dir(1) | Sequence |
| TEST1098 | Common media | All PDFs | common=Some("pdf") |
| TEST1099 | Heterogeneous | PDF + PNG | common=None |

---

*Document created: 2024-03-01*
*Tests: TEST1000-TEST1099*
