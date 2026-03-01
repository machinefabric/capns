//! Source code adapters — Rust, Python, JavaScript, etc.
//!
//! All source code files are treated as scalar opaque (the file as a whole).

use std::path::Path;
use crate::input_resolver::adapter::{MediaAdapter, AdapterResult};

/// Rust source code adapter
pub struct RustAdapter;

impl MediaAdapter for RustAdapter {
    fn name(&self) -> &'static str { "rust" }

    fn extensions(&self) -> &'static [&'static str] {
        &["rs"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:rust;textable;code")
    }
}

/// Python source code adapter
pub struct PythonAdapter;

impl MediaAdapter for PythonAdapter {
    fn name(&self) -> &'static str { "python" }

    fn extensions(&self) -> &'static [&'static str] {
        &["py", "pyw", "pyi", "pyx", "pxd"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"#!/usr/bin/env python", 0),
            (b"#!/usr/bin/python", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:python;textable;code")
    }
}

/// JavaScript source code adapter
pub struct JavaScriptAdapter;

impl MediaAdapter for JavaScriptAdapter {
    fn name(&self) -> &'static str { "javascript" }

    fn extensions(&self) -> &'static [&'static str] {
        &["js", "mjs", "cjs", "jsx"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"#!/usr/bin/env node", 0),
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "jsx" => "media:jsx;textable;code",
            _ => "media:javascript;textable;code",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// TypeScript source code adapter
pub struct TypeScriptAdapter;

impl MediaAdapter for TypeScriptAdapter {
    fn name(&self) -> &'static str { "typescript" }

    fn extensions(&self) -> &'static [&'static str] {
        &["ts", "tsx", "mts", "cts"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "tsx" => "media:tsx;textable;code",
            _ => "media:typescript;textable;code",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Go source code adapter
pub struct GoAdapter;

impl MediaAdapter for GoAdapter {
    fn name(&self) -> &'static str { "go" }

    fn extensions(&self) -> &'static [&'static str] {
        &["go"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:go;textable;code")
    }
}

/// Java source code adapter
pub struct JavaAdapter;

impl MediaAdapter for JavaAdapter {
    fn name(&self) -> &'static str { "java" }

    fn extensions(&self) -> &'static [&'static str] {
        &["java"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:java;textable;code")
    }
}

/// Kotlin source code adapter
pub struct KotlinAdapter;

impl MediaAdapter for KotlinAdapter {
    fn name(&self) -> &'static str { "kotlin" }

    fn extensions(&self) -> &'static [&'static str] {
        &["kt", "kts"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:kotlin;textable;code")
    }
}

/// C source code adapter
pub struct CAdapter;

impl MediaAdapter for CAdapter {
    fn name(&self) -> &'static str { "c" }

    fn extensions(&self) -> &'static [&'static str] {
        &["c", "h"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "h" => "media:c-header;textable;code",
            _ => "media:c;textable;code",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// C++ source code adapter
pub struct CppAdapter;

impl MediaAdapter for CppAdapter {
    fn name(&self) -> &'static str { "cpp" }

    fn extensions(&self) -> &'static [&'static str] {
        &["cpp", "cc", "cxx", "c++", "hpp", "hh", "hxx", "h++", "ipp"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "hpp" | "hh" | "hxx" | "h++" => "media:cpp-header;textable;code",
            _ => "media:cpp;textable;code",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Swift source code adapter
pub struct SwiftAdapter;

impl MediaAdapter for SwiftAdapter {
    fn name(&self) -> &'static str { "swift" }

    fn extensions(&self) -> &'static [&'static str] {
        &["swift"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:swift;textable;code")
    }
}

/// Objective-C source code adapter
pub struct ObjCAdapter;

impl MediaAdapter for ObjCAdapter {
    fn name(&self) -> &'static str { "objc" }

    fn extensions(&self) -> &'static [&'static str] {
        &["m", "mm"]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "mm" => "media:objcpp;textable;code",
            _ => "media:objc;textable;code",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// Ruby source code adapter
pub struct RubyAdapter;

impl MediaAdapter for RubyAdapter {
    fn name(&self) -> &'static str { "ruby" }

    fn extensions(&self) -> &'static [&'static str] {
        &["rb", "rake", "gemspec"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"#!/usr/bin/env ruby", 0),
            (b"#!/usr/bin/ruby", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:ruby;textable;code")
    }
}

/// PHP source code adapter
pub struct PhpAdapter;

impl MediaAdapter for PhpAdapter {
    fn name(&self) -> &'static str { "php" }

    fn extensions(&self) -> &'static [&'static str] {
        &["php", "phtml", "php3", "php4", "php5", "phps"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"<?php", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:php;textable;code")
    }
}

/// Shell script adapter
pub struct ShellAdapter;

impl MediaAdapter for ShellAdapter {
    fn name(&self) -> &'static str { "shell" }

    fn extensions(&self) -> &'static [&'static str] {
        &["sh", "bash", "zsh", "fish", "ksh", "csh", "tcsh"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"#!/bin/sh", 0),
            (b"#!/bin/bash", 0),
            (b"#!/usr/bin/env bash", 0),
            (b"#!/bin/zsh", 0),
            (b"#!/usr/bin/env zsh", 0),
        ]
    }

    fn detect(&self, path: &Path, _content: &[u8]) -> AdapterResult {
        let ext = path.extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();

        let media = match ext.as_str() {
            "zsh" => "media:zsh;textable;code",
            "fish" => "media:fish;textable;code",
            _ => "media:shell;textable;code",
        };

        AdapterResult::scalar_opaque(media)
    }
}

/// SQL adapter
pub struct SqlAdapter;

impl MediaAdapter for SqlAdapter {
    fn name(&self) -> &'static str { "sql" }

    fn extensions(&self) -> &'static [&'static str] {
        &["sql", "ddl", "dml"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:sql;textable;code")
    }
}

/// Perl adapter
pub struct PerlAdapter;

impl MediaAdapter for PerlAdapter {
    fn name(&self) -> &'static str { "perl" }

    fn extensions(&self) -> &'static [&'static str] {
        &["pl", "pm", "pod", "t"]
    }

    fn magic_bytes(&self) -> &'static [(&'static [u8], usize)] {
        &[
            (b"#!/usr/bin/perl", 0),
            (b"#!/usr/bin/env perl", 0),
        ]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:perl;textable;code")
    }
}

/// Lua adapter
pub struct LuaAdapter;

impl MediaAdapter for LuaAdapter {
    fn name(&self) -> &'static str { "lua" }

    fn extensions(&self) -> &'static [&'static str] {
        &["lua"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:lua;textable;code")
    }
}

/// Scala adapter
pub struct ScalaAdapter;

impl MediaAdapter for ScalaAdapter {
    fn name(&self) -> &'static str { "scala" }

    fn extensions(&self) -> &'static [&'static str] {
        &["scala", "sc"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:scala;textable;code")
    }
}

/// R adapter
pub struct RLangAdapter;

impl MediaAdapter for RLangAdapter {
    fn name(&self) -> &'static str { "r" }

    fn extensions(&self) -> &'static [&'static str] {
        &["r", "R", "rmd", "Rmd"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:r;textable;code")
    }
}

/// Julia adapter
pub struct JuliaAdapter;

impl MediaAdapter for JuliaAdapter {
    fn name(&self) -> &'static str { "julia" }

    fn extensions(&self) -> &'static [&'static str] {
        &["jl"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:julia;textable;code")
    }
}

/// Haskell adapter
pub struct HaskellAdapter;

impl MediaAdapter for HaskellAdapter {
    fn name(&self) -> &'static str { "haskell" }

    fn extensions(&self) -> &'static [&'static str] {
        &["hs", "lhs"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:haskell;textable;code")
    }
}

/// Elixir adapter
pub struct ElixirAdapter;

impl MediaAdapter for ElixirAdapter {
    fn name(&self) -> &'static str { "elixir" }

    fn extensions(&self) -> &'static [&'static str] {
        &["ex", "exs"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:elixir;textable;code")
    }
}

/// Erlang adapter
pub struct ErlangAdapter;

impl MediaAdapter for ErlangAdapter {
    fn name(&self) -> &'static str { "erlang" }

    fn extensions(&self) -> &'static [&'static str] {
        &["erl", "hrl"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:erlang;textable;code")
    }
}

/// Clojure adapter
pub struct ClojureAdapter;

impl MediaAdapter for ClojureAdapter {
    fn name(&self) -> &'static str { "clojure" }

    fn extensions(&self) -> &'static [&'static str] {
        &["clj", "cljs", "cljc", "edn"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:clojure;textable;code")
    }
}

/// C# adapter
pub struct CSharpAdapter;

impl MediaAdapter for CSharpAdapter {
    fn name(&self) -> &'static str { "csharp" }

    fn extensions(&self) -> &'static [&'static str] {
        &["cs", "csx"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:csharp;textable;code")
    }
}

/// Visual Basic adapter
pub struct VbAdapter;

impl MediaAdapter for VbAdapter {
    fn name(&self) -> &'static str { "vb" }

    fn extensions(&self) -> &'static [&'static str] {
        &["vb", "vbs", "vba"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:vb;textable;code")
    }
}

/// Dart adapter
pub struct DartAdapter;

impl MediaAdapter for DartAdapter {
    fn name(&self) -> &'static str { "dart" }

    fn extensions(&self) -> &'static [&'static str] {
        &["dart"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:dart;textable;code")
    }
}

/// Vue SFC adapter
pub struct VueAdapter;

impl MediaAdapter for VueAdapter {
    fn name(&self) -> &'static str { "vue" }

    fn extensions(&self) -> &'static [&'static str] {
        &["vue"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:vue;textable;code")
    }
}

/// Svelte adapter
pub struct SvelteAdapter;

impl MediaAdapter for SvelteAdapter {
    fn name(&self) -> &'static str { "svelte" }

    fn extensions(&self) -> &'static [&'static str] {
        &["svelte"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:svelte;textable;code")
    }
}

/// Zig adapter
pub struct ZigAdapter;

impl MediaAdapter for ZigAdapter {
    fn name(&self) -> &'static str { "zig" }

    fn extensions(&self) -> &'static [&'static str] {
        &["zig"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:zig;textable;code")
    }
}

/// Nim adapter
pub struct NimAdapter;

impl MediaAdapter for NimAdapter {
    fn name(&self) -> &'static str { "nim" }

    fn extensions(&self) -> &'static [&'static str] {
        &["nim", "nims"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:nim;textable;code")
    }
}

/// PowerShell adapter
pub struct PowerShellAdapter;

impl MediaAdapter for PowerShellAdapter {
    fn name(&self) -> &'static str { "powershell" }

    fn extensions(&self) -> &'static [&'static str] {
        &["ps1", "psm1", "psd1"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:powershell;textable;code")
    }
}

/// Batch file adapter
pub struct BatchAdapter;

impl MediaAdapter for BatchAdapter {
    fn name(&self) -> &'static str { "batch" }

    fn extensions(&self) -> &'static [&'static str] {
        &["bat", "cmd"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:batch;textable;code")
    }
}

/// Makefile adapter
pub struct MakefileAdapter;

impl MediaAdapter for MakefileAdapter {
    fn name(&self) -> &'static str { "makefile" }

    fn extensions(&self) -> &'static [&'static str] {
        &["mk", "mak", "make"]
    }

    fn matches(&self, path: &Path, _content_prefix: &[u8]) -> crate::input_resolver::adapter::AdapterMatch {
        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        // Check for Makefile variants
        if filename == "Makefile" || filename == "makefile" ||
           filename == "GNUmakefile" || filename == "gnumakefile" ||
           filename.ends_with(".mk") || filename.ends_with(".mak") {
            return crate::input_resolver::adapter::AdapterMatch::ByExtension;
        }

        // Check extension
        if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            if self.extensions().contains(&ext.to_lowercase().as_str()) {
                return crate::input_resolver::adapter::AdapterMatch::ByExtension;
            }
        }

        crate::input_resolver::adapter::AdapterMatch::NoMatch
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:makefile;textable")
    }

    fn priority(&self) -> i32 {
        1 // Higher priority for filename matching
    }
}

/// Dockerfile adapter
pub struct DockerfileAdapter;

impl MediaAdapter for DockerfileAdapter {
    fn name(&self) -> &'static str { "dockerfile" }

    fn extensions(&self) -> &'static [&'static str] {
        &["dockerfile"]
    }

    fn matches(&self, path: &Path, _content_prefix: &[u8]) -> crate::input_resolver::adapter::AdapterMatch {
        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        if filename == "Dockerfile" || filename.starts_with("Dockerfile.") ||
           filename.ends_with(".dockerfile") {
            return crate::input_resolver::adapter::AdapterMatch::ByExtension;
        }

        crate::input_resolver::adapter::AdapterMatch::NoMatch
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:dockerfile;textable")
    }

    fn priority(&self) -> i32 {
        1
    }
}

/// CMake adapter
pub struct CMakeAdapter;

impl MediaAdapter for CMakeAdapter {
    fn name(&self) -> &'static str { "cmake" }

    fn extensions(&self) -> &'static [&'static str] {
        &["cmake"]
    }

    fn matches(&self, path: &Path, _content_prefix: &[u8]) -> crate::input_resolver::adapter::AdapterMatch {
        let filename = path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        if filename == "CMakeLists.txt" || filename.ends_with(".cmake") {
            return crate::input_resolver::adapter::AdapterMatch::ByExtension;
        }

        crate::input_resolver::adapter::AdapterMatch::NoMatch
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:cmake;textable")
    }

    fn priority(&self) -> i32 {
        1
    }
}

/// Graphviz DOT adapter
pub struct DotAdapter;

impl MediaAdapter for DotAdapter {
    fn name(&self) -> &'static str { "dot" }

    fn extensions(&self) -> &'static [&'static str] {
        &["dot", "gv"]
    }

    fn detect(&self, _path: &Path, _content: &[u8]) -> AdapterResult {
        AdapterResult::scalar_opaque("media:dot;textable")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use crate::input_resolver::ContentStructure;

    // TEST1084: Rust code extension mapping
    #[test]
    fn test1084_rust_extension() {
        let adapter = RustAdapter;
        let path = PathBuf::from("main.rs");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:rust;textable;code");
        assert_eq!(result.content_structure, ContentStructure::ScalarOpaque);
    }

    // TEST1085: Python code extension mapping
    #[test]
    fn test1085_python_extension() {
        let adapter = PythonAdapter;
        let path = PathBuf::from("script.py");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:python;textable;code");
        assert_eq!(result.content_structure, ContentStructure::ScalarOpaque);
    }

    #[test]
    fn test_typescript_tsx() {
        let adapter = TypeScriptAdapter;
        let path = PathBuf::from("component.tsx");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:tsx;textable;code");
    }

    #[test]
    fn test_cpp_header() {
        let adapter = CppAdapter;
        let path = PathBuf::from("header.hpp");

        let result = adapter.detect(&path, &[]);
        assert_eq!(result.media_urn, "media:cpp-header;textable;code");
    }

    #[test]
    fn test_makefile_detection() {
        let adapter = MakefileAdapter;
        let path = PathBuf::from("Makefile");

        assert!(adapter.matches(&path, &[]).matches());
    }

    #[test]
    fn test_dockerfile_detection() {
        let adapter = DockerfileAdapter;

        assert!(adapter.matches(&PathBuf::from("Dockerfile"), &[]).matches());
        assert!(adapter.matches(&PathBuf::from("Dockerfile.prod"), &[]).matches());
    }
}
