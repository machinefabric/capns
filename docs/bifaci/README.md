# Bifaci Documentation

Binary Frame Cap Invocation — the plugin communication protocol and runtime system.

## What is Bifaci

Bifaci is a binary protocol for communication between a host engine and plugin processes (called cartridges). The engine sends capability invocation requests to plugins over stdin/stdout using length-prefixed CBOR frames. Plugins respond with multiplexed streams of typed data, progress updates, and log messages through the same channel.

The protocol handles connection setup (handshake and identity verification), payload chunking, frame ordering across relay boundaries, and health monitoring. The runtime libraries on both sides — `PluginRuntime` for plugins, `PluginHostRuntime` for hosts — manage the frame lifecycle so that handler code only deals with typed input and output streams.

Bifaci has two implementations: Rust (`capdag/src/bifaci/`) and Swift (`capdag-objc/Sources/Bifaci/`).

## Reading Order

Start with the architecture overview, then follow the document numbering. Documents 20–29 cover the protocol layer (wire format, frame types, handshake, streaming). Documents 30–39 cover the runtime layer (plugin-side and host-side runtimes, peer calls, progress, relay topology). Documents 40–49 cover execution (orchestrator, DAG execution, progress mapping, planner). Documents 50–59 cover cartridge development (how to build plugins).

### Architecture & Concepts
- [20-ARCHITECTURE.md](20-ARCHITECTURE.md) — System topology, component roles, frame flow
- [21-FRAME-PROTOCOL.md](21-FRAME-PROTOCOL.md) — Wire format, frame types, CBOR encoding
- [22-HANDSHAKE.md](22-HANDSHAKE.md) — Connection setup, limit negotiation, identity verification
- [23-STREAMING.md](23-STREAMING.md) — Multiplexed streams, chunking, sequencing, reordering

### Plugin Runtime
- [30-PLUGIN-RUNTIME.md](30-PLUGIN-RUNTIME.md) — PluginRuntime, handler registration, CLI/plugin mode
- [31-INPUT-OUTPUT.md](31-INPUT-OUTPUT.md) — InputStream, OutputStream, stream lookup helpers
- [32-PEER-INVOCATION.md](32-PEER-INVOCATION.md) — PeerInvoker, PeerCall, PeerResponse, cross-plugin calls
- [33-PROGRESS-AND-LOGGING.md](33-PROGRESS-AND-LOGGING.md) — LOG frames, progress mapping, keepalive, ProgressSender

### Host & Relay
- [34-HOST-RUNTIME.md](34-HOST-RUNTIME.md) — PluginHostRuntime, plugin lifecycle, frame routing
- [35-RELAY-SWITCH.md](35-RELAY-SWITCH.md) — RelaySwitch, cap-aware routing, master health
- [36-RELAY-TOPOLOGY.md](36-RELAY-TOPOLOGY.md) — RelaySlave, RelayMaster, relay chains, XID assignment

### Execution Engine
- [40-ORCHESTRATOR.md](40-ORCHESTRATOR.md) — Machine notation parsing, DAG construction, cap resolution
- [41-EXECUTION.md](41-EXECUTION.md) — execute_dag, execute_fanin, edge grouping, topological sort
- [42-PROGRESS-MAPPING.md](42-PROGRESS-MAPPING.md) — ProgressMapper, deterministic subdivision, nested mapping
- [43-PLANNER.md](43-PLANNER.md) — LiveCapGraph, path finding, MachinePlan, plan building

### Cartridge Development
- [50-CARTRIDGE-ANATOMY.md](50-CARTRIDGE-ANATOMY.md) — Structure of a cartridge, manifest, cap definitions
- [51-HANDLER-PATTERNS.md](51-HANDLER-PATTERNS.md) — Op trait, request handling, argument extraction
- [52-MODEL-CARTRIDGES.md](52-MODEL-CARTRIDGES.md) — ML model loading, peer calls, download delegation, keepalive
- [53-CONTENT-CARTRIDGES.md](53-CONTENT-CARTRIDGES.md) — Document processing, multi-type registration, standard caps
- [54-RUST-VS-SWIFT.md](54-RUST-VS-SWIFT.md) — Implementation differences between capdag and capdag-objc

### Integration
- [60-TASK-INTEGRATION.md](60-TASK-INTEGRATION.md) — How cartridge execution ties into machfab tasks
- [61-ERROR-HANDLING.md](61-ERROR-HANDLING.md) — Error types, error propagation, failure modes

## Relationship to Existing Docs

The documents in the parent directory (`../00-OVERVIEW.md` through `../A0-FORMAL-FOUNDATIONS.md`) define the theoretical type system: URN semantics, predicates, dispatch matching, specificity scoring, and validation rules. This directory describes how those concepts are applied at runtime through the Bifaci protocol.

Specifically:
- Cap URN dispatch (see `../05-DISPATCH.md`) is how the RelaySwitch routes REQ frames to the correct plugin
- Specificity ranking (see `../06-RANKING.md`) is how the RelaySwitch chooses among multiple providers
- Validation rules (see `../10-VALIDATION-RULES.md`) are enforced on cap manifests during handshake
- Media URNs (see `../11-MEDIA-URNS.md`) are how InputStream/OutputStream streams are identified and matched
