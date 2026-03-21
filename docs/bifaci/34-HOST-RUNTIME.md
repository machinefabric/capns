# Host Runtime

The host-side manager that spawns plugin processes, routes frames, and monitors health.

## PluginHostRuntime

`PluginHostRuntime` sits between a RelaySlave and one or more plugin processes. It manages their full lifecycle — spawning, handshake, frame routing, health monitoring, and death handling.

```
RelaySlave ←→ PluginHostRuntime ←→ Plugin A (stdin/stdout)
                                ←→ Plugin B (stdin/stdout)
                                ←→ Plugin C (stdin/stdout)
```

The struct tracks each plugin as a `ManagedPlugin` with its process handle, I/O channels, manifest, capabilities, health status, and pending heartbeats.

```rust
pub struct PluginHostRuntime {
    plugins: Vec<ManagedPlugin>,
    cap_table: Vec<(String, usize)>,           // cap_urn → plugin index
    outgoing_rids: HashMap<MessageId, usize>,   // peer requests: RID → plugin index
    incoming_rxids: HashMap<(MessageId, MessageId), usize>, // relay requests: (XID,RID) → plugin
    outgoing_max_seq: HashMap<FlowKey, u64>,    // max seq per flow (for ERR frames)
    capabilities: Vec<u8>,                      // aggregate manifest JSON
    event_tx: mpsc::UnboundedSender<PluginEvent>,
    event_rx: Option<mpsc::UnboundedReceiver<PluginEvent>>,
}
```

Source: `capdag/src/bifaci/host_runtime.rs:278`.

## Plugin Registration

Two ways to connect plugins:

### register_plugin(path, known_caps)

Registers a plugin binary for on-demand spawning. The plugin is not started until a REQ arrives for one of its capabilities.

```rust
host.register_plugin(Path::new("/path/to/ggufcartridge"), &[
    "cap:in=...;out=...;op=generate;model=gguf".to_string(),
    "cap:in=...;out=...;op=describe;model=gguf".to_string(),
]);
```

The `known_caps` parameter enables provisional routing before the HELLO handshake reveals the plugin's actual manifest. After spawn + HELLO, the real caps from the manifest replace the provisional ones.

### attach_plugin(read, write)

Attaches a pre-connected plugin (already running). Performs HELLO handshake and identity verification immediately. Used for testing and pre-spawned processes.

```rust
let plugin_idx = host.attach_plugin(reader, writer).await?;
```

On HELLO failure, returns an error. The failure is permanent — the binary is considered broken and will not be retried.

Source: `host_runtime.rs` (`register_plugin`, line 324; `attach_plugin`, line 339).

## Plugin Spawning

When a REQ arrives for a cap handled by a registered-but-not-running plugin:

1. **Spawn**: The host starts the plugin binary as a child process with no arguments (triggering plugin CBOR mode).
2. **Handshake**: HELLO exchange and limit negotiation (see [22-HANDSHAKE.md](22-HANDSHAKE.md)).
3. **Identity verification**: REQ for CAP_IDENTITY with nonce echo.
4. **Reader/Writer tasks**: A reader task reads frames from the plugin's stdout; a writer task writes frames to the plugin's stdin.
5. **Update cap table**: The plugin's manifest replaces the provisional known_caps.
6. **Forward REQ**: The original REQ is forwarded to the now-running plugin.

If HELLO fails, the plugin is marked with `hello_failed = true` and will not be spawned again. The REQ that triggered the spawn receives an ERR frame.

Source: `host_runtime.rs`.

## Frame Routing: Engine → Plugin

Frames arriving from the relay (engine side) are routed to plugins:

**REQ**: The cap URN (key 10) is looked up in `cap_table` to find which plugin handles it. If the plugin is registered but not running, it is spawned on demand. The (XID, RID) pair is recorded in `incoming_rxids` mapping to the destination plugin index.

**Continuation frames** (STREAM_START, CHUNK, STREAM_END, END, ERR): Routed by (XID, RID) lookup in `incoming_rxids`. The frame is forwarded to the plugin that is handling the original request.

**All other frame types** from the relay are protocol errors — the relay should never send Hello, Heartbeat, or RelayNotify/RelayState to a host.

Source: `host_runtime.rs` (run loop).

## Frame Routing: Plugin → Engine

Frames from plugins are forwarded to the relay (engine side):

**HELLO**: Fatal error during run. HELLO is consumed during handshake and must never appear afterward.

**HEARTBEAT**: Handled locally. The host sends back a Heartbeat with the same ID. Never forwarded to the relay.

**REQ** (peer invoke): The plugin is calling another cap. The RID is recorded in `outgoing_rids` mapping to the source plugin. The frame is forwarded to the relay, which will assign an XID at the RelaySwitch.

**RelayNotify / RelayState**: Fatal error. Plugins must never send relay control frames.

**Everything else** (STREAM_START, CHUNK, STREAM_END, END, ERR, LOG): Forwarded to the relay. These carry the XID from the original request, which the relay uses for routing.

Source: `host_runtime.rs`.

## Self-Loop Routing

An edge case arises when a plugin peer-invokes a cap that another plugin on the same host handles. Both `incoming_rxids` and `outgoing_rids` contain entries for the same request IDs, creating ambiguity about which direction a continuation frame belongs to.

The host resolves this by frame type discrimination:

- **Request body frames** (STREAM_START, CHUNK, STREAM_END, END from the relay): These are the arguments of the incoming request. Routed via `incoming_rxids` to the handling plugin.
- **Peer response frames** (STREAM_START, CHUNK, STREAM_END, END from the handling plugin): These are the response to the peer call. After the incoming request's END is received, subsequent frames for the same (XID, RID) are peer response frames routed back to the calling plugin via `outgoing_rids`.

Source: `host_runtime.rs`.

## Heartbeat Health Monitoring

The host probes each running plugin every 30 seconds:

1. Send a HEARTBEAT frame with a fresh UUID.
2. Record the UUID and timestamp in `pending_heartbeats`.
3. When the plugin responds with a HEARTBEAT carrying the same UUID, remove the entry.
4. If a heartbeat goes unanswered for 10 seconds, mark the plugin as unhealthy.

Constants:
- `HEARTBEAT_INTERVAL`: 30 seconds between probes.
- `HEARTBEAT_TIMEOUT`: 10 seconds max wait for a response.

An unhealthy plugin is not automatically killed — it may recover. But its capabilities may be removed from the aggregate manifest until it responds again.

Source: `host_runtime.rs` (`HEARTBEAT_INTERVAL`, `HEARTBEAT_TIMEOUT`).

## Plugin Death Handling

Three scenarios:

**Ordered shutdown** (`ordered_shutdown = true`): The host sets this flag before killing a plugin process intentionally (e.g., on host shutdown). The death handler sees the flag and cleans up routing entries without sending ERR frames.

**Unexpected death with pending requests**: The plugin process exited while requests were in flight. The host sends an ERR frame for each pending request ID (both from `incoming_rxids` and `outgoing_rids`). The `outgoing_max_seq` map provides the correct seq number for each ERR frame (max_seen + 1). The `last_death_message` (including stderr output) is included in the error message.

**Idle death**: The plugin died while no requests were pending. Routing entries are cleaned up. The next REQ for this plugin's caps will trigger a respawn.

Source: `host_runtime.rs`.

## Capability Advertisement

When the set of available capabilities changes (plugin added, died, or respawned), the host rebuilds its aggregate manifest — a JSON array of all caps from all healthy plugins — and sends a RelayNotify frame to the relay.

The relay's master receives this and updates its manifest and limits. The RelaySwitch re-reads the master's capabilities and updates its routing table. This propagation means the engine's view of available capabilities stays in sync with the actual set of running plugins.

Source: `host_runtime.rs` (`rebuild_capabilities`).

## Swift Equivalent

The `PluginHost` class in `capdag-objc/Sources/Bifaci/PluginHost.swift` provides the same functionality for Swift-based hosts. The frame routing semantics are identical — the differences are in process management (using Foundation's `Process` class) and async patterns (structured concurrency instead of tokio).
