# Error Handling

Error types across the stack, error propagation patterns, and common failure modes.

## Error Type Hierarchy

```mermaid
graph BT
    OP["OpError<br/>(handler layer)"] --> RT["RuntimeError<br/>(plugin layer)"]
    RT --> ERR["ERR frame<br/>(wire)"]
    ERR --> AH["AsyncHostError<br/>(host layer)"]
    AH --> RSE["RelaySwitchError<br/>(relay layer)"]
    RSE --> EX["ExecutionError<br/>(execution layer)"]
    EX --> TASK["Task failure<br/>(application layer)"]
```

Each layer of the system defines its own error type. Errors propagate upward — a plugin handler error becomes a runtime error, then an ERR frame, then an execution error, then a task failure.

### Plugin Layer (RuntimeError)

Defined in `capdag/src/bifaci/plugin_runtime.rs`:

| Variant | Cause |
|---------|-------|
| `Cbor` | CBOR encoding or decoding failure. |
| `Io` | I/O error on stdin/stdout. |
| `NoHandler` | REQ arrived for a cap URN with no registered handler. |
| `Handler` | Handler logic error (custom message from the Op). |
| `CapUrn` | Cap URN parse error. |
| `Deserialize` / `Serialize` | Data conversion errors. |
| `PeerRequest` | Peer call initiation failed. |
| `PeerResponse` | Peer call response error. |
| `Cli` | CLI argument parsing error. |
| `MissingArgument` | Required argument not provided. |
| `UnknownSubcommand` | CLI subcommand not recognized. |
| `Manifest` | Manifest validation error. |
| `CorruptedData` | Data integrity check failed. |
| `Protocol` | Protocol violation (unexpected frame type, missing required fields). |
| `Stream` | Stream-level error (wraps `StreamError`). |

### Host Layer (AsyncHostError)

Defined in `capdag/src/bifaci/host_runtime.rs`:

| Variant | Cause |
|---------|-------|
| `Cbor` | CBOR error during frame processing. |
| `Io` | I/O error on plugin pipes or relay socket. |
| `PluginError` | Plugin sent an ERR frame (code + message). |
| `ProcessExited` | Plugin process died unexpectedly. |
| `Handshake` | HELLO negotiation failed. |
| `Closed` | Host has been shut down. |
| `DuplicateStreamId` | Stream ID reused within a request. |
| `UnknownStreamId` | CHUNK for a stream that was never opened. |
| `ChunkAfterStreamEnd` | CHUNK after the stream was ended. |
| `StreamAfterRequestEnd` | Stream activity after END frame. |
| `StreamStartMissingId` / `StreamStartMissingUrn` | Required fields missing on STREAM_START. |
| `ChunkMissingStreamId` | CHUNK without a stream_id. |
| `Protocol` | Other protocol violations. |
| `NoHandler` | No plugin handles the requested cap. |

### Relay Layer (RelaySwitchError)

Defined in `capdag/src/bifaci/relay_switch.rs`:

| Variant | Cause |
|---------|-------|
| `Cbor` | CBOR error during frame processing. |
| `Io` | I/O error on master socket. |
| `NoHandler` | No master provides the requested cap. |
| `UnknownRequest` | (XID, RID) not in routing tables (continuation frame for unknown request). |
| `Protocol` | Protocol violation. |
| `AllMastersUnhealthy` | No healthy masters available for routing. |

### Execution Layer (ExecutionError)

Defined in `capdag/src/orchestrator/executor.rs`:

| Variant | Cause |
|---------|-------|
| `PluginNotFound` | No plugin binary provides the required cap. |
| `ActivityTimeout` | No frames received for > timeout seconds (default 120s). |
| `PluginExecutionFailed` | Plugin returned ERR frame (code + message). |
| `NoIncomingData` | Source node data missing when executing an edge group. |
| `IoError` | Infrastructure I/O failure. |
| `HostError` | PluginHostRuntime error. |
| `RegistryError` | Plugin registry lookup or download failure. |

### Handler Layer (OpError)

Defined in the `ops` crate:

| Variant | Cause |
|---------|-------|
| `ExecutionFailed` | Generic handler failure with a message string. |

All handler errors are wrapped in this type. The message should be descriptive enough for debugging.

## Error Propagation

```mermaid
graph TD
    subgraph "Plugin Process"
        OP["Op handler"] -->|"Err(OpError)"| DR["dispatch_op()"]
        DR -->|"ERR frame"| STDOUT["stdout"]
    end

    STDOUT --> PHR["PluginHostRuntime"]
    PHR --> RS["RelaySlave → RelayMaster"]
    RS --> SW["RelaySwitch"]
    SW --> EF["execute_fanin"]
    EF -->|"ExecutionError::<br/>PluginExecutionFailed"| CI["cap_interpreter"]
    CI -->|"task state → Failed"| DB["SQLite"]
```

### Plugin → Engine

When a handler fails, the error travels through several layers:

1. **Handler** returns `Err(OpError::ExecutionFailed("message"))`.
2. **PluginRuntime** catches the error in `dispatch_op()`.
3. **ERR frame** sent to stdout with the error code and message.
4. **PluginHostRuntime** forwards the ERR frame to the relay.
5. **RelaySlave → RelayMaster → RelaySwitch** routes the ERR frame to the engine.
6. **execute_fanin** receives the ERR frame on the response channel.
7. Returns `ExecutionError::PluginExecutionFailed { cap_urn, code, message }`.
8. **cap_interpreter** records the failure; task state → `Failed`.

### Plugin Death

```mermaid
sequenceDiagram
    participant P as Plugin
    participant PH as PluginHostRuntime
    participant E as Engine

    Note over P: Process dies unexpectedly
    P--xPH: stdout EOF
    Note over PH: Detect death via reader task
    PH->>PH: Read stderr (last_death_message)
    loop For each pending request
        PH->>E: ERR frame (death message)
    end
    Note over E: ExecutionError::PluginExecutionFailed<br/>or ExecutionError::HostError
```

When a plugin process dies unexpectedly:

1. **PluginHostRuntime** detects stdout EOF (reader task returns).
2. Reads stderr for crash output (last_death_message).
3. For each pending request: sends an ERR frame to the relay with the death message.
4. **execute_fanin** receives the ERR frame or channel closure.
5. Returns `ExecutionError::PluginExecutionFailed` or `ExecutionError::HostError`.

### Activity Timeout

When no frames arrive for too long:

1. **execute_fanin** monitors `last_activity` timestamp in its select loop.
2. If `Instant::now() - last_activity > timeout` (default 120s):
3. Returns `ExecutionError::ActivityTimeout { cap_urn, seconds }`.
4. Task is marked as failed.

The timeout is checked on every iteration of the select loop (every 200ms due to the pump timeout).

## Common Failure Modes

### Stderr Blocking

**Symptom**: Plugin at 0% CPU, all threads sleeping, task appears idle, eventually times out.

**Cause**: An FFI library's log callback calls `fputs(text, stderr)`. In a GUI app sandbox, stderr goes to `/dev/null`. On macOS, `write()` to a dead file descriptor can block forever in `__write_nocancel`.

**Fix**: Suppress log callbacks before loading models:
- `backend.void_logs()` for llama.cpp.
- `mtmd_log_set(void_callback)` for CLIP/MTMD.
- Remove any `fputs(stderr)` or `eprintln!()` calls in plugin code.

Source: ggufcartridge `vision.rs`, `model.rs`. See [33-PROGRESS-AND-LOGGING.md](33-PROGRESS-AND-LOGGING.md).

### Writer Task Starvation

**Symptom**: Frames queued in the output channel but never reach the engine. Activity timeout fires after 120s.

**Cause**: Blocking FFI (model load) runs on a tokio worker thread. The writer task needs a tokio worker to drain the output channel and write to stdout. If all workers are blocked, frames never flush.

**Fix**: Use `run_with_keepalive()` to move blocking work to `tokio::task::spawn_blocking`. This frees the tokio workers for the writer task and emits keepalive frames every 30s.

Source: `plugin_runtime.rs`. See [33-PROGRESS-AND-LOGGING.md](33-PROGRESS-AND-LOGGING.md).

### Missing Peer Route

**Symptom**: `RelaySwitchError::NoHandler` when a plugin makes a peer call.

**Cause**: A plugin calls a cap that no registered plugin provides. This happens when the executor only registered DAG-referenced caps instead of all manifest caps.

**Fix**: The executor registers ALL manifest caps from each plugin, not just the ones referenced by the DAG. This ensures peer invocations can route to caps that are not in the DAG but are in another plugin's manifest.

Source: `executor.rs`, `relay_switch.rs`.

### Disk Full During Download

**Symptom**: Peer response contains error "No space left on device (os error 28)".

**Cause**: Model download fails because the disk does not have enough space for the model file.

**Fix**: Free disk space. The model cartridge does not pre-check available space — it fails during the write.

## Fail-Fast Philosophy

The project follows a strict fail-fast approach:

- **No stopgaps**: No placeholder values that paper over missing data.
- **No fallbacks that hide issues**: If a required stream is missing, fail — do not substitute an empty buffer.
- **No error swallowing**: Every `Result` is propagated with `?` or handled explicitly. `let _ = ...` is only used for non-critical side effects (progress emission, where failure to emit is not worth crashing over).
- **Panics for invariant violations**: `Frame::req()` panics on invalid cap URNs. `PluginRuntime::new()` panics on missing CAP_IDENTITY. These are bugs in the calling code, not runtime errors.
- **Descriptive errors**: Every error variant includes enough context (cap URN, node name, frame type) to diagnose the problem without a debugger.

This philosophy means bugs surface as loud failures at the point of the error rather than as mysterious behavior downstream.
