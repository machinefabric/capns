# Handshake

Connection setup between host and plugin: frame exchange, limit negotiation, identity verification.

## Handshake Sequence

The handshake is a two-frame exchange followed by an identity verification request.

```
Host                          Plugin
  │                              │
  │──── Hello (limits) ─────────►│
  │                              │
  │◄──── Hello (limits+manifest)─│
  │                              │
  │  both sides compute min()    │
  │  and update reader/writer    │
  │                              │
  │──── REQ(CAP_IDENTITY,nonce) ►│
  │──── STREAM_START ───────────►│
  │──── CHUNK(nonce) ───────────►│
  │──── STREAM_END ─────────────►│
  │──── END ────────────────────►│
  │                              │
  │◄──── STREAM_START ───────────│
  │◄──── CHUNK(nonce echo) ──────│
  │◄──── STREAM_END ─────────────│
  │◄──── END ────────────────────│
  │                              │
  │  connection is now live       │
```

The host always sends first. The plugin waits for the host's Hello before responding.

Source: `capdag/src/bifaci/io.rs` (`handshake`, `handshake_accept`).

## Limit Negotiation

Each Hello frame proposes three parameters in its `meta` map:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_frame` | 3,670,016 (3.5 MB) | Maximum size of a single encoded frame in bytes. |
| `max_chunk` | 262,144 (256 KB) | Maximum payload size for a single CHUNK frame. |
| `max_reorder_buffer` | 64 | Maximum number of out-of-order frames the reorder buffer holds per flow. |

After the exchange, both sides compute `min(ours, theirs)` for each parameter. The resulting limits are applied to both the frame reader and writer immediately. All subsequent frames must respect the negotiated limits.

The `max_frame` default of 3.5 MB provides a safety margin below the hard frame limit of 16 MB. The `max_chunk` default of 256 KB means large responses are automatically split into chunks of at most 256 KB each.

Source: `capdag/src/bifaci/frame.rs` (`Limits`, `DEFAULT_MAX_FRAME`, `DEFAULT_MAX_CHUNK`, `DEFAULT_MAX_REORDER_BUFFER`).

## Manifest Exchange

The plugin's Hello includes a JSON-encoded manifest in `meta["manifest"]` as a byte string. The host's Hello does not include a manifest — it only proposes limits.

The manifest declares:

```json
{
  "name": "ggufcartridge",
  "version": "1.0.0",
  "description": "GGUF model inference via llama.cpp",
  "caps": [
    {
      "urn": "cap:in=\"media:text;encoding=utf8\";out=\"media:text;encoding=utf8\";op=generate;model=gguf",
      "title": "Text Generation",
      "slug": "text-generation",
      "args": [ ... ]
    }
  ]
}
```

The manifest is required — a plugin Hello without a manifest causes a handshake error. The host extracts the manifest and uses it to build the capability routing table.

After the host receives the manifest, it validates that `CAP_IDENTITY` (`cap:in="media:";out="media:"`) is present. Every plugin must declare the identity capability because the handshake verifies it immediately after the Hello exchange.

Source: `capdag/src/bifaci/manifest.rs` (`CapManifest`, `validate`).

## Identity Verification

After the Hello exchange, the host sends a capability invocation for `CAP_IDENTITY` to verify the protocol stack works end-to-end.

The nonce is a deterministic value: CBOR-encoded `Text("bifaci")`, which produces exactly 7 bytes. The host sends this as a standard streaming request:

1. **REQ**: cap = `CAP_IDENTITY`, with XID and seq assigned.
2. **STREAM_START**: stream_id = `"identity-verify"`, media_urn = `"media:"`.
3. **CHUNK**: CBOR-encoded nonce bytes with checksum.
4. **STREAM_END**: chunk_count = 1.
5. **END**: request complete.

The plugin's identity handler receives the nonce and echoes it back through the same streaming pattern. The host reads the response, decodes the CBOR chunks, concatenates the bytes, and compares them to the original nonce.

A mismatch or timeout is a fatal error. The connection is abandoned — something in the frame encoding, relay forwarding, or handler dispatch is broken.

Using a deterministic nonce (rather than random bytes) simplifies testing and debugging. The verification is not a security mechanism — it tests protocol correctness.

Source: `capdag/src/bifaci/io.rs` (`verify_identity`, `identity_nonce`).

## Host-Side API

```rust
pub async fn handshake<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
) -> Result<HandshakeResult, CborError>
```

Sends the host's Hello, reads the plugin's Hello, negotiates limits, updates both reader and writer, and returns a `HandshakeResult` containing the negotiated `Limits` and the plugin's manifest bytes.

Source: `capdag/src/bifaci/io.rs`.

## Plugin-Side API

```rust
pub async fn handshake_accept<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: &mut FrameReader<R>,
    writer: &mut FrameWriter<W>,
    manifest: &[u8],
) -> Result<Limits, CborError>
```

Reads the host's Hello, sends the plugin's Hello (with manifest), negotiates limits, updates both reader and writer, and returns the negotiated `Limits`.

Source: `capdag/src/bifaci/io.rs`.

## Swift Equivalent

The Swift PluginRuntime in `capdag-objc/Sources/Bifaci/PluginRuntime.swift` follows the same handshake protocol. The `run()` method reads the host's Hello from stdin, sends the plugin's Hello with manifest to stdout, and negotiates limits identically. The identity handler is registered automatically.

The main structural difference is that Swift uses synchronous I/O on stdin/stdout (with `FileHandle` reads) rather than async I/O, since the Swift runtime blocks the main thread with a `DispatchSemaphore` during `run()`.
