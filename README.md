# resp_kv_store

A minimal Redis-compatible in-memory key-value store, written from scratch
against the Rust standard library plus tokio. Speaks a subset of RESP (the
REdis Serialization Protocol) directly over TCP, so real Redis clients like
`redis-cli` can talk to it without any translation layer.

## What it demonstrates

- **Wire protocol parsing from raw bytes**: `read_command` parses RESP's
  array-of-bulk-strings request format directly off a buffered TCP stream,
  byte by byte, no framing library.
- **Shared mutable state across async connections**: the store is
  `Arc<RwLock<HashMap<String, Bytes>>>`. It uses `std::sync::RwLock` rather
  than `tokio::sync::RwLock` deliberately: every critical section is a
  synchronous HashMap operation with no `.await` inside it, so a std lock is
  never held across a yield point, and the hot path avoids async lock
  overhead entirely.
- **Per-connection concurrency**: each client connection is a separate
  spawned tokio task; a protocol error on one connection replies with a RESP
  error and closes just that connection.

## Supported commands

`GET`, `SET`, `DEL`, `PING`. Unknown commands get a RESP error reply rather
than closing the connection.

## Run it

```sh
cargo run
```

Starts listening on `127.0.0.1:6380`. In another terminal:

```sh
redis-cli -p 6380 SET foo bar
redis-cli -p 6380 GET foo
redis-cli -p 6380 DEL foo
```

## Tests

```sh
cargo test
```

Integration-style tests spin up the server on an OS-assigned port (so they
never collide with each other or a real instance on 6380) and drive it with
raw RESP byte sequences over a real TCP connection.

## Known limitations

Only the array-of-bulk-strings RESP request form is implemented (what every
real client library sends). The human-typed inline command form isn't
supported. No pub/sub, expiry, or persistence, this is scoped to demonstrate
the protocol and concurrency model, not to be feature-complete.
