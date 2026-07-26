//! A minimal Redis-compatible in-memory key-value store.
//!
//! Speaks a subset of the RESP (REdis Serialization Protocol) that real
//! Redis clients use to send commands, so `redis-cli` can talk to this
//! server directly. Supports GET, SET, DEL, and PING.
//!
//! State is shared across connections behind `Arc<RwLock<HashMap<..>>>`.
//! We use `std::sync::RwLock` rather than `tokio::sync::RwLock` because
//! every critical section here is a plain, non-blocking HashMap operation
//! with no `.await` inside it, so the lock is never held across a yield
//! point. That keeps the hot path free of async lock overhead.
//!
//! Run it with `cargo run`, then in another terminal:
//!   redis-cli -p 6380 SET foo bar
//!   redis-cli -p 6380 GET foo
//!   redis-cli -p 6380 DEL foo

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

type Db = Arc<RwLock<HashMap<String, Bytes>>>;

const ADDR: &str = "127.0.0.1:6380";

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let db: Db = Arc::new(RwLock::new(HashMap::new()));
    let listener = TcpListener::bind(ADDR).await?;
    println!("resp_kv_store listening on {ADDR}");

    loop {
        let (socket, peer) = listener.accept().await?;
        let db = Arc::clone(&db);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, db).await {
                eprintln!("connection {peer} ended with error: {e}");
            }
        });
    }
}

/// Drives a single client connection until it disconnects or a fatal
/// I/O error occurs. Protocol errors are reported to the client with a
/// RESP error reply and then the connection is closed, since a malformed
/// frame can leave the byte stream desynchronized and unsafe to keep
/// parsing.
async fn handle_connection(socket: TcpStream, db: Db) -> std::io::Result<()> {
    let mut reader = BufReader::new(socket);
    loop {
        match read_command(&mut reader).await {
            Ok(Some(args)) => {
                let response = execute(args, &db);
                reader.get_mut().write_all(&response).await?;
            }
            Ok(None) => return Ok(()), // client closed the connection cleanly
            Err(RespError::Protocol(msg)) => {
                let response = encode_error(&format!("ERR {msg}"));
                reader.get_mut().write_all(&response).await?;
                return Ok(());
            }
            Err(RespError::Io(e)) => return Err(e),
        }
    }
}

#[derive(Debug)]
enum RespError {
    Io(std::io::Error),
    Protocol(String),
}

impl From<std::io::Error> for RespError {
    fn from(e: std::io::Error) -> Self {
        RespError::Io(e)
    }
}

impl fmt::Display for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespError::Io(e) => write!(f, "io error: {e}"),
            RespError::Protocol(msg) => write!(f, "protocol error: {msg}"),
        }
    }
}

/// Reads one CRLF-terminated line, trimming the trailing CRLF. Returns
/// `Ok(None)` only when the stream is closed before any bytes arrive,
/// which distinguishes a clean disconnect from a truncated frame.
async fn read_line(reader: &mut BufReader<TcpStream>) -> Result<Option<String>, RespError> {
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Ok(None);
    }
    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }
    Ok(Some(line))
}

/// Parses one RESP request off the wire. Real Redis clients send commands
/// as an array of bulk strings, e.g. `SET foo bar` becomes:
///   *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
/// This function only implements that array-of-bulk-strings form, which
/// is all a client library ever sends; it does not implement the
/// human-typed inline command form.
async fn read_command(reader: &mut BufReader<TcpStream>) -> Result<Option<Vec<Bytes>>, RespError> {
    let header = match read_line(reader).await? {
        Some(l) => l,
        None => return Ok(None),
    };

    let mut chars = header.chars();
    let prefix = chars
        .next()
        .ok_or_else(|| RespError::Protocol("empty request line".into()))?;
    if prefix != '*' {
        return Err(RespError::Protocol(format!(
            "expected array header starting with '*', got '{prefix}'"
        )));
    }
    let count: i64 = chars
        .as_str()
        .parse()
        .map_err(|_| RespError::Protocol("invalid array length".into()))?;
    if count <= 0 {
        return Ok(Some(Vec::new()));
    }

    let mut args = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let bulk_header = read_line(reader)
            .await?
            .ok_or_else(|| RespError::Protocol("unexpected eof reading bulk header".into()))?;
        let mut bc = bulk_header.chars();
        let bprefix = bc
            .next()
            .ok_or_else(|| RespError::Protocol("empty bulk header".into()))?;
        if bprefix != '$' {
            return Err(RespError::Protocol(format!(
                "expected bulk string starting with '$', got '{bprefix}'"
            )));
        }
        let len: i64 = bc
            .as_str()
            .parse()
            .map_err(|_| RespError::Protocol("invalid bulk length".into()))?;

        if len < 0 {
            args.push(Bytes::new());
            continue;
        }

        let mut buf = vec![0u8; len as usize];
        reader.read_exact(&mut buf).await?;

        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await?;

        args.push(Bytes::from(buf));
    }

    Ok(Some(args))
}

fn execute(args: Vec<Bytes>, db: &Db) -> Vec<u8> {
    let Some(cmd_bytes) = args.first() else {
        return encode_error("ERR empty command");
    };
    let cmd = String::from_utf8_lossy(cmd_bytes).to_ascii_uppercase();

    match cmd.as_str() {
        "PING" => encode_simple("PONG"),

        "GET" => {
            if args.len() != 2 {
                return encode_error("ERR wrong number of arguments for 'get' command");
            }
            let key = String::from_utf8_lossy(&args[1]);
            let db = db.read().expect("kv store lock poisoned");
            match db.get(key.as_ref()) {
                Some(value) => encode_bulk(value),
                None => encode_null(),
            }
        }

        "SET" => {
            if args.len() != 3 {
                return encode_error("ERR wrong number of arguments for 'set' command");
            }
            let key = String::from_utf8_lossy(&args[1]).into_owned();
            let value = args[2].clone();
            let mut db = db.write().expect("kv store lock poisoned");
            db.insert(key, value);
            encode_simple("OK")
        }

        "DEL" => {
            if args.len() < 2 {
                return encode_error("ERR wrong number of arguments for 'del' command");
            }
            let mut db = db.write().expect("kv store lock poisoned");
            let removed = args[1..]
                .iter()
                .filter(|k| db.remove(String::from_utf8_lossy(k).as_ref()).is_some())
                .count();
            encode_integer(removed as i64)
        }

        other => encode_error(&format!("ERR unknown command '{other}'")),
    }
}

fn encode_simple(s: &str) -> Vec<u8> {
    format!("+{s}\r\n").into_bytes()
}

fn encode_error(s: &str) -> Vec<u8> {
    format!("-{s}\r\n").into_bytes()
}

fn encode_integer(n: i64) -> Vec<u8> {
    format!(":{n}\r\n").into_bytes()
}

fn encode_null() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

fn encode_bulk(data: &Bytes) -> Vec<u8> {
    let mut out = format!("${}\r\n", data.len()).into_bytes();
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Spins up the server on an OS-assigned port and returns it so tests
    /// don't collide with each other or with a real instance on 6380.
    async fn spawn_test_server() -> std::net::SocketAddr {
        let db: Db = Arc::new(RwLock::new(HashMap::new()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let db = Arc::clone(&db);
                tokio::spawn(handle_connection(socket, db));
            }
        });
        addr
    }

    async fn roundtrip(addr: std::net::SocketAddr, request: &[u8]) -> Vec<u8> {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut buf = vec![0u8; 512];
        let n = stream.read(&mut buf).await.unwrap();
        buf.truncate(n);
        buf
    }

    #[tokio::test]
    async fn set_then_get() {
        let addr = spawn_test_server().await;

        let set_reply = roundtrip(addr, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").await;
        assert_eq!(set_reply, b"+OK\r\n");

        let get_reply = roundtrip(addr, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n").await;
        assert_eq!(get_reply, b"$3\r\nbar\r\n");
    }

    #[tokio::test]
    async fn get_missing_key_returns_null() {
        let addr = spawn_test_server().await;
        let reply = roundtrip(addr, b"*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n").await;
        assert_eq!(reply, b"$-1\r\n");
    }

    #[tokio::test]
    async fn del_reports_count_removed() {
        let addr = spawn_test_server().await;
        roundtrip(addr, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n").await;
        let reply = roundtrip(addr, b"*3\r\n$3\r\nDEL\r\n$1\r\nk\r\n$7\r\nmissing\r\n").await;
        assert_eq!(reply, b":1\r\n");
    }

    #[tokio::test]
    async fn unknown_command_returns_error() {
        let addr = spawn_test_server().await;
        let reply = roundtrip(addr, b"*1\r\n$4\r\nNOPE\r\n").await;
        assert!(reply.starts_with(b"-ERR unknown command"));
    }
}
