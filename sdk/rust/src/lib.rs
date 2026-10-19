// File: sdk\rust\src\lib.rs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-19
// Description: 
// License: MIT

//! Zero-dependency blocking client for `mcache-server` (the MSMQ-backed cache / queue server).
//!
//! A command that fails with a connection error or timeout is **never retried** (the server is at-most-once);
//! the connection is dropped and re-opened by the next call.

use std::collections::HashMap;
use std::fmt;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

const MAX_LINE: usize = 64 * 1024;
const MAX_KEY: usize = 1024;

// ---------------------------------------------------------------- errors
#[derive(Debug)]
pub enum Error {
    /// TCP failure; `timeout` = no reply in time (connection closed, the command may still be executed).
    Connection { source: io::Error, timeout: bool },
    Protocol(String),
    /// Server `-ERR ...`
    Response(String),
    /// `NOAUTH` / `WRONGPASS`
    Auth(String),
    /// Rejected on the client side before sending.
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn is_timeout(&self) -> bool { matches!(self, Error::Connection { timeout: true, .. }) }
    pub fn is_auth(&self) -> bool { matches!(self, Error::Auth(_)) }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connection { source, timeout: true } => write!(f, "timeout, connection closed, command may still be executed: {source}"),
            Error::Connection { source, .. } => write!(f, "connection error: {source}"),
            Error::Protocol(m) => write!(f, "protocol error: {m}"),
            Error::Response(m) => write!(f, "server error: {m}"),
            Error::Auth(m) => write!(f, "authentication error: {m}"),
            Error::Invalid(m) => write!(f, "invalid argument: {m}"),
        }
    }
}

impl std::error::Error for Error {}

fn conn_err(e: io::Error) -> Error {
    let timeout = matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut);
    Error::Connection { source: e, timeout }
}

fn server_err(msg: String) -> Error {
    if msg.starts_with("NOAUTH") || msg.starts_with("WRONGPASS") { Error::Auth(msg) } else { Error::Response(msg) }
}

// ---------------------------------------------------------------- values
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Nil,
    Status(String),
    Int(i64),
    Bulk(Vec<u8>),
    Array(Vec<Value>),
    /// Server error reply (only appears inside pipeline results and arrays; single commands return `Err`).
    Error(String),
}

impl Value {
    fn into_bytes(self) -> Result<Option<Vec<u8>>> {
        match self {
            Value::Nil => Ok(None),
            Value::Bulk(b) => Ok(Some(b)),
            other => Err(Error::Protocol(format!("expected bulk reply, got {other:?}"))),
        }
    }
    fn into_int(self) -> Result<i64> {
        match self { Value::Int(n) => Ok(n), other => Err(Error::Protocol(format!("expected integer reply, got {other:?}"))) }
    }
    fn into_list(self) -> Result<Vec<Option<Vec<u8>>>> {
        match self {
            Value::Array(items) => items.into_iter().map(Value::into_bytes).collect(),
            other => Err(Error::Protocol(format!("expected array reply, got {other:?}"))),
        }
    }
}

// ---------------------------------------------------------------- options
#[derive(Debug, Clone)]
pub struct Options {
    pub addr: String,
    pub password: Option<String>,
    /// Per reply timeout (default 10 s).
    pub timeout: Duration,
    pub connect_timeout: Duration,
    pub max_value_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options { addr: "127.0.0.1:6390".into(), password: None, timeout: Duration::from_secs(10),
                  connect_timeout: Duration::from_secs(5), max_value_bytes: 4_000_000 }
    }
}

/// `ex` seconds, `px` milliseconds; `None` = unset.
#[derive(Debug, Clone, Default)]
pub struct SetOptions { pub ex: Option<u64>, pub px: Option<u64>, pub nx: bool, pub xx: bool, pub keep_ttl: bool }

/// `priority` 0..=7 (7 highest), `ttl` seconds (MSMQ TimeToBeReceived).
#[derive(Debug, Clone, Default)]
pub struct QPushOptions { pub priority: Option<u8>, pub ttl: Option<u64> }

// ---------------------------------------------------------------- wire
fn encode(cmds: &[Vec<Vec<u8>>]) -> Vec<u8> {
    let mut out = Vec::new();
    for args in cmds {
        out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
    }
    out
}

fn read_line<R: BufRead>(r: &mut R) -> Result<String> {
    let mut line = Vec::new();
    loop {
        let n = r.read_until(b'\n', &mut line).map_err(conn_err)?;
        if n == 0 {
            return Err(Error::Connection { source: io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed by server"), timeout: false });
        }
        if line.ends_with(b"\n") { break; }
        if line.len() > MAX_LINE { return Err(Error::Protocol("line too long".into())); }
    }
    if !line.ends_with(b"\r\n") { return Err(Error::Protocol("line not terminated by CRLF".into())); }
    line.truncate(line.len() - 2);
    String::from_utf8(line).map_err(|_| Error::Protocol("non UTF-8 header line".into()))
}

fn read_value<R: BufRead>(r: &mut R) -> Result<Value> {
    let line = read_line(r)?;
    let (t, rest) = line.split_at(line.chars().next().map(|c| c.len_utf8()).unwrap_or(0));
    let num = |what: &str| rest.parse::<i64>().map_err(|_| Error::Protocol(format!("invalid {what}: {rest:?}")));
    match t {
        "+" => Ok(Value::Status(rest.to_string())),
        "-" => Ok(Value::Error(rest.to_string())),
        ":" => Ok(Value::Int(num("integer")?)),
        "$" => {
            let n = num("bulk length")?;
            if n < 0 { return Ok(Value::Nil); }
            let mut buf = vec![0u8; n as usize + 2];
            r.read_exact(&mut buf).map_err(conn_err)?;
            buf.truncate(n as usize);
            Ok(Value::Bulk(buf))
        }
        "*" => {
            let n = num("array length")?;
            if n < 0 { return Ok(Value::Nil); }
            let mut items = Vec::with_capacity(n as usize);
            for _ in 0..n { items.push(read_value(r)?); }
            Ok(Value::Array(items))
        }
        _ => Err(Error::Protocol(format!("unknown reply type {t:?}"))),
    }
}

struct Conn { w: TcpStream, r: BufReader<TcpStream> }

// ---------------------------------------------------------------- client
/// Blocking client. Methods take `&mut self`; wrap it in a `Mutex` to share between threads.
pub struct Client { o: Options, conn: Option<Conn> }

impl Client {
    pub fn new(o: Options) -> Client { Client { o, conn: None } }
    pub fn connect(addr: &str) -> Client { Client::new(Options { addr: addr.into(), ..Options::default() }) }
    pub fn with_password(mut self, p: &str) -> Client { self.o.password = Some(p.into()); self }
    pub fn with_timeout(mut self, t: Duration) -> Client { self.o.timeout = t; self }
    /// Drop the connection; the next command reconnects.
    pub fn close(&mut self) { self.conn = None; }

    fn ensure(&mut self) -> Result<()> {
        if self.conn.is_some() { return Ok(()); }
        let mut last = None;
        let addrs = self.o.addr.to_socket_addrs().map_err(conn_err)?;
        let mut stream = None;
        for a in addrs {
            match TcpStream::connect_timeout(&a, self.o.connect_timeout) {
                Ok(s) => { stream = Some(s); break; }
                Err(e) => last = Some(e),
            }
        }
        let s = stream.ok_or_else(|| conn_err(last.unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no address resolved"))))?;
        s.set_nodelay(true).ok();
        s.set_read_timeout(Some(self.o.timeout)).map_err(conn_err)?;
        s.set_write_timeout(Some(self.o.timeout)).map_err(conn_err)?;
        let r = BufReader::new(s.try_clone().map_err(conn_err)?);
        self.conn = Some(Conn { w: s, r });
        if let Some(pw) = self.o.password.clone() {
            let mut reps = self.roundtrip(&[vec![b"AUTH".to_vec(), pw.into_bytes()]])?;
            if let Value::Error(m) = reps.remove(0) { self.conn = None; return Err(server_err(m)); }
        }
        Ok(())
    }

    fn roundtrip(&mut self, cmds: &[Vec<Vec<u8>>]) -> Result<Vec<Value>> {
        let conn = self.conn.as_mut().expect("connected");
        let io = (|| -> Result<Vec<Value>> {
            conn.w.write_all(&encode(cmds)).map_err(conn_err)?;
            conn.w.flush().map_err(conn_err)?;
            cmds.iter().map(|_| read_value(&mut conn.r)).collect()
        })();
        if io.is_err() { self.conn = None; } // unread bytes would be misread as the next reply
        io
    }

    fn exec(&mut self, cmds: &[Vec<Vec<u8>>]) -> Result<Vec<Value>> {
        self.ensure()?;
        self.roundtrip(cmds)
    }

    /// Run any command: `c.execute(&["INFO"])`. A server error reply becomes `Err`.
    pub fn execute<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Result<Value> {
        let cmd: Vec<Vec<u8>> = args.iter().map(|a| a.as_ref().to_vec()).collect();
        match self.exec(&[cmd])?.remove(0) {
            Value::Error(m) => Err(server_err(m)),
            v => Ok(v),
        }
    }

    fn key(k: &[u8]) -> Result<Vec<u8>> {
        if k.is_empty() || k.len() > MAX_KEY { return Err(Error::Invalid(format!("key length must be 1..={MAX_KEY} bytes"))); }
        Ok(k.to_vec())
    }
    fn val(&self, v: &[u8]) -> Result<Vec<u8>> {
        if v.len() > self.o.max_value_bytes { return Err(Error::Invalid(format!("value is {} bytes; limit is {}", v.len(), self.o.max_value_bytes))); }
        Ok(v.to_vec())
    }
    fn set_args(&self, key: &[u8], value: &[u8], o: &SetOptions) -> Result<Vec<Vec<u8>>> {
        if o.nx && o.xx { return Err(Error::Invalid("nx and xx are mutually exclusive".into())); }
        if o.ex.is_some() && o.px.is_some() { return Err(Error::Invalid("use either ex or px".into())); }
        if o.ex == Some(0) || o.px == Some(0) { return Err(Error::Invalid("ex/px must be positive".into())); }
        let mut a = vec![b"SET".to_vec(), Self::key(key)?, self.val(value)?];
        if let Some(n) = o.ex { a.push(b"EX".to_vec()); a.push(n.to_string().into_bytes()); }
        if let Some(n) = o.px { a.push(b"PX".to_vec()); a.push(n.to_string().into_bytes()); }
        if o.nx { a.push(b"NX".to_vec()); }
        if o.xx { a.push(b"XX".to_vec()); }
        if o.keep_ttl { a.push(b"KEEPTTL".to_vec()); }
        Ok(a)
    }
    fn qpush_args(&self, queue: &str, value: &[u8], o: &QPushOptions) -> Result<Vec<Vec<u8>>> {
        let mut a = vec![b"QPUSH".to_vec(), queue.as_bytes().to_vec(), self.val(value)?];
        if let Some(p) = o.priority {
            if p > 7 { return Err(Error::Invalid("priority must be 0..=7".into())); }
            a.push(b"PRIORITY".to_vec()); a.push(p.to_string().into_bytes());
        }
        if let Some(t) = o.ttl {
            if t == 0 { return Err(Error::Invalid("ttl must be positive".into())); }
            a.push(b"TTL".to_vec()); a.push(t.to_string().into_bytes());
        }
        Ok(a)
    }
    fn run(&mut self, a: Vec<Vec<u8>>) -> Result<Value> { self.execute(&a) }

    pub fn ping(&mut self) -> Result<bool> { Ok(self.execute(&["PING"])? == Value::Status("PONG".into())) }
    /// `Ok(true)` stored, `Ok(false)` NX/XX prevented it.
    pub fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>, o: &SetOptions) -> Result<bool> {
        let a = self.set_args(key.as_ref(), value.as_ref(), o)?;
        Ok(self.run(a)? != Value::Nil)
    }
    /// `Ok(None)` for a missing key; an empty value is `Some(vec![])`.
    pub fn get(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        self.run(vec![b"GET".to_vec(), Self::key(key.as_ref())?])?.into_bytes()
    }
    pub fn mget<K: AsRef<[u8]>>(&mut self, keys: &[K]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut a = vec![b"MGET".to_vec()];
        for k in keys { a.push(Self::key(k.as_ref())?); }
        self.run(a)?.into_list()
    }
    /// Not atomic on the server (one MSMQ message per key).
    pub fn mset<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, pairs: &[(K, V)]) -> Result<()> {
        let mut a = vec![b"MSET".to_vec()];
        for (k, v) in pairs { a.push(Self::key(k.as_ref())?); a.push(self.val(v.as_ref())?); }
        self.run(a).map(|_| ())
    }
    pub fn del<K: AsRef<[u8]>>(&mut self, keys: &[K]) -> Result<i64> {
        let mut a = vec![b"DEL".to_vec()];
        for k in keys { a.push(Self::key(k.as_ref())?); }
        self.run(a)?.into_int()
    }
    pub fn exists<K: AsRef<[u8]>>(&mut self, keys: &[K]) -> Result<i64> {
        let mut a = vec![b"EXISTS".to_vec()];
        for k in keys { a.push(Self::key(k.as_ref())?); }
        self.run(a)?.into_int()
    }
    /// Re-sends the value on the server (MSMQ messages are immutable).
    pub fn expire(&mut self, key: impl AsRef<[u8]>, seconds: u64) -> Result<bool> {
        Ok(self.run(vec![b"EXPIRE".to_vec(), Self::key(key.as_ref())?, seconds.to_string().into_bytes()])?.into_int()? == 1)
    }
    /// Seconds left; -1 no expiry; -2 no such key.
    pub fn ttl(&mut self, key: impl AsRef<[u8]>) -> Result<i64> { self.run(vec![b"TTL".to_vec(), Self::key(key.as_ref())?])?.into_int() }
    pub fn incr(&mut self, key: impl AsRef<[u8]>) -> Result<i64> { self.run(vec![b"INCR".to_vec(), Self::key(key.as_ref())?])?.into_int() }
    pub fn decr(&mut self, key: impl AsRef<[u8]>) -> Result<i64> { self.run(vec![b"DECR".to_vec(), Self::key(key.as_ref())?])?.into_int() }
    pub fn incr_by(&mut self, key: impl AsRef<[u8]>, n: i64) -> Result<i64> {
        self.run(vec![b"INCRBY".to_vec(), Self::key(key.as_ref())?, n.to_string().into_bytes()])?.into_int()
    }
    /// O(n) on the server.
    pub fn keys(&mut self, pattern: &str) -> Result<Vec<Vec<u8>>> {
        Ok(self.run(vec![b"KEYS".to_vec(), pattern.as_bytes().to_vec()])?.into_list()?.into_iter().flatten().collect())
    }
    pub fn dbsize(&mut self) -> Result<i64> { self.execute(&["DBSIZE"])?.into_int() }
    pub fn flushdb(&mut self) -> Result<()> { self.execute(&["FLUSHDB"]).map(|_| ()) }

    pub fn qpush(&mut self, queue: &str, value: impl AsRef<[u8]>, o: &QPushOptions) -> Result<()> {
        let a = self.qpush_args(queue, value.as_ref(), o)?;
        self.run(a).map(|_| ())
    }
    /// Non-blocking: `Ok(None)` when empty.
    pub fn qpop(&mut self, queue: &str) -> Result<Option<Vec<u8>>> { self.run(vec![b"QPOP".to_vec(), queue.as_bytes().to_vec()])?.into_bytes() }
    pub fn qpeek(&mut self, queue: &str) -> Result<Option<Vec<u8>>> { self.run(vec![b"QPEEK".to_vec(), queue.as_bytes().to_vec()])?.into_bytes() }
    pub fn qlen(&mut self, queue: &str) -> Result<i64> { self.run(vec![b"QLEN".to_vec(), queue.as_bytes().to_vec()])?.into_int() }
    pub fn qlist(&mut self) -> Result<Vec<Vec<u8>>> { Ok(self.execute(&["QLIST"])?.into_list()?.into_iter().flatten().collect()) }

    pub fn info(&mut self) -> Result<HashMap<String, HashMap<String, String>>> {
        let text = String::from_utf8_lossy(&self.execute(&["INFO"])?.into_bytes()?.unwrap_or_default()).into_owned();
        let mut out: HashMap<String, HashMap<String, String>> = HashMap::new();
        let mut section = "default".to_string();
        for line in text.lines().map(str::trim).filter(|l| !l.is_empty()) {
            if let Some(h) = line.strip_prefix('#') { section = h.trim().to_lowercase(); continue; }
            if let Some((k, v)) = line.split_once(':') { out.entry(section.clone()).or_default().insert(k.into(), v.into()); }
        }
        Ok(out)
    }

    pub fn pipeline(&mut self) -> Pipeline<'_> { Pipeline { c: self, cmds: Vec::new(), err: None } }
}

/// Queues commands and sends them in one write. The server still runs them one after another.
pub struct Pipeline<'a> { c: &'a mut Client, cmds: Vec<Vec<Vec<u8>>>, err: Option<Error> }

impl<'a> Pipeline<'a> {
    pub fn cmd<A: AsRef<[u8]>>(mut self, args: &[A]) -> Self { self.cmds.push(args.iter().map(|a| a.as_ref().to_vec()).collect()); self }
    pub fn set(mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>, o: &SetOptions) -> Self {
        match self.c.set_args(key.as_ref(), value.as_ref(), o) { Ok(a) => self.cmds.push(a), Err(e) => { self.err.get_or_insert(e); } }
        self
    }
    pub fn get(mut self, key: impl AsRef<[u8]>) -> Self {
        match Client::key(key.as_ref()) { Ok(k) => self.cmds.push(vec![b"GET".to_vec(), k]), Err(e) => { self.err.get_or_insert(e); } }
        self
    }
    pub fn incr(mut self, key: impl AsRef<[u8]>) -> Self {
        match Client::key(key.as_ref()) { Ok(k) => self.cmds.push(vec![b"INCR".to_vec(), k]), Err(e) => { self.err.get_or_insert(e); } }
        self
    }
    /// All replies in order; server errors appear as `Value::Error` entries.
    pub fn exec_lenient(self) -> Result<Vec<Value>> {
        if let Some(e) = self.err { return Err(e); }
        if self.cmds.is_empty() { return Ok(vec![]); }
        self.c.exec(&self.cmds)
    }
    /// Like `exec_lenient` but returns the first server error as `Err`.
    pub fn exec(self) -> Result<Vec<Value>> {
        let reps = self.exec_lenient()?;
        if let Some(Value::Error(m)) = reps.iter().find(|v| matches!(v, Value::Error(_))) { return Err(server_err(m.clone())); }
        Ok(reps)
    }
}
