# msmqcache (Rust)

Rust 2021, **no dependencies**, blocking client (`&mut self`; wrap in a `Mutex` to share). An async variant would need an async runtime and is not included.
Client for **mcache-server**, the MSMQ-backed cache/queue server (Windows). This client runs anywhere and talks to the server's TCP port.

```toml
[dependencies]
msmqcache = { path = "sdk/rust" }   # not on crates.io yet
```
```rust
use msmqcache::{Client, QPushOptions, SetOptions};

let mut c = Client::connect("127.0.0.1:6390");
c.set("session:1", "alice", &SetOptions { ex: Some(300), ..Default::default() })?;  // TTL = MSMQ TimeToBeReceived
let v: Option<Vec<u8>> = c.get("session:1")?;
c.qpush("jobs", "resize:42", &QPushOptions { priority: Some(5), ttl: None })?;
let res = c.pipeline().set("a", "1", &SetOptions::default()).get("a").exec()?;
```
Errors: `Error::{Connection{timeout}, Protocol, Response, Auth, Invalid}` with `is_timeout()` / `is_auth()`.

## Behaviour you should know
* Values are binary safe (tested with `\r\n`, NUL, `$`, `*`). A nil reply and an empty value are distinguishable.
* Keys: 1..1024 bytes. Values: up to 4,000,000 bytes by default (MSMQ's limit is 4 MB per message). Both are checked
  client-side before anything is sent.
* **No automatic retry.** The server executes each command at most once; after a timeout or lost connection the
  command may or may not have run. The client closes the socket (so replies cannot desynchronise) and reconnects on the
  next call.
* Authentication failures (`NOAUTH` / `WRONGPASS`) are a distinct error type from other server errors.
* `expire`/`persist` make the server re-send the value (MSMQ messages are immutable): cost grows with value size.
  `keys` and `qlen` are O(n) on the server. `qpop` is non-blocking (poll it).
* Default client timeout is 10 s; the server's own `command-timeout-ms` is 5 s and answers `-ERR timeout ...` first.
* Wire format: [`docs/08-client-protocol-spec.md`](../../docs/08-client-protocol-spec.md).

## Tests
Run against the shared fake server (reproduces the wire format only; it is **not** a real mcache-server or MSMQ):
`cd sdk/rust && ../conformance/run.sh cargo test`

---

## 👤 Author
        
[Hadi Cahyadi](mailto:cumulus13@gmail.com)
    

[![Buy Me a Coffee](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/cumulus13)

[![Donate via Ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/cumulus13)
 
[Support me on Patreon](https://www.patreon.com/cumulus13)