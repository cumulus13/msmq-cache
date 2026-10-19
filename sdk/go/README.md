# msmqcache (Go)

Go 1.20+, standard library only, `context` aware.
Client for **mcache-server**, the MSMQ-backed cache/queue server (Windows). This client runs anywhere and talks to the server's TCP port.

```bash
go get github.com/cumulus13/msmq-cache/sdk/go
```
```go
c := msmqcache.New(msmqcache.Options{Addr: "127.0.0.1:6390"})
defer c.Close()
ctx := context.Background()

c.Set(ctx, "session:1", "alice", msmqcache.SetOptions{EX: 300})   // TTL = MSMQ TimeToBeReceived
ok, _ := c.Set(ctx, "lock", "me", msmqcache.SetOptions{NX: true, PX: 5000})
v, err := c.Get(ctx, "session:1")
if errors.Is(err, msmqcache.ErrNil) { /* missing key */ }
c.QPush(ctx, "jobs", "resize:42", msmqcache.QPushOptions{Priority: 5}) // Priority: -1 = default
res, err := c.Pipeline().Set("a", 1, msmqcache.SetOptions{}).Get("a").Exec(ctx)
```
Errors: `*ResponseError`, `*AuthError` (`IsAuth`), `*ConnectionError` (`IsTimeout`), `*ProtocolError`, sentinel `ErrNil`.

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
`cd sdk/go && ../conformance/run.sh go test -count=1 ./...`

---

## 👤 Author
        
[Hadi Cahyadi](mailto:cumulus13@gmail.com)
    

[![Buy Me a Coffee](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/cumulus13)

[![Donate via Ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/cumulus13)
 
[Support me on Patreon](https://www.patreon.com/cumulus13)