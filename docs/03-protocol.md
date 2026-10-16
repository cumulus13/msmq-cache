# 3. Protocol and commands

The wire format is a subset of **RESP2**: commands are arrays of bulk strings (binary safe); plain "inline" lines such as `SET a "b c"` also work (telnet). **The same bytes are the body of the MSMQ messages.** Replies: `+OK`, `-ERR ...`, `:123`, `$len`, `*n`.

| Command | Description |
|---|---|
| `PING [msg]`, `ECHO msg` | liveness |
| `SET key value [EX s] [PX ms] [NX] [XX] [KEEPTTL]` | store; expiry = MSMQ TimeToBeReceived (rounded **up** to whole seconds in MSMQ; exact ms enforced by the server on read) |
| `GET key` / `MGET k...` / `MSET k v ...` | read / multi-read / multi-write (MSET is not atomic) |
| `DEL k...`, `EXISTS k...` | delete / count existing |
| `EXPIRE key s`, `PEXPIRE key ms`, `PERSIST key` | change TTL. MSMQ messages are immutable, so the value is **re-sent** |
| `TTL key`, `PTTL key` | remaining; `-1` no expiry, `-2` missing |
| `INCR/DECR/INCRBY/DECRBY` | integer arithmetic (executed inside the single engine thread, so atomic) |
| `KEYS pattern`, `DBSIZE`, `FLUSHDB` | O(n) over the index; FLUSHDB purges the data queue |
| `QPUSH q value [PRIORITY 0-7] [TTL s]` | append to native MSMQ queue `q` (auto-created) |
| `QPOP q`, `QPEEK q` | non-blocking pop / peek, nil if empty |
| `QLEN q` | O(n) (enumerates the queue) |
| `QPURGE q`, `QDEL q`, `QLIST` | purge / delete queue / list queues |
| `INFO` | server and MSMQ info |
| `AUTH password`, `QUIT` | TCP only (handled by the gateway) |

Not implemented: pub/sub, blocking pops, transactions across commands (`MULTI`), Lua, clustering, replication. See `07-limitations.md`.

## COM object (`MsmqCache.Client`)
`Connect(host, prefix)`, `TimeoutMs`, `Execute(cmdline)`, `Post(cmdline)` (no reply), `Set(key,value,ttl)`, `Get`, `Del`, `Exists`, `Incr`, `Ttl`, `Close`. nil is returned as `Empty`; server errors raise a COM error with the server text. Integers beyond 32 bit are returned as strings (VBScript has no 64-bit integer).
