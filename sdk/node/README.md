# msmqcache (Node.js / TypeScript)

Node 18+, TypeScript types included, zero runtime dependencies. Commands are pipelined natively over one socket.
Client for **mcache-server**, the MSMQ-backed cache/queue server (Windows). This client runs anywhere and talks to the server's TCP port.

```bash
npm install ./sdk/node      # from the repo root (not on npm yet)
```
```ts
import { Client } from "msmqcache";

const c = new Client({ host: "127.0.0.1", port: 6390, password: undefined, decodeResponses: true });
await c.set("session:1", "alice", { ex: 300 });          // TTL = MSMQ TimeToBeReceived on the server
await c.set("lock", "me", { nx: true, px: 5000 });
console.log(await c.get("session:1"), await c.ttl("session:1"));
await c.qpush("jobs", "resize:42", { priority: 5 });      // native MSMQ queue, priority 0..7
console.log(await c.qpop("jobs"));                        // null when empty
const res = await c.pipeline().set("a", 1).set("b", 2).mget(["a", "b"]).execute();
await c.close();
```
Errors: `ResponseError`, `AuthenticationError`, `CacheConnectionError`, `CacheTimeoutError`, `ProtocolError`.

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
`cd sdk/node && ../conformance/run.sh npm test`
