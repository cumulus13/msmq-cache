# msmqcache (Python)

Zero-dependency Python 3.8+ client for **mcache-server**, the MSMQ-backed cache/queue server. Sync, asyncio and pipelines.
The server runs on Windows; this client runs anywhere and talks to the server's TCP port.

```bash
pip install ./sdk/python        # from the repo root (not on PyPI yet)
```

```python
from datetime import timedelta
from msmqcache import Client

with Client("127.0.0.1", 6390, password=None, decode_responses=True) as c:
    c.set("session:1", "alice", ex=300)            # TTL = MSMQ TimeToBeReceived on the server
    c.set("lock", "me", nx=True, px=timedelta(seconds=5))
    print(c.get("session:1"), c.ttl("session:1"))
    c.incr("hits")
    c.qpush("jobs", "resize:42", priority=5)       # native MSMQ queue, priority 0..7
    print(c.qpop("jobs"))                          # None when empty (no blocking pop)
    with c.pipeline() as p:                        # one write, replies read in order
        p.set("a", 1).set("b", 2)
        p.mget(["a", "b"])
        print(p.execute())                         # [True, True, [b'1', b'2']]
```

```python
import asyncio
from msmqcache import AsyncClient

async def main():
    async with AsyncClient(port=6390) as c:
        await c.set("k", b"\x00binary\r\n")
        print(await c.get("k"))
asyncio.run(main())
```

## Behaviour you should know
* Values are binary safe. `str` is encoded with `encoding` (UTF-8); replies are `bytes` unless `decode_responses=True`.
* Keys: 1..1024 bytes. Values: up to `max_value_bytes` (default 4,000,000; MSMQ's limit is 4 MB per message). Checked client-side.
* **No automatic retry.** The server executes commands at most once; after a timeout or lost connection the command may
  or may not have run. The client closes the socket (so replies cannot desynchronise) and reconnects on the next call.
* Errors: `ResponseError` (server `-ERR`), `AuthenticationError`, `CacheConnectionError`, `CacheTimeoutError`, `ProtocolError`.
* `expire()`/`persist()` make the server re-send the value (MSMQ messages are immutable): cost grows with value size.
* `keys()`, `qlen()` are O(n) on the server. One connection serialises calls; open several clients for parallelism.
* Default client timeout is 10 s; the server's own `command-timeout-ms` is 5 s and answers `-ERR timeout ...` first.

## Tests
`python -m unittest discover -s sdk/python/tests` runs against an in-process fake server that reproduces the wire format
(framing, nil/error replies, auth). It does **not** exercise a real mcache-server or MSMQ.
