# 8. Client protocol specification (for SDK authors)

Transport: TCP to `bind:port` of mcache-server (default 6390). No TLS.

## Framing (RESP2 subset)
Request: array of bulk strings: `*<n>\r\n` then n times `$<len>\r\n<bytes>\r\n`. (Inline text lines are accepted by the server but SDKs must not use them: they cannot carry binary values.)

Reply, first byte selects the type:

| Byte | Meaning | Example |
|---|---|---|
| `+` | simple string | `+OK\r\n`, `+PONG\r\n` |
| `-` | error | `-ERR wrong number of arguments for 'get'\r\n`, `-NOAUTH ...`, `-WRONGPASS ...` |
| `:` | integer | `:42\r\n` |
| `$` | bulk string, `$-1\r\n` = nil | `$3\r\nabc\r\n` |
| `*` | array of replies | `*2\r\n$1\r\na\r\n$-1\r\n` |

## Session rules
1. If the server has `requirepass`, send `AUTH <password>` first; until then every command returns `-NOAUTH`.
2. One command in flight per connection is always safe. Pipelining is safe too: replies come back in request order.
3. `QUIT` replies `+OK` and closes.
4. Limits: key 1..1024 bytes; value <= `max-value-bytes` (<= 4,000,000, MSMQ limit); bulk longer than the limit closes the connection with `-ERR protocol`.
5. Timeouts: the server answers `-ERR timeout after N ms ...` if its MSMQ processor is too slow. The command may still run later.
6. Delivery is at-most-once. SDKs must not retry a failed write automatically.
7. After a client-side timeout or a parse error, close the socket: leftover bytes would be misread as the next reply.

## Commands
See `03-protocol.md`. Reply shapes: `SET` -> `+OK` or nil (NX/XX not met); `GET` -> bulk/nil; `DEL`/`EXISTS`/`INCR*`/`TTL`/`PTTL`/`DBSIZE`/`QLEN` -> integer;
`EXPIRE`/`PEXPIRE`/`PERSIST`/`QPURGE`/`QDEL` -> `:1`/`:0`; `MGET`/`KEYS`/`QLIST` -> array; `INFO` -> bulk text of `key:value` lines grouped by `# Section`.

## Conformance checklist for a new SDK
- binary value containing `\r\n`, NUL, `$`, `*` round-trips;
- nil vs empty string are distinguishable;
- `-ERR` becomes an exception/error value, connection stays usable;
- `NOAUTH`/`WRONGPASS` are distinguishable from other errors;
- timeout closes the connection and the next call reconnects;
- one-command pipeline still returns a list (this exact bug existed in the first Python draft).
