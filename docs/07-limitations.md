# 7. Limitations and known risks

1. **Not a drop-in Redis.** No pub/sub, blocking pops, `MULTI`, replication, clustering, persistence snapshots.
2. **Performance is unmeasured.** Every command costs MSMQ messages (request + reply, plus a transaction for writes). Throughput is bounded by MSMQ disk I/O; use `recoverable no` for express (in-memory) delivery of commands.
3. **At-most-once command execution.** A command is removed from the ingress queue when received; a crash during execution loses it. A TCP timeout does not cancel the command: it may still execute later.
4. **Index is in RAM.** Keys (not values) must fit in memory; startup time grows with the number of stored messages.
5. **Single writer assumption.** Only the server process may write to the `.data` queue (used for the `Last` lookup).
6. **TTL granularity.** MSMQ expiry is whole seconds; `PX` precision is enforced by the server on read, and `EXPIRE` re-sends the value.
7. **Not-found handling** assumes `InvalidOperationException` for a missing lookup id (per .NET documentation); a `MessageQueueException` instead would surface as `-ERR MSMQ ...` rather than nil.
8. **COM clients**: one object serialises its calls; shared reply queue `prefix.reply.com` needs permissions for the server to send replies back; orphaned replies expire after `reply-ttl`.
9. **Server targets .NET Framework 4.8** (not single-file). Reason: `System.Messaging` is in-box there, so the API surface is the documented one and COM registration uses classic `regasm`. The demo's `Experimental.System.Messaging` package was not used because its API coverage could not be verified.
10. No TLS on the TCP port; bind to localhost or protect with a tunnel/VPN. `requirepass` is plain text over the wire.
