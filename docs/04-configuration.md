# 4. Configuration (`mcache.conf`)

Format: one `directive value` per line, `#` comments, double/single quotes allowed. Unknown directives are an error (reported as `file:line`). Validate with `mcache-server --check-config --config file`.

| Directive | Default | Meaning |
|---|---|---|
| `tcp-enabled` | yes | start the TCP gateway |
| `bind` / `port` | 127.0.0.1 / 6390 | listen address (IP literal) / port |
| `requirepass` | empty | TCP `AUTH` password (COM/MSMQ use queue ACLs instead) |
| `maxclients` | 1000 | concurrent TCP connections |
| `command-timeout-ms` | 5000 | gateway wait for the MSMQ reply |
| `queue-prefix` | mcache | prefix of all queues (`[A-Za-z0-9_-]`, 1-40) |
| `recoverable` | yes | MSMQ recoverable delivery for commands and work-queue messages |
| `request-ttl` / `reply-ttl` | 60 / 30 | MSMQ TimeToBeReceived (s) for unconsumed commands / uncollected replies |
| `max-queue-kb` | 0 | queue quota in KB, 0 = MSMQ default |
| `max-value-bytes` | 4000000 | value size limit (MSMQ message limit is 4 MB) |
| `sweep-interval` | 30 | seconds between expired-key sweeps |
| `ingress-send-account` | none | repeatable; grants `WriteMessage` on the ingress queue |
| `loglevel`, `logfile` | info, none | logging |
