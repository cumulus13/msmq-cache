# SDK conformance checklist

Every SDK test suite runs these against `fake_server.py` (started by `run.sh`). Names are the test names used in each language.

1. `ping`                      PING returns true / PONG
2. `set_get_delete`            round trip; GET of a deleted key is nil
3. `binary_safe`              value containing \r\n, NUL, `$`, `*` round-trips byte for byte
4. `nil_vs_empty`             empty string value is distinguishable from nil
5. `set_options`              EX/PX/NX/XX are encoded as `SET k v EX 30 NX` (checked with the LASTCMD hook); NX on existing key returns false
6. `numbers_and_multi`        INCR / INCRBY / MGET (with nil) / MSET / KEYS
7. `server_error`             `-ERR` becomes an error, and the connection stays usable
8. `auth`                     right password works; wrong password and missing password raise an auth error (not a generic one)
9. `timeout_reconnect`        SLOW 1 with a 300 ms timeout raises a timeout error; the next call works on a fresh connection
10. `dropped_connection`      DROP raises a connection error; the next call works
11. `pipeline`                several commands in order, and a single-command pipeline still returns a list
12. `queues`                  QPUSH with priority/ttl, QPOP, QPOP on empty returns nil
