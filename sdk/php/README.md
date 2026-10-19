# msmqcache (PHP)

PHP 8.1+, no extensions or packages needed (streams only).
Client for **mcache-server**, the MSMQ-backed cache/queue server (Windows). This client runs anywhere and talks to the server's TCP port.

```bash
composer require cumulus13/msmqcache      # not on Packagist yet; use a path repository for now
```
```php
use Cumulus13\MsmqCache\Client;

$c = new Client('127.0.0.1', 6390);
$c->set('session:1', 'alice', ex: 300);                 // TTL = MSMQ TimeToBeReceived
$c->set('lock', 'me', nx: true, px: 5000);
$v = $c->get('session:1');                              // null when missing
$c->qpush('jobs', 'resize:42', priority: 5);
$res = $c->pipeline()->set('a', 1)->get('a')->exec();
```
Exceptions: `ResponseException`, `AuthenticationException`, `ConnectionException`, `TimeoutException`, `ProtocolException`.

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
`cd sdk/php && ../conformance/run.sh php tests/conformance.php`
