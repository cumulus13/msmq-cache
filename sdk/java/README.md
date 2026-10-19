# msmqcache (Java)

Java 11+, no dependencies, blocking and thread-safe. Unchecked exceptions.
Client for **mcache-server**, the MSMQ-backed cache/queue server (Windows). This client runs anywhere and talks to the server's TCP port.

```java
import com.cumulus13.msmqcache.MsmqCacheClient;
import com.cumulus13.msmqcache.MsmqCacheClient.*;

try (MsmqCacheClient c = new MsmqCacheClient(new Options().host("127.0.0.1").port(6390))) {
    c.set("session:1", "alice", new SetOptions().ex(300));            // TTL = MSMQ TimeToBeReceived
    c.set("lock", "me", new SetOptions().nx().px(5000));
    String v = c.getString("session:1");                              // null when missing
    c.qpush("jobs", "resize:42", new QPushOptions().priority(5));
    List<Object> r = c.pipeline().set("a", "1", new SetOptions()).get("a").exec();
}
```
Exceptions: `ResponseException`, `AuthenticationException`, `CacheConnectionException`, `CacheTimeoutException`, `ProtocolException`.
`pom.xml` is provided for packaging but has **not been built** (no Maven in the authoring environment); the tests use plain `javac`.

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
`cd sdk/java && ./run-tests.sh`

---

## 👤 Author
        
[Hadi Cahyadi](mailto:cumulus13@gmail.com)
    

[![Buy Me a Coffee](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/cumulus13)

[![Donate via Ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/cumulus13)
 
[Support me on Patreon](https://www.patreon.com/cumulus13)