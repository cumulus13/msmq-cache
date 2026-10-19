# Client SDKs

All SDKs speak the TCP protocol in [`docs/08-client-protocol-spec.md`](../docs/08-client-protocol-spec.md) and share the same
command surface and error model. Every SDK is zero-dependency and is tested against the same checklist
(`conformance/CHECKLIST.md`, 13 tests) using `conformance/fake_server.py`.

| Language | Path | Status |
|---|---|---|
| Python (sync + asyncio) | `sdk/python` | done, 17 tests (own suite) |
| TypeScript / Node.js | `sdk/node` | done, 13/13 conformance |
| Go | `sdk/go` | done, 13/13 conformance |
| Rust (blocking) | `sdk/rust` | done, 13/13 conformance |
| Java 11+ | `sdk/java` | done, 13/13 conformance (`pom.xml` not built) |
| PHP 8.1+ | `sdk/php` | done, 13/13 conformance |
| Ruby 3.0+ | `sdk/ruby` | done, 13/13 conformance |
| C# / .NET (netstandard2.0) | `src/MsmqCache.Client` | done |
| COM (VBScript, AutoHotkey, PowerShell, VB6) | `src/MsmqCache.Com` | done |
| Kotlin, Swift, Dart, Elixir, C/C++, Lua, Perl | | planned |

**What "tested" means:** the fake server reproduces the wire format (framing, nil/error replies, auth, a slow-reply hook and a
drop-connection hook). It is not the real server and knows nothing about MSMQ, so a mismatch between a real mcache-server and
the SDKs is still possible. Run `scripts`/CI against a real server before relying on any SDK in production.

Run any suite: `cd sdk/<lang> && ../conformance/run.sh <test command>` (see each README; needs `python3` for the fake server).

Rules every SDK follows: zero runtime dependencies, binary-safe values, client-side key/value limits, no automatic retry of a
failed command, close the connection after a timeout, pipelines, `AUTH` errors distinct from other errors.
