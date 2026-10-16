# Client SDKs

All SDKs speak the TCP protocol described in [`docs/08-client-protocol-spec.md`](../docs/08-client-protocol-spec.md)
and share the same command surface and error model.

| Language | Status | Path |
|---|---|---|
| Python (sync + asyncio) | done, tested against a fake server only | `sdk/python` |
| C# / .NET (netstandard2.0) | done (`MsmqCache.Client`), never compiled | `src/MsmqCache.Client` |
| COM (Windows: VBScript, AutoHotkey, PowerShell, VB6) | done, never compiled | `src/MsmqCache.Com` |
| Go | planned | |
| Node.js / TypeScript | planned | |
| Rust | planned | |
| Java / Kotlin | planned | |
| PHP, Ruby, others | planned | |

Rules for every SDK: zero runtime dependencies, binary-safe values, client-side key/value limits, no automatic retry of
a failed write, close the connection after a timeout, pipelines, and a test suite that runs against a fake RESP server.
