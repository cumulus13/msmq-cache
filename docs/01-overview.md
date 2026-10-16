# 1. Overview

MsmqCache is a networked cache and work-queue server in which **MSMQ is the engine**:

| Concern | Provided by |
|---|---|
| Ordering and serialisation of all commands | MSMQ ingress queue (one consumer) |
| Durability | MSMQ recoverable messages / transactional queue (disk backed) |
| Key expiry | MSMQ `TimeToBeReceived` on the message that holds the value |
| Atomic overwrite | MSMQ internal transaction (send new + receive old) |
| Request/reply | MSMQ `ResponseQueue` + `CorrelationId` |
| Work queues, priorities | native MSMQ queues and message priority |
| Back-pressure, quota | MSMQ queue quota (`max-queue-kb`) |
| Access control | MSMQ queue ACLs (COM / remote MSMQ), `requirepass` (TCP) |


```mermaid
flowchart LR
    subgraph Clients
        A[Any OS: mcache-cli / MsmqCache.Client / SDK] -->|TCP :6390| T
        B[Windows: VBScript / AHK / PowerShell / VB6<br/>ProgID MsmqCache.Client] -->|COM -> MSMQ send| I
    end
    subgraph Windows server
        T[TCP gateway] -->|command = MSMQ message| I[(ingress queue)]
        I --> E[Engine - single consumer]
        E <--> D[(data queue: 1 message per key,<br/>TTL = TimeToBeReceived)]
        E <--> W[(work queues QPUSH/QPOP,<br/>priority, TTL)]
        E -->|reply = MSMQ message<br/>CorrelationId| R[(reply queues)]
        R --> T
    end
    R --> B
```

[![Architecture 1](https://files.catbox.moe/06j9zb.jpg)](https://files.catbox.moe/06j9zb.jpg)


[![Architecture 2](https://files.catbox.moe/o6ztj4.jpg)](https://files.catbox.moe/o6ztj4.jpg)


## Two ingress paths, one queue
* **TCP**: `mcache-server` listens on a port. The gateway never touches the cache; it wraps each command into an MSMQ message.
* **COM**: the in-process automation object `MsmqCache.Client` sends the same message directly to the ingress queue. There is no separate COM server process; the *queue* is the COM entry point. Works locally or against a remote server's queue via MSMQ.

Both paths produce byte-identical messages (RESP-encoded command in the body), so the engine cannot tell them apart.

## Components
| Project | Target | Role |
|---|---|---|
| `MsmqCache.Core` | netstandard2.0 + net48 | protocol, config parser, MSMQ helpers (net48 only) |
| `MsmqCache.Server` | net48 | `mcache-server.exe` (console or Windows service) |
| `MsmqCache.Com` | net48 | COM automation object, ProgID `MsmqCache.Client` |
| `MsmqCache.Client` | netstandard2.0 | TCP client library, any OS |
| `MsmqCache.Cli` | net8.0 | `mcache-cli`, any OS |

## 👤 Author
        
[Hadi Cahyadi](mailto:cumulus13@gmail.com)
    

[![Buy Me a Coffee](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/cumulus13)

[![Donate via Ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/cumulus13)
 
[Support me on Patreon](https://www.patreon.com/cumulus13)