# 2. Architecture

## 2.1 Components and queues
```mermaid
flowchart TB
    subgraph TCPClients[TCP clients - any OS]
      C1[mcache-cli]; C2[MsmqCache.Client]; C3[telnet / redis-cli style]
    end
    subgraph COMClients[COM clients - Windows]
      V[VBScript]; H[AutoHotkey]; P[PowerShell]
    end
    C1 & C2 & C3 -->|RESP over TCP| G[TCP gateway]
    V & H & P -->|MsmqCache.Client COM| ZR[MsmqRequester]
    G -->|Send, AppSpecific=ticket| IQ[(mcache.ingress<br/>non-transactional, recoverable)]
    ZR -->|Send, ResponseQueue| IQ
    IQ -->|Receive| EN[Engine thread]
    EN --> CP[CommandProcessor]
    CP --> CS[CacheStore]
    CP --> WQ[WorkQueues]
    CS <--> DQ[(mcache.data<br/>transactional)]
    WQ <--> QQ[(mcache.q.NAME<br/>non-transactional, priority)]
    EN -->|reply: CorrelationId = request Id| RT[(mcache.reply.tcp)]
    EN -->|reply| RC[(mcache.reply.com)]
    RT --> G
    RC -->|ReceiveByCorrelationId| ZR
```

## 2.2 Sequence: TCP
```mermaid
sequenceDiagram
    participant C as TCP client
    participant G as TCP gateway
    participant I as ingress queue
    participant E as Engine
    participant D as data queue
    participant R as reply.tcp queue
    C->>G: SET k v EX 60
    G->>G: ticket = N, register pending[N]
    G->>I: Send(body=RESP, ResponseQueue=reply.tcp, AppSpecific=N)
    I->>E: Receive()
    E->>D: tx: Send(new) + ReceiveByLookupId(old)
    E->>R: Send(reply, CorrelationId=Id, AppSpecific=N, TTL=reply-ttl)
    R->>G: pump thread Receive()
    G->>C: +OK
```

## 2.3 Sequence: COM
```mermaid
sequenceDiagram
    participant S as Script (COM)
    participant Z as MsmqCache.Client
    participant I as ingress queue
    participant E as Engine
    participant R as reply.com queue
    S->>Z: Get("k")
    Z->>I: Send(body=RESP, ResponseQueue=reply.com)
    I->>E: Receive()
    E->>E: index lookup, PeekByLookupId on data queue
    E->>R: Send(reply, CorrelationId=request Id)
    Z->>R: ReceiveByCorrelationId(Id, timeout)
    R-->>Z: reply
    Z-->>S: value (Empty = nil)
```

## 2.4 Data model (one MSMQ message per key)
```mermaid
flowchart LR
    K[key] --> M[MSMQ message in data queue]
    M --> B[Body = value bytes]
    M --> X[Extension = expireTicks + key UTF-8]
    M --> L[Label = key, first 200 chars]
    M --> T[TimeToBeReceived = TTL, MSMQ deletes it]
    IDX[in-memory index<br/>key to LookupId + expireTicks] -. rebuilt at start by enumerating queue .-> M
```

## 2.5 SET
```mermaid
flowchart TD
    A[SET key value] --> B{key live in index?}
    B -- NX and yes / XX and no --> N[reply nil]
    B -- ok --> C[begin MSMQ transaction]
    C --> D[Send new message]
    D --> E{had old?}
    E -- yes --> F[ReceiveByLookupId old]
    E -- no --> G
    F --> G[Commit]
    G --> H[PeekByLookupId Last -> new LookupId]
    H --> I[update index] --> J[reply +OK]
    D -. error .-> X[Abort -> reply -ERR]
```

## 2.6 Startup recovery
```mermaid
flowchart TD
    S[start] --> Q[open or create queues, check Transactional flag]
    Q --> EN[enumerate data queue in arrival order]
    EN --> P{expired by ticks?}
    P -- yes --> RM[remove]
    P -- no --> DUP{key already indexed?}
    DUP -- yes --> RO[remove older, newer wins]
    DUP -- no --> AD[add to index]
    RM & RO & AD --> RUN[start engine, then TCP gateway]
```

## 2.7 Concurrency model
One engine thread consumes the ingress queue, so all commands are executed in MSMQ arrival order (linearisable, like Redis' single thread). The index is touched only by that thread. TCP clients are handled asynchronously; each connection has at most one command in flight (replies keep request order).
