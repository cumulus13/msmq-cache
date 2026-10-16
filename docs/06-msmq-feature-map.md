# 6. MSMQ feature map

| MSMQ feature | Used? | How |
|---|---|---|
| Private queues, `DIRECT=OS:` format names | yes | all queues; works in workgroup mode and remotely |
| Recoverable (disk) vs express delivery | yes | `recoverable` for commands and work queues |
| Transactional queues + internal transactions | yes | data queue: atomic SET (send + receive in one tx) |
| Non-transactional queues | yes | ingress, replies, work queues. **Transactional queues ignore message priority**, hence work queues are non-transactional |
| `TimeToBeReceived` | yes | key TTL, command TTL, reply TTL |
| Message priority 0-7 | yes | `QPUSH ... PRIORITY n` |
| `ResponseQueue`, `CorrelationId` | yes | request/reply for TCP and COM |
| `PeekByLookupId` / `ReceiveByLookupId` | yes | O(1)-style access by key through the index; `Last` finds the message just written |
| `ReceiveByCorrelationId` | yes | COM replies on a shared queue |
| Message enumerator | yes | index rebuild at start, `QLEN` |
| Queue quota (`MaximumQueueSize`) | yes | `max-queue-kb` |
| Queue ACLs | yes | `ingress-send-account` |
| Message `Extension` (16 KB) | yes | key + expiry metadata |
| 4 MB message size limit | yes | `max-value-bytes` upper bound |
| Dead-letter / journal queues | no (disabled per message) | could be enabled for audit; roadmap |
| Acknowledgements (positive/negative) | no | roadmap |
| Authentication / encryption of messages | no | roadmap (`PrivacyLevel`, certificates) |
| MSMQ Triggers, HTTP/multicast, routing service | no | out of scope |
| External (MSDTC) transactions | no | internal transactions only |

Facts verified against Microsoft documentation while building this: `PeekByLookupId(MessageLookupAction, long)` with `Last` and lookupId 0; 4 MB per message limit; priority ignored for transactional messages. Everything else in this table is from the author's knowledge of MSMQ and must be validated on a real MSMQ host (see limitations).
