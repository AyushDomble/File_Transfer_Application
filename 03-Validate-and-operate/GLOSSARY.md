# Glossary

| Term | Meaning |
| --- | --- |
| Host | Computer accepting approved peer connections and offering shared files |
| Client | Computer connected to a host for upload/download and chat |
| Peer | Authenticated participant associated with a connection and scoped state |
| Session | Connection-level protocol context carrying multiple operations |
| Transfer | One identified file operation with metadata, progress and lifecycle |
| Group/batch | One selected set of files submitted for a single host decision |
| Staging | Local disk storage of browser-selected bytes before peer transfer |
| Partial file | Transfer-owned unfinished destination content |
| Checkpoint | Receiver-confirmed progress retained for recovery |
| ACK | Acknowledgement of validated receiver progress |
| Stop-and-wait | Sending one chunk and waiting for its acknowledgement before the next |
| Byte window | Admitted in-flight byte capacity, distinct from active-file slots |
| Multiplexing | Several transfers sharing one framed peer connection |
| Fingerprint | SHA-256 digest used to verify the exact host certificate |
| Certificate pin | Stored expected certificate identity for a host endpoint |
| Bootstrap | One-use local launch capability establishing the UI session |
| Revision | Increasing state/progress version for a transfer object |
| Generation | Identity distinguishing reconstructed objects with the same transfer ID |
| Snapshot | Authoritative set of current resources used to synchronize the UI |
| WAL | SQLite write-ahead log associated with durable database activity |
| Smoke test | Narrow startup/integration check, not complete product certification |
| Onedir | Executable distribution requiring its accompanying dependency directory |
| Release candidate | Artifact prepared for acceptance testing, with stated verification boundaries |

Related guides: [Architecture](../02-Understand-and-integrate/ARCHITECTURE.md), [Protocol](../02-Understand-and-integrate/PROTOCOL.md),
[Security](../02-Understand-and-integrate/SECURITY.md), [Testing](TESTING.md).

## Measurement vocabulary

| Term | Meaning and reporting rule |
| --- | --- |
| MiB / GiB | Binary units: 1 MiB = 1,048,576 bytes; 1 GiB = 1,024 MiB |
| RSS | Resident process memory; a sample is not necessarily the true peak |
| Throughput | Verified bytes divided by a clearly defined elapsed interval |
| Action latency | Elapsed time for an instrumented UI action; distinct from frame interval |
| p95 | A sample percentile; report the calculation method and workload |
| Long task | Browser main-thread task reported by long-task instrumentation, generally at least 50 ms |
| Jain index | Relative rate-equality measure; near one means similar measured rates |
| Warm cache | Data may already reside in OS/storage caches, affecting repeated reads |
| Seeded mutation | Reproducible generated input variation; finite coverage, not exhaustive fuzzing |
| Native integration | Actual participating services/transports rather than a mocked HTTP contract |
