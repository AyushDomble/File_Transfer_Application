# Product overview

File Transfer is a local desktop application for sharing files between a host and
connected clients. A Python backend handles transfers; a local web interface runs
inside a desktop window. Peer traffic uses TLS. The interface is served only on
loopback and requires a per-launch session.

## Main capabilities

- Nearby-host discovery and manual address entry.
- Certificate fingerprint verification, single-use PINs, and host connection approval.
- Multiple clients on one host; one host connection per client.
- Uploads and downloads with five concurrent file transfers per peer by default.
- Per-file pause, resume, cancel, retry, and bulk actions.
- Durable transfer journals and reconnect/restart recovery.
- One host decision for each selected upload group.
- Optional ZIP packaging and configurable file rules.
- Private host/client chat, light/dark/system themes, and transfer animations.

The host is the computer offering a shared destination and file listing. The client
selects that host, sends files, or requests downloads. Discovery is a convenience,
not proof of identity. ZIP packaging is optional and received ZIPs are not extracted
automatically. Chat connects people directly; it is not an AI assistant.

See [Known limitations](../03-Validate-and-operate/KNOWN_LIMITATIONS.md) for scope and [Quick start](QUICK_START.md)
for the normal workflow.

## User journey and boundaries

```mermaid
flowchart LR
    D[Discover or enter host] --> I[Verify identity and enter PIN]
    I --> C[Host approves connection]
    C --> S[Select files and optional ZIP]
    S --> G[Host decides on group]
    G --> T[Transfer with queue controls]
    T --> V[Verify and publish completed files]
```

Connection approval admits the device; group approval admits a selected upload.
They are separate decisions. A connected host can serve multiple clients, but each
client maintains one host connection. Chat and queue controls are available within
that relationship. A completed progress display must follow successful integrity
verification, not merely the sender finishing its last write.

For reproducible behavior checks, see the subsystem commands in [Testing](../03-Validate-and-operate/TESTING.md)
and the measured local workload results in [Performance](../03-Validate-and-operate/PERFORMANCE.md).
