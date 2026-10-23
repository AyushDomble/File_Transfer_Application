# Known limitations

## Product and transport

- One client connects to one host at a time; a host can serve multiple clients.
- Each transfer has one outstanding chunk. There is no within-file data pipeline.
- ZIP packaging is disk-backed; received archives are not automatically extracted.
- Existing selected ZIP files are not recursively inspected as generated groups.
- Incomplete browser staging is not restart-durable.
- Chat is human messaging with bounded in-memory history, not durable messaging or AI.
- Event delivery synchronizes current state; it is not a persistent replay/audit log.
- A certificate trust-reset UI is not provided.
- File extension rules are not malware or content-type verification.

## Verification and assurance

Native desktop window/file-dialog behavior on an independent clean Windows system
and physical-device LAN/Wi-Fi acceptance remain unverified. Source and executable
smoke tests run without a window. A clean dependency install plus `pip check` is
not a vulnerability scan.

The security review is targeted, not an independent penetration test. Aggregate
authenticated-peer resource pressure, malicious local filesystem races, Windows
reparse-point/ACL attacks and broad stateful fuzzing need deeper assurance work.
The symlink admission regression uses a mocked link check rather than proving all
Windows reparse-point behavior.

Local load measurements do not certify 10 GiB transfers, cold-cache behavior,
long endurance or real Wi-Fi throughput. UI latency can vary with system load.
The portable executable is unsigned and is not an installer. These limits should
be considered during deployment acceptance, not hidden by successful unit tests.

## Reading evidence correctly

| Available result | Does not establish |
| --- | --- |
| 6,680 MiB across 835 soak files | A single 10 GiB transfer or long-duration endurance |
| Three equal-workload local clients complete evenly | Fair bandwidth allocation across unequal physical networks |
| Browser frame/action measurements | Native-window behavior on every Windows device |
| Seeded malformed-input tests | Exhaustive stateful fuzzing or independent security certification |
| Fresh installation and smoke | Dependency vulnerability clearance or signed publisher identity |

Use the focused commands in [Testing](TESTING.md) to reproduce available contracts.
Closing an assurance gap requires evidence for that gap, not relabeling an existing
unit test or reusing a benchmark with different workload assumptions.
