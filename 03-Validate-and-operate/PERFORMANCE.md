# Performance

The application prioritizes integrity and recoverability. Its default transfer
profile is five active files per peer, 1 MiB chunks, an 8 MiB session byte window
and a 2 MiB per-transfer window. Each transfer waits for a chunk acknowledgement;
there is no multi-chunk pipeline within a single file.

## What affects throughput

- Round-trip latency bounds stop-and-wait throughput, especially for one large file.
- Source hashing, destination hashing and durable checkpoints require CPU and disk I/O.
- More concurrent files can hide latency but also increase disk contention.
- ZIP preparation adds local work; savings depend on content compressibility.
- Browser staging time and human approval time are distinct from peer transfer time.

The implementation uses bounded preparation, progress sampling and retained SQLite
connections while preserving durability settings. Do not trade away certificate
verification, checksums or checkpoint ordering to improve a benchmark number.

## Measurement practice

Use `tools/benchmark_transfer.py --help` for the supported benchmark arguments.
Measure repeated runs with fixed data, chunk size, concurrency and network delay.
Record end-to-end time, verified bytes, CPU/RSS, disk activity, handles, threads,
per-client completion and browser interaction latency. Keep generation/preparation,
approval and wire timing clearly separated.

Local tests cover a three-minute soak, multiple clients and browser interactions
during transfers. These are not LAN/Wi-Fi bandwidth guarantees. A 10 GiB transfer,
cold-cache storage, long endurance and physical-device network measurements remain
unverified. Passing a responsiveness guardrail does not promise identical latency
under every background workload.

## Recorded local measurements

These values come from test properties in `outputs/r721/python.xml`, the passing
full-suite run described in [Testing](TESTING.md). They are observations from one
machine, not release service-level objectives or independent LAN measurements.

| Workload | Measurement | Scope |
| --- | --- | --- |
| Persistent pinned-TLS session; five 8 MiB files per round | 167 rounds, 835 files, 6,680 MiB; last sample at 180.07 s | Each destination independently hashed; test destinations removed after each round |
| Soak process RSS | First sample 191,057,920 bytes; last/max sampled 208,629,760 bytes | Samples after rounds; process already ran other suite cases |
| Soak resources | 925 handles at first/last sample; 13 to 15 threads; maximum sampled 15 | Not continuous resource tracing |
| Soak retained transfer records | 5 to 835 | History retention contributes to growth; these samples do not prove a leak or prove absence of leaks |
| Three clients, each five 16 MiB files; five rounds; 10 ms synthetic ACK delay | Client completion times 1.722–3.655 s; largest within-round spread 61.43 ms | Equal workloads, approximately 20 ms completion sampling |
| Same fairness workload | Jain completion-rate index 0.999951–0.999977 | Very even observed local completion, not arbitrary network fairness |
| Client browser under five 128 MiB uploads, 50 ms ACK delay | Action latency median 306.85 ms, max 680.05 ms | Ten theme/refresh actions in authenticated Chromium |
| Host browser under the same upload workload | Action latency median 384.38 ms, max 810.90 ms | Separate host-browser case |
| Browser animation frame intervals | Client p95 16.7 ms, max 166.6 ms; host p95 16.7 ms, max 16.8 ms | Empirical p95 uses the lower ordered sample at floor(0.95 × (n−1)) |
| Browser long tasks | Client 1; host 0 | Observed during each instrumented interval, not lifetime totals |

The browser cases assert no observed page errors and enforce a five-second action
guardrail. Passing that ceiling is not a promise of sub-second response on every
machine. Frame intervals, action durations and startup timing are different metrics;
do not combine them into a single latency score.

## Interpreting throughput and fairness

For one stop-and-wait transfer, an approximate upper bound is
`chunk bytes / (network round trip + receiver checkpoint time + other per-chunk work)`.
Several concurrent files can overlap waiting time, but share storage, CPU and the
session socket. Increasing a byte window does not itself introduce pipelining.

For equal-size jobs, the reported fairness index uses completion rates `x_i`:
`J = (sum(x_i))² / (n × sum(x_i²))`. A value near one indicates similar rates for
that workload. It says nothing about bandwidth allocation to unequal jobs or
malicious peers. Report per-client completion times alongside the index.

## Verify

```powershell
$env:FT_LOCAL_LOAD='1'
$env:FT_BENCHMARK_UPLOAD='1'
python -m pytest tests/test_local_load.py tests/test_performance_upload.py tests/test_performance.py -q -o junit_family=xunit1 --junitxml=local-performance.xml
Remove-Item Env:FT_LOCAL_LOAD
Remove-Item Env:FT_BENCHMARK_UPLOAD
python tools/benchmark_transfer.py --help
```

Use a new report filename for each measurement. These tests enable real local
networking and browser subprocesses. Record data compressibility, storage type,
free space, background activity and whether caches were warm. ZIP preparation
benchmarks omit human decision time; a faster staged upload is not necessarily a
faster end-to-end transfer. Repeated trials and independent machines are required
before making broader performance claims.
