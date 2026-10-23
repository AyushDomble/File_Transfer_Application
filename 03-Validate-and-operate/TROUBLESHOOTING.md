# Troubleshooting

| Symptom | Check and safe response |
| --- | --- |
| No nearby hosts | Confirm hosting is active, network isolation/VPN settings and UDP firewall permission; try manual address entry |
| Fingerprint mismatch | Stop pairing; verify the host identity through a trusted channel. Do not disable verification |
| PIN rejected | Obtain the current PIN; account for use/expiry and throttling before retrying |
| Waiting for connection | Check the host's pending connection approvals |
| Files waiting after connection | Check group approval; connection approval and selection approval are separate |
| Group rejected | Inspect extensions, per-file/container sizes, free space and host decision; acknowledge and reselect when resolved |
| Local API returns 403 | Launch through the desktop launcher; a direct browser address alone does not establish the UI session |
| Port unavailable | Check configured ports and other processes; do not terminate unrelated applications blindly |
| SQLite cannot open | Check writable state paths, permissions, capacity and path length; preserve existing state |
| Queue says connection lost | Reconnect to the trusted endpoint and allow recovery to reconcile offsets |
| Checksums fail | Check source changes and storage health; keep integrity validation enabled |
| ZIP preparation stalls | Check CPU/disk load and free space for originals plus archive |
| EXE cannot find assets/modules | Re-extract the entire distribution with `_internal` intact |
| No native window | Verify pywebview/WebView2 prerequisites and inspect launcher logs |
| Slow interactions | Separate background disk/CPU load, network delay and UI timing; capture repeated measurements |

## Collect useful evidence

Record exact action, filename/size if safe, transfer ID, state, timestamps and the
error text. Include whether it occurs in source, executable, browser or native
window. Reproduce with a harmless small file when possible. Never send private
keys, database recovery secrets, cookies, bootstrap URLs or PINs with a bug report.

Use [Testing](TESTING.md) for repeatable checks rather than deleting all application
state as a first troubleshooting step.

## Verify a suspected subsystem

```powershell
python -m pytest tests/test_tls_reliability.py tests/test_reconnection.py -q
python -m pytest tests/test_batches.py tests/test_cleanup_browser.py -q
```

The first command checks transport/reconnect behavior; the second checks grouped
approval and UI cleanup. Run from a source checkout in an isolated test environment,
not by deleting live application state. A passing regression does not invalidate an
observed user failure: retain the exact reproduction, environment and sanitized logs.

For slowness, separate preparation, approval wait, wire transfer and final verification.
Record whether the issue is one large file or many small files, and compare repeated
runs with the same content. See [Performance](PERFORMANCE.md) for measured baselines
and the distinction between local results and physical-network behavior.
