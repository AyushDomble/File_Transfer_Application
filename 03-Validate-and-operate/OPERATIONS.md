# Operations, backup and recovery

## Routine checks

Monitor available disk space, failed/recoverable queue entries, logs and peer
connections. Verify that data and state paths are stable across launches. Keep
source files available while transfers may require retries. Restrict the shared
directory to content intentionally offered to approved peers.

The default log is `filetransfer.log`, with a 2,000,000-byte rotation threshold
and three backups. Use distinct log paths for independently running instances or
test processes to avoid Windows rotation conflicts. Review diagnostics before
sharing them: filenames, paths and peer metadata can be sensitive even without PINs.

## Back up safely

1. Stop transfers and close the application cleanly.
2. Back up role data, `.ft-state` directories, settings and the host TLS identity
   together. SQLite WAL-associated state must not be separated from a live database.
3. Store the backup in protected storage; identity files contain private keys.
4. Test restoration into an isolated environment before depending on it.

Restore matching files and state while the app is closed, then reconnect using
normal fingerprint, PIN and host approval. Do not clear trust records merely to
make a changed certificate warning disappear.

## Cleanup

UI refresh/dismissal is not data deletion. Do not remove scratch files, journal
directories or `.part` files while jobs are active or recovery is expected. Chat
history is not included in durable transfer backups because it is retained in memory.
Retain only the diagnostics and completed files needed by the operator's policy.

## Operational decision rules

| Situation | Action | Preserve |
| --- | --- | --- |
| Connection lost | Reconnect to the verified endpoint and reconcile | Source bytes, partial destination and journal |
| Certificate unexpectedly changed | Stop and verify with the host operator | Existing trust records and diagnostics |
| Capacity low | Stop admitting new work; arrange safe capacity recovery | Active scratch files and recoverable state |
| Restore required | Close the app, restore a consistent backup, test in isolation | Matching data, state and identity |
| UI activity needs clearing | Use supported refresh/dismiss behavior | Completed files and durable recovery records |

## Verify

```powershell
python -m pytest tests/test_retry_and_persistence.py tests/test_receiver_restart.py tests/test_process_recovery.py tests/test_refresh_browser.py -q
```

These verify recovery and presentation contracts in test fixtures. They do not
validate your actual backup media or retention policy; perform an isolated restore
exercise and compare file hashes before depending on a backup.
