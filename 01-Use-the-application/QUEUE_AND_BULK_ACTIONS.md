# Queue and bulk actions

The client queue represents both uploads and downloads. Rows show filename,
direction, state, progress and available actions. Host activity groups transfers
by peer. Lists can collapse and queues scroll to avoid occupying the entire page.

| Action | Meaning |
| --- | --- |
| Pause | Request a safe stopping point; PAUSING is not yet PAUSED |
| Resume | Request scheduler admission from a confirmed checkpoint |
| Cancel | Stop that transfer without closing the session |
| Retry | Start a new attempt for an eligible failed transfer |
| Recover | Reattach eligible saved work to an authenticated session |

Bulk pause/resume/cancel/retry applies to eligible transfers; it is not an instruction
to force every row into the same state. Controls use authoritative backend responses
and display errors instead of assuming success. Cancelled rows clear automatically
according to the interface's cleanup behavior; clearing a row is not remote deletion.

Refresh updates displayed state. It does not establish a new peer connection,
restart the application, erase journals, or remove received files. A connection
loss leaves recoverable rows rather than claiming successful completion.

Completion animations and progress indicators are visual feedback; the backend's
verified terminal state remains the source of truth.

## Action rules

| UI condition | Valid interpretation |
| --- | --- |
| Pause requested | Await PAUSED before treating bytes as stopped |
| Resume pending admission | Intent recorded; not yet evidence of active transfer |
| Cancel all | Apply to eligible jobs; preserve unrelated session health |
| Row disappears | Display cleanup; not deletion of committed content |
| Progress reaches 100% | Wait for COMPLETED if verification/publication is pending |
| Live updates reconnect | Apply snapshot and revisions rather than append duplicates |

## Verify

```powershell
python -m pytest tests/test_queue_browser.py tests/test_cleanup_browser.py tests/test_refresh_browser.py -q
node --test tests/queue.test.cjs
python -m pytest -q -k "cancel_all or pause_all or resume_all or retry_all"
```

Coverage includes stale progress/action replies, safe filenames, cancelled-row
cleanup, refresh behavior and selected bulk-action backend contracts. The Node
tests use a DOM adapter; Chromium tests exercise real DOM behavior, with HTTP
mocking where explicitly defined by each fixture. They do not all send real files.
