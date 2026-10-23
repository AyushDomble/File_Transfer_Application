# Developer guide

## Source map

`filetransfer/` contains framing, sessions, host/client links, transfer engines,
scheduling, persistence, integrity, group policy and chat. `webapp/` exposes local
HTTP/WebSocket routes and desktop startup. `webapp/static/` contains the interface.
`tests/` contains Python and Node checks; `tools/` contains verification, benchmark,
packaging, build and smoke commands.

Shared test infrastructure lives in `tests/helpers/`; common fixtures are
registered in `tests/conftest.py`. Import reusable support from the helper modules,
not from `test_*.py` modules. Collected test filenames and their verification
commands are unchanged by this organization. See
[test organization and fixture ownership](../tests/README.md).

## Working rules

Keep transport framing separate from semantic transfer validation. Use the manager's
legal state transitions and scheduler admission; do not implement independent UI/API
schedulers. Scope host operations by peer and transfer. Preserve checkpoint ordering,
destination ownership, no-overwrite guarantees and certificate validation.

Do not block the ASGI event loop with hashing, disk writes or long network waits.
Keep event publication non-blocking for transfer workers. Use revisions/generations
when reconciling asynchronous state and render untrusted names/messages as text.

For a change, add a focused regression, run the affected integration tier, and then
the complete verification runner when release scope warrants it. Distinguish mocked
HTTP, in-memory sockets, real localhost TLS, Chromium and native window coverage.
Do not weaken tests or security checks to obtain a passing result.

Use fresh output directories, separate logs for independent processes and isolated
runtime data. Never package a working directory recursively without checking for
identities, journals and transferred content. See [Build and packaging](BUILD_AND_PACKAGING.md).

## Change-to-verification rules

| Changed area | Minimum targeted evidence |
| --- | --- |
| Framing or ACK semantics | Parser, chunk stress, TLS and recovery tests |
| Scheduling or windows | Admission, cancellation and pause/window races; load measurements for tuning |
| Persistence | Offset reconciliation, receiver restart and actual subprocess recovery |
| API or events | Authorization, resource-state errors, revision reconciliation and browser consumers |
| Frontend | Node contracts plus Chromium interactions; identify mocked versus native coverage |
| Packaging | Launcher regressions, isolated smoke and extracted-artifact smoke |

## Verify

```powershell
python -m pytest tests/test_session_protocol.py tests/test_flow_control.py tests/test_transfer_events.py -q
node --test tests/queue.test.cjs
python tools/verify_all.py --output ..\developer-verification
```

The first command is an example targeted subset, not a universal minimum for every
change. The final command is the complete local gate and needs a new output path.
Keep benchmark assertions separate from correctness invariants; avoid turning one
machine's measured throughput into an unsupported portable promise.
