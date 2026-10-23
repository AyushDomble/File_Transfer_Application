# Testing

Verification separates unit contracts, protocol integration, browser behavior,
performance measurements and packaging smoke tests. Passing one category is not
proof that all others were exercised.

## Test layout and shared support

Collected cases remain in `tests/test_*.py`; the existing verification commands
still use those paths. Shared fixtures and support implementations live in
`tests/helpers/`, with common fixtures registered in `tests/conftest.py`. Test
modules no longer import helpers from other test modules.

For example, the native peer/HTTP fixtures live in `tests/helpers/native_peers.py`,
the Chromium page fixture in `tests/helpers/browser.py`, and the in-process ASGI
harness in `tests/helpers/asgi.py`. These helper files are support code, not test
modules to substitute into pytest commands. See the
[test organization guide](../tests/README.md) for the complete helper map.

## Run the complete local suite

From the application source directory:

```powershell
python -m pip install -r requirements-dev.txt
python -m playwright install chromium
python tools/verify_all.py --output ..\verification-run
```

The output directory must be new. Node is required for JavaScript unit tests.
The runner enables local load and upload measurements, uses real networking, and
rejects skipped Python cases. Set `PLAYWRIGHT_BROWSERS_PATH` when using a custom
Chromium cache. Allow loopback TCP/UDP and browser subprocesses in the test environment.

## Coverage and evidence

| Category | Coverage |
| --- | --- |
| Core | States, scheduler admission, frame validation, ACKs and cancellation |
| Integrity/recovery | Checksums, retries, pause/resume, connection loss, persisted restart |
| Process recovery | Terminated host process and same-ID recovery after restart |
| API/events | Local authorization, resources, snapshots, revisions and bounded subscribers |
| Browser | Queue actions, themes, chat, group approval and real local exchanges |
| Security | Certificate trust, policy checks, malformed inputs and seeded mutations |
| Load | Local soak, multi-client fairness, browser interactions and upload staging |
| Release | Startup readiness, long-path journals, source/executable smoke |

The recorded full run passed **721 Python cases in 786.13 seconds** and **34 Node
cases**, with no skipped/failed/errored Python cases. The fresh installation/build
environment separately passed smoke checks; it was not the environment used for
every full-suite case. Record package inventories with results.

The Node suite uses a minimal DOM adapter; it is not Chromium coverage. Some browser
layout tests mock HTTP, while native integration tests run real services. The
`--memory-transport` option is for restricted contract testing and must never be
reported as TCP/TLS verification. Native desktop dialogs and a clean independent
Windows machine remain separate acceptance checks.

## Verify by subsystem

| Test files | Main contract | Important boundary |
| --- | --- | --- |
| `tests/test_transfer_manager.py`, `tests/test_scheduler.py` | Legal lifecycle transitions and bounded admission | Manager correctness is not a network speed measurement |
| `tests/test_session_protocol.py`, `tests/test_chunk_ack_stress.py` | Framing, negotiation, chunk acknowledgement and integrity | Malformed input and race coverage is finite |
| `tests/test_pause_resume.py`, `tests/test_pause_flow_race.py` | Safe pause boundaries and interaction with byte windows | Timing-sensitive cases complement deterministic state tests |
| `tests/test_retry_and_persistence.py`, `tests/test_receiver_restart.py` | Durable state and receiver-authoritative offsets | Restart simulations are distinct from process termination |
| `tests/test_process_recovery.py` | Terminate/restart a host subprocess and recover | Same-machine recovery, not power-loss hardware testing |
| `tests/test_api_transfers.py`, `tests/test_transfer_events.py` | Resource actions, events, snapshots and revisions | API authorization remains required |
| `tests/test_batches.py`, `tests/test_rules_ui.py` | Group approvals, ZIP preparation and rule controls | Layout checks can use mocked HTTP |
| `tests/test_chat.py`, `tests/test_chat_browser.py` | Bidirectional messaging, retries and chat UI | Native HTTP/TCP exchange is separate from mocked visual cases |
| `tests/test_security_hardening.py`, `tests/test_security_adversarial.py`, `tests/test_security_browser.py` | Trust, malformed inputs and browser boundaries | Not a dependency vulnerability audit |
| `tests/test_local_load.py`, `tests/test_performance_upload.py` | Soak, fairness, browser load and staging measurements | Opt-in; loopback, synthetic workloads |
| `tests/test_release_launcher.py` | Launcher and release-startup contracts | Does not certify native desktop dialogs |
| `tests/queue.test.cjs` | JavaScript queue behavior using a minimal DOM | Not a substitute for Chromium |

```powershell
python -m pytest tests/test_api_transfers.py tests/test_transfer_events.py tests/test_process_recovery.py -q
node --test tests/queue.test.cjs
```

Run a focused command after changing its subsystem, then the complete runner before
release acceptance. A selected test command cannot establish that omitted modules
passed. Preserve the command, environment inventory, exit status, XML and console log.

## Recorded evidence

| Evidence | Result | Interpretation |
| --- | --- | --- |
| Full Python suite | 721 passed; 786.13 seconds; no skips/failures/errors | Recorded run, not a new execution triggered by editing this documentation |
| Node suite | 34 passed | JavaScript contract tests |
| Local persistent-session soak | 167 rounds; 835 files; 6,680 MiB over approximately 180 seconds | Aggregate bytes, not one large-file test |
| Multi-client fairness | 75 files over five equal-workload rounds | Three local clients; synthetic acknowledgement delay |
| Build/install smoke | Source, built executable and extracted executable checks passed | No-window startup and service checks |

The full-run evidence is retained in workspace artifacts `outputs/r721/python.xml`,
`outputs/r721/python.log` and `outputs/r721/node.log`. These artifacts are not bundled
with this documentation set. Load measurements and their limits are detailed in
[Performance](PERFORMANCE.md).
