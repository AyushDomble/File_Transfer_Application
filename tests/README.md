# Test organization

Run tests from the project root using `requirements-dev.txt`. Browser cases also
require Playwright Chromium; the JavaScript queue suite requires Node.

Test modules contain collected cases. Reusable fixtures and helper implementations
live in `helpers/`; no test module imports another test module. Shared application,
native-peer and browser fixtures are registered in `conftest.py`. Specialized
fixtures can also be imported explicitly by their consumers.

| Helper module | Purpose |
| --- | --- |
| `browser.py` | Chromium page fixture and static-asset root |
| `ui_mock.py`, `rules_mock.py` | Mocked HTTP/browser setup, distinct from native networking |
| `native_peers.py` | Real TLS peer pair and authenticated local HTTP server |
| `app_peers.py` | Socket-pair application harness and bounded waiting |
| `asgi.py` | In-process HTTP/WebSocket harness and polling event loop |
| `protocol.py` | Recording sessions and transfer-frame helpers |
| `persistent_peers.py` | Integrity/retry/persistence peer harness |
| `scheduler_peers.py` | Scheduler peer fixture and file/state helpers |
| `flow_peers.py` | Flow-control peer harness and chunk constants |

Similar helper names are intentionally kept in separate modules where their
setup, teardown, persistence or timing contracts differ. They are not interchangeable
merely because both helpers create a pair of peers.

## Verify

```powershell
python -m pytest --collect-only -q
python tools/verify_protocol.py --memory-transport
node --test tests/queue.test.cjs
python tools/verify_all.py --output ..\verification-test-layout
```

The complete suite collects 721 cases with the current dependencies. Shared helper
extraction preserves test bodies, decorators and parameter inputs. The memory
transport command is only a contract subset: it does not verify native TCP/TLS,
browser exchanges, discovery or physical-network performance. The complete runner
requires working local networking and browsers and rejects skipped required cases.

## Applying this folder update

Copy both `tests/` and `tools/` from the supplied ZIP into the cleaned project root,
replacing matching files and retaining `tests/helpers/`. This is not a standalone
application: the unchanged `filetransfer/`, `webapp/` and requirements files are
still required. Runtime APIs and implementation are deliberately not changed.

After applying the update, regenerate the source package and manifest with a new
output filename. The packager creates the manifest inside the ZIP; copy it back
to the working root if a current local manifest is desired.

```powershell
python tools/package_source.py --output ..\filetransfer-test-layout.zip
```

## Validation of this reorganization

- All 721 parameterized Python cases collect.
- All 530 test-function definitions retain their bodies, decorators and parameters,
  apart from import paths redirected to shared support modules.
- The in-memory protocol contract runner passed 516 cases.
- The Node queue suite passed all 34 cases.
- Browser execution timed out during Windows socket-pair creation while Playwright
  initialized its event loop, before browser assertions ran. Native browser and
  TCP/TLS coverage was not revalidated by the in-memory run.

No application/runtime files were changed. The packaging tool now includes this
Markdown test guide; diagnostic, benchmark, build and verification tools remain
available. Removing public API adapters or engine helpers is outside this folder-only
update and has not been performed.
