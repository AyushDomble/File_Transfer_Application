# Release checklist

Use this checklist for a specific artifact and environment. Do not mark unchecked
items complete merely because tests passed on another machine.

## Source and configuration

- [ ] Record the exact source inventory and dependency versions.
- [ ] Review startup defaults and intended data/state/log paths.
- [ ] Confirm the package excludes credentials, private keys, databases and test data.
- [ ] Confirm current documentation matches shipped behavior and limitations.

## Automated verification

- [ ] Full Python suite completes with no failures, errors or skipped required cases.
- [ ] Node and required browser tests pass.
- [ ] Source installation works in a fresh virtual environment; `pip check` passes.
- [ ] Source smoke passes with isolated state.
- [ ] Executable builds and passes no-window smoke.
- [ ] Extracted delivery archive passes smoke and manifest/hash verification.
- [ ] Keep results, interpreter/package inventories and archive hashes with the artifact.

## Target-device acceptance

- [ ] Native window, file dialogs and WebView2 prerequisites work on a clean target.
- [ ] Private-network firewall/discovery behavior is understood.
- [ ] Two devices complete approved uploads/downloads with verified content.
- [ ] Pause, cancellation, recovery, rejection, chat and rules behave as expected.
- [ ] Capacity, backup/restore and support procedures have an owner.
- [ ] Signing, installer and license/redistribution decisions are resolved if required.

## Current evidence boundary

Local evidence records 721 passing Python cases, 34 passing Node cases, a fresh
source smoke and successful executable/extracted-archive smoke. Native-window
acceptance on an independent clean Windows system and physical-device network
acceptance are not established by those results. See [Production deployment](PRODUCTION_DEPLOYMENT.md).

## Verify

```powershell
python tools/verify_all.py --output ..\release-verification
python -m pip check
python tools/smoke_release.py --output ..\release-source-smoke
```

Run executable and extracted-archive smoke separately using the commands in
[Build and packaging](BUILD_AND_PACKAGING.md). All output directories must be new.
Associate evidence with the exact artifact hash, interpreter/package inventory and
test environment. Do not automatically check the target-device boxes from local
results: an unsigned portable build and a signed installer have different acceptance
requirements, and only the portable build is described here.
