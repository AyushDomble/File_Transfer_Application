# Build and packaging

Run these commands from the application source directory. Each output path must be
new, preventing accidental replacement of existing artifacts.

## Source archive

```powershell
python tools/package_source.py --output ..\filetransfer-source.zip
```

The packager selects source, static assets, tests, tools and documentation, and
creates `SOURCE_MANIFEST.json` with SHA-256 digests. It excludes generated databases,
private identities, transfer folders, caches and runtime logs. It verifies archive
integrity and file hashes after creation.

## Windows executable

```powershell
python -m pip install -r requirements-release-lock.txt
python tools/build_release.py --output ..\windows-build
python tools/smoke_release.py --exe ..\windows-build\dist\FileTransfer\FileTransfer.exe --output ..\exe-smoke
```

The build is a PyInstaller console **onedir** distribution. Ship the entire
`FileTransfer` directory including `_internal`, not just the executable. The build
includes static web assets and explicit backend imports. It does not create an MSI
or sign the executable.

## Source smoke and artifact checks

```powershell
python tools/smoke_release.py --output ..\source-smoke
```

Smoke uses isolated runtime directories and tests startup, assets, local session
authorization and TLS host start/stop without opening a native window. Smoke the
extracted delivery ZIP as well as the build directory. Preserve dependency inventories,
test reports and hashes separately from private runtime state. SHA-256 hashes detect
artifact changes; without trusted distribution they do not authenticate the publisher.

## Build acceptance flow

```mermaid
flowchart LR
    S[Reviewed source] --> T[Full verification]
    T --> E[Isolated dependency environment]
    E --> B[Build onedir distribution]
    B --> M[Smoke built executable]
    M --> Z[Package and extract delivery archive]
    Z --> X[Smoke extracted executable]
    X --> A[Independent desktop acceptance]
```

Every gate produces separate evidence. Do not infer full-suite success from a build
exit code, or native-window usability from a no-window smoke run. Use a fresh output
directory for each build/smoke invocation and keep the entire onedir payload intact.

## Verify

```powershell
python -m pytest tests/test_release_launcher.py -q
python tools/smoke_release.py --output ..\source-smoke-check
```

The recorded source, built-executable and extracted-executable smokes passed. They
exercise actual local HTTP authorization, packaged assets and TLS host startup/
shutdown. Native WebView interaction, signing reputation and a clean independent
Windows installation remain distinct checks.
