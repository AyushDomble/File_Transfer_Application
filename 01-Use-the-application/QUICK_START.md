# Quick start

## Install and launch

For source use, the locally verified runtime is Windows x64 with Python 3.14.4.
From an extracted application source directory:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-release-lock.txt
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m webapp.main
```

The lock file is the tested runtime/build dependency snapshot, not a vulnerability
audit. The native window requires a functioning pywebview backend and WebView2.
For a portable Windows distribution, extract the complete directory and run
`FileTransfer.exe`; keep `_internal` alongside it.

## Share a selection

1. Open Host on the receiving computer, choose its rules, and start hosting.
2. On the sending computer, discover/select the host or enter its address.
3. Verify the full certificate fingerprint through a trusted channel, enter the
   host's PIN, and wait for connection approval.
4. Optionally select ZIP packaging before choosing or dropping files.
5. On the host, inspect the selection and choose **Receive group**.
6. Monitor the queue until completion. A completed file has passed integrity checks.

Use a writable folder in your own profile. Keep both apps open during preparation
and transfer. Do not expose the local HTTP interface to other computers.

## Verify

Before using personal files, run a no-window installation check from the source root:

```powershell
.\.venv\Scripts\python.exe tools/smoke_release.py --output ..\quickstart-smoke
```

Use a new output directory. This check creates isolated runtime state and validates
HTTP startup, bundled assets, session controls and TLS host start/stop. It does not
open the native window. Then manually send a harmless small file between intended
devices and compare its contents before relying on the installation.
