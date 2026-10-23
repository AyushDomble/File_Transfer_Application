# Production deployment

This is a desktop peer-sharing application, not a public internet file-hosting
service. Deploy only after validating the intended Windows devices, network and
operational policies. The available Windows build is an unsigned portable candidate,
not a signed MSI installer.

## Environment preparation

Use a protected writable user profile, a functioning WebView2/pywebview environment,
and stable data/state locations. Keep the complete portable distribution together.
The locally tested platform is Windows x64 with Python 3.14.4 for source operation;
other environments require their own validation.

Allow peer TCP and discovery UDP only on the intended private network. Discovery
may not cross subnets; use explicit addressing where appropriate. Never expose the
loopback HTTP UI through a reverse proxy, port-forward, or unrestricted bind.
Establish a trusted method of exchanging certificate fingerprints.

## Acceptance before routine use

1. Run dependency checks and source or executable smoke tests.
2. Open the actual native window; test file/save dialogs, keyboard access and themes.
3. Pair two physical devices and approve a group.
4. Verify uploaded/downloaded hashes, rejection rules and cancel/pause/resume behavior.
5. Test connection interruption, restart recovery and disk-full handling safely.
6. Validate backups, private-key access, log retention and operator troubleshooting.

Budget sender staging, optional ZIP output, destination partials and retained files.
No single byte-window setting guarantees total machine memory/disk consumption.
Define a capacity and retention policy rather than relying on free-space checks alone.

Code signing, installer behavior, organizational licensing and clean-machine native
acceptance are distribution decisions/checks not established by a successful build.

## Acceptance evidence matrix

| Gate | Local evidence available | Target-device requirement |
| --- | --- | --- |
| Dependencies and startup | Fresh installation and isolated smoke passed | Repeat with the intended artifact and account |
| Services and assets | Source and executable HTTP/TLS smoke passed | Confirm firewall and writable paths |
| Native desktop | Automated browser coverage exists | Exercise actual WebView, dialogs and keyboard navigation |
| Network transfer | Real loopback TLS integration and load passed | Measure two physical devices on intended LAN/Wi-Fi |
| Recovery and safety | Journal, restart and policy regressions passed | Validate backup restoration and capacity handling safely |

## Verify

```powershell
python -m pip check
python tools/smoke_release.py --output ..\deployment-source-smoke
```

For a packaged deployment, use `--exe` as shown in [Build and packaging](BUILD_AND_PACKAGING.md).
`pip check` validates dependency compatibility, not security advisories. Never run
destructive disk-full or process-termination experiments against real user data.
