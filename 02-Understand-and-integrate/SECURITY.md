# Security model

The intended deployment is a local desktop application sharing with explicitly
approved peers on a trusted/private network. Authentication is required even for
loopback API access. An approved peer is not automatically trusted with arbitrary
filesystem access.

## Identity and transport

The desktop requires TLS 1.2 or newer. The host persists a self-signed EC identity
containing its private key. Clients verify the full SHA-256 certificate fingerprint
through a trusted channel and check certificate validity before transmitting PIN
or recovery credentials. Saved pins reject unexpected certificate changes.

Single-use PINs, expiry and host approval provide additional admission controls.
PIN admission is bounded to five attempts per source and sixty globally per minute;
successful attempts count too. TLS protects transport records against replay, but
does not remove the need for correct pairing and application authorization.

## Local interface

The launcher supplies a one-use bootstrap URL privately to its window. It establishes
an HttpOnly, SameSite=Strict session cookie. Host and Origin checks restrict API
calls and WebSockets; cross-site access is rejected. The local HTTP service is
loopback-only, not an HTTPS internet service. Restart invalidates the UI session.

## Files and messages

Safe-name and destination checks reject unsafe requests. Listings and downloads
exclude hidden, partial and symlink files; alternate-stream syntax and problematic
path aliases are rejected. Group approvals bind content metadata and enforce host
rules. Remote deletion is explicitly opt-in and is not per-file ownership control.
Checksums detect corruption, not malicious but correctly hashed content. Extension
rules and ZIP verification are not malware scanners.

Frames, JSON, pre-authentication traffic, connection admission and chat have bounds.
JSON rejects duplicate keys, non-finite numbers, invalid Unicode and excessive
nesting. UI text is not interpreted as HTML.

## Operator responsibilities and limits

Keep identity keys, SQLite state and received files in your own protected profile.
Windows identity files inherit directory ACLs; the app is not a substitute for OS
account isolation. Do not publish runtime state or capture bootstrap URLs/cookies
in diagnostics. Investigate fingerprint mismatches rather than clearing trust to
silence a warning; no trust-reset UI is provided.

The targeted review includes 18 adversarial cases and 4,000 seeded mutations. It
is not an independent penetration test or dependency-CVE audit. Aggregate malicious
peer resource exhaustion, real reparse-point/ACL attacks and malicious local
filesystem races have not been comprehensively certified. Local account compromise
is outside the protection boundary. See [Testing](../03-Validate-and-operate/TESTING.md) and [Limitations](../03-Validate-and-operate/KNOWN_LIMITATIONS.md).

## Control-to-test mapping

| Boundary | Required rule | Verification |
| --- | --- | --- |
| Untrusted discovery → pairing | Discovery is a hint, not authenticated identity; verify the fingerprint independently | `tests/test_security_hardening.py`, `tests/test_tls_reliability.py` |
| Remote bytes → parser | Validate framing and length before allocating/reading an announced body | `tests/test_security_adversarial.py` |
| Approved peer → filesystem | Reapply safe-name, path, policy and content constraints; approval is not arbitrary file access | `tests/test_security_adversarial.py`, `tests/test_batches.py` |
| Browser → local API | Require the authorized session and enforce Host/Origin constraints | `tests/test_security_browser.py`, `tests/test_security_hardening.py` |
| Peer text → DOM | Render as text, never executable markup | `tests/test_chat_browser.py` |
| Runtime state → distributed artifact | Exclude keys, journals, cookies and user content | Packaging manifest checks and operator review |

The targeted adversarial set contains 18 cases, including 2,000 seeded JSON
mutations and 2,000 seeded invalid frame headers. Mutation count is input count,
not 4,000 independent security guarantees. Hidden-file requests use real TLS;
the symlink rejection case uses a mocked filesystem condition and does not certify
Windows junctions, reparse points or ACL behavior.

## Verify

```powershell
python -m pytest tests/test_security_hardening.py tests/test_security_adversarial.py tests/test_security_browser.py tests/test_tls_reliability.py -q
```

Run in an isolated test directory with loopback networking available. A green run
does not establish that dependencies have no known vulnerabilities. Keep independent
review, dependency inventory/audit and physical-machine filesystem acceptance as
separate release decisions. Never weaken trust or authorization to make a test pass.
