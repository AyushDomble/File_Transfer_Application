"""
Transfer-resource REST endpoint contracts
(/api/client/transfers/...) that expose ConcurrentScheduler/
TransferManager directly -- create (upload/download), pause, resume,
cancel, retry, get and list. Uses
TestClient against the real webapp.api app.
"""

import os
import shutil
import threading
import time

import pytest

os.environ.setdefault("FT_PORT", "5680")
os.environ.setdefault("FT_SERVER_DIR", "api_tx_server_files")
os.environ.setdefault("FT_CLIENT_DIR", "api_tx_client_files")
os.environ.setdefault("FT_PENDING_TIMEOUT", "5")
os.environ.setdefault("FT_PIN_EXPIRY_SECONDS", "0")

from filetransfer import config


@pytest.fixture()
def api_client(tmp_path, monkeypatch):
    # Note: filetransfer.config reads FT_SERVER_DIR/FT_CLIENT_DIR (and
    # everything else) at MODULE IMPORT time as plain constants, and
    # `filetransfer.config` may already be imported by the time
    # this fixture runs -- so setting os.environ here again has no
    # effect on it. Set both the cached constants and environment here.
    # Durable journals must not leak into the next test. Windows may retain
    # an open SQLite WAL anchor, so deleting/reusing a fixed folder is unsafe.
    for name in ('SERVER_DIR', 'CLIENT_DIR'):
        directory = str(tmp_path / name.lower())
        monkeypatch.setenv('FT_' + name, directory)
        monkeypatch.setattr(config, name, directory)
    monkeypatch.setattr(config, 'STATE_DIR', None)
    import sys as _sys
    for mod in ("webapp.api", "webapp"):
        _sys.modules.pop(mod, None)
    from fastapi.testclient import TestClient
    from webapp import api as api_module
    # Used as a context manager (not just constructed) so Starlette
    # actually runs the app's startup/shutdown lifespan -- needed for
    # _main_loop to get set, which every WebSocket broadcast depends on.
    from authenticated_client import authenticated_client
    # These exercise low-level resource operations; group approval is tested
    # separately through the authenticated batch API.
    api_module.transfer_host.require_batch_approval = False
    with authenticated_client(api_module) as tc:
        tc.post("/api/host/start")
        yield tc, api_module
        tc.post("/api/client/disconnect")
        tc.post("/api/host/stop")


def _connect(tc):
    info = tc.get("/api/host/info").json()

    def approve():
        for _ in range(100):
            p = tc.get("/api/host/pending").json()["pending"]
            if p:
                tc.post(f"/api/host/accept/{p[0]['id']}")
                return
            time.sleep(0.05)
    threading.Thread(target=approve, daemon=True).start()
    r = tc.post("/api/client/connect", json={"host": "127.0.0.1", "pin": info["pin"],
                "fingerprint": info["certificate_fingerprint"]})
    assert r.json()["ok"] is True
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if tc.get('/api/host/peers').json()['peers']:
            break
        time.sleep(0.01)
    assert tc.get('/api/host/peers').json()['peers']


def _wait_terminal(tc, transfer_id, timeout=15):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        r = tc.get(f"/api/client/transfers/{transfer_id}").json()
        last = r
        if r["ok"] and r["transfer"]["state"] in ("completed", "failed", "cancelled"):
            return r["transfer"]
        time.sleep(0.05)
    raise AssertionError(f"never reached terminal state: {last}")


def test_create_upload_transfer_and_poll_to_completion(api_client, tmp_path):
    tc, _ = api_client
    _connect(tc)
    src = tmp_path / "up.bin"
    src.write_bytes(os.urandom(300_000))

    r = tc.post("/api/client/transfers/upload", json={"local_path": str(src)})
    body = r.json()
    assert body["ok"] is True
    tid = body["transfer"]["id"]
    assert body["transfer"]["state"] in ("queued", "active")

    final = _wait_terminal(tc, tid)
    assert final["state"] == "completed", final
    dest = os.path.join(os.environ["FT_SERVER_DIR"], "up.bin")
    assert open(dest, "rb").read() == src.read_bytes()


def test_create_upload_transfer_missing_local_file(api_client, tmp_path):
    tc, _ = api_client
    _connect(tc)
    r = tc.post("/api/client/transfers/upload", json={"local_path": str(tmp_path / "nope.bin")})
    assert r.json() == {"ok": False, "error": f"local file not found: {tmp_path / 'nope.bin'}"}


def test_create_transfer_without_connection(api_client):
    tc, _ = api_client
    r = tc.post("/api/client/transfers/upload", json={"local_path": "/tmp/whatever"})
    body = r.json()
    assert not body['ok'] and body['disconnected'] and not body['recoverable']
    assert 'not connected' in body['error']


def test_download_transfer_round_trip(api_client, tmp_path):
    tc, _ = api_client
    _connect(tc)
    src = tmp_path / "srv.bin"
    src.write_bytes(os.urandom(200_000))
    up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
    _wait_terminal(tc, up["transfer"]["id"])

    r = tc.post("/api/client/transfers/download", json={"filename": "srv.bin"})
    body = r.json()
    assert body["ok"] is True
    final = _wait_terminal(tc, body["transfer"]["id"])
    assert final["state"] == "completed"
    got = os.path.join(os.environ["FT_CLIENT_DIR"], "srv.bin")
    assert open(got, "rb").read() == src.read_bytes()


def test_list_and_get_transfers(api_client, tmp_path):
    tc, _ = api_client
    assert tc.get("/api/client/transfers").json() == {"ok": True, "transfers": []}
    _connect(tc)
    src = tmp_path / "l.bin"
    src.write_bytes(os.urandom(10_000))
    up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
    tid = up["transfer"]["id"]
    _wait_terminal(tc, tid)

    listing = tc.get("/api/client/transfers").json()
    assert listing["ok"] is True
    assert any(t["id"] == tid for t in listing["transfers"])

    got = tc.get(f"/api/client/transfers/{tid}").json()
    assert got["ok"] is True and got["transfer"]["id"] == tid

    missing = tc.get("/api/client/transfers/does-not-exist").json()
    assert missing == {"ok": False, "error": "no such transfer"}


def test_pause_and_resume_transfer(api_client, tmp_path):
    tc, _ = api_client
    _connect(tc)
    src = tmp_path / "pr.bin"
    src.write_bytes(os.urandom(6_000_000))
    up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
    tid = up["transfer"]["id"]

    deadline = time.time() + 10
    while time.time() < deadline:
        st = tc.get(f"/api/client/transfers/{tid}").json()["transfer"]
        if st["offset"] > 0:
            break
        time.sleep(0.01)
    else:
        pytest.fail("transfer never made progress")

    r = tc.post(f"/api/client/transfers/{tid}/pause")
    assert r.json()["ok"] is True

    deadline = time.time() + 5
    while time.time() < deadline:
        st = tc.get(f"/api/client/transfers/{tid}").json()["transfer"]["state"]
        if st == "paused":
            break
        time.sleep(0.02)
    assert st == "paused", st

    r = tc.post(f"/api/client/transfers/{tid}/resume")
    assert r.json()["ok"] is True
    final = _wait_terminal(tc, tid, timeout=20)
    assert final["state"] == "completed", final
    dest = os.path.join(os.environ["FT_SERVER_DIR"], "pr.bin")
    assert open(dest, "rb").read() == src.read_bytes()


def test_cancel_transfer(api_client, tmp_path):
    tc, _ = api_client
    _connect(tc)
    src = tmp_path / "cx.bin"
    src.write_bytes(os.urandom(5_000_000))
    up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
    tid = up["transfer"]["id"]
    r = tc.post(f"/api/client/transfers/{tid}/cancel")
    assert r.json()["ok"] is True
    final = _wait_terminal(tc, tid)
    assert final["state"] == "cancelled"


def test_pause_nonexistent_transfer_returns_error_not_500(api_client):
    tc, _ = api_client
    _connect(tc)
    r = tc.post("/api/client/transfers/nope/pause")
    assert r.status_code == 200
    assert r.json() == {"ok": False, "error": "no such transfer"}


def test_settings_reject_extension_via_new_endpoint(api_client, tmp_path):
    tc, _ = api_client
    tc.post("/api/host/settings", json={"blocked_extensions": ".exe"})
    _connect(tc)
    src = tmp_path / "bad.exe"
    src.write_bytes(b"x")
    up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
    final = _wait_terminal(tc, up["transfer"]["id"])
    assert final["state"] == "failed"


def test_host_transfers_endpoint_reports_peer_side(api_client, tmp_path):
    tc, _ = api_client
    _connect(tc)
    src = tmp_path / "hv.bin"
    src.write_bytes(os.urandom(50_000))
    up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
    _wait_terminal(tc, up["transfer"]["id"])

    r = tc.get("/api/host/transfers").json()
    assert r["ok"] is True
    assert any(t["filename"] == "hv.bin" and t["state"] == "completed" for t in r["transfers"])


def test_host_peers_endpoint(api_client):
    tc, _ = api_client
    assert tc.get("/api/host/peers").json() == {"peers": []}
    _connect(tc)
    peers = tc.get("/api/host/peers").json()["peers"]
    assert len(peers) == 1
    tc.post("/api/client/disconnect")
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        peers = tc.get('/api/host/peers').json()['peers']
        if peers and not peers[0]['connected']:
            break
        time.sleep(0.01)
    assert len(peers) == 1 and not peers[0]['connected']


def test_transfer_events_websocket_receives_progress_and_completion(api_client, tmp_path):
    """Starlette's TestClient websocket receive() has no built-in
    timeout, so it's driven from a background thread and joined with a
    real one -- otherwise a message that never arrives hangs the test
    suite instead of failing it."""
    tc, _ = api_client
    _connect(tc)
    with tc.websocket_connect("/api/transfers/events") as ws:
        types_seen = []
        stop = threading.Event()

        def collector():
            while not stop.is_set():
                try:
                    types_seen.append(ws.receive_json(mode="text").get("type"))
                except Exception:
                    return
        th = threading.Thread(target=collector, daemon=True)
        th.start()

        src = tmp_path / "ev.bin"
        src.write_bytes(os.urandom(2_000_000))
        up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
        tid = up["transfer"]["id"]
        _wait_terminal(tc, tid)

        deadline = time.time() + 5
        while time.time() < deadline and "TRANSFER_COMPLETED" not in types_seen:
            time.sleep(0.05)
        stop.set()
        assert "TRANSFER_COMPLETED" in types_seen, types_seen


def test_snippet_endpoints_report_unavailable_without_crashing(api_client):
    tc, _ = api_client
    assert tc.get("/api/host/snippets").json() == {"ok": True, "snippets": []}
    r = tc.post("/api/host/snippets", json={"text": "hi"})
    assert r.json()["ok"] is False
    _connect(tc)
    r2 = tc.post("/api/client/send_snippet", json={"text": "hi"})
    assert r2.json()["ok"] is False


# ═══════════════════════════════════════════════════════════════════════
# bulk actions: bulk (all-transfers) REST endpoints
# ═══════════════════════════════════════════════════════════════════════

def test_cancel_all_endpoint(api_client, tmp_path):
    tc, _ = api_client
    _connect(tc)
    ids = []
    for i in range(3):
        src = tmp_path / f"ball{i}.bin"
        src.write_bytes(os.urandom(4_000_000))
        up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
        ids.append(up["transfer"]["id"])

    r = tc.post("/api/client/transfers/cancel_all")
    body = r.json()
    assert body["ok"] is True
    assert body["count"] >= 1

    for tid in ids:
        final = _wait_terminal(tc, tid)
        assert final["state"] in ("cancelled", "completed")  # a fast one may race to done


def test_pause_all_and_resume_all_endpoints(api_client, tmp_path):
    """End-to-end proof the endpoints exist and work; the deterministic
    regression test for the actual race bulk actions found and fixed here
    (resume_all() catching a transfer still PAUSING, not yet PAUSED --
    see scheduler.resume_all()'s docstring) lives at the scheduler
    level instead (test_bulk_actions.py), where an artificial
    per-chunk delay makes the timing controllable; this endpoint has
    no such knob, and on a fast loopback even a large file can
    complete before Pause All lands, which is an acceptable race here
    -- see the final assertion below -- not something this test needs
    to force open."""
    tc, _ = api_client
    _connect(tc)
    ids = []
    for i in range(2):
        src = tmp_path / f"pall{i}.bin"
        src.write_bytes(os.urandom(60_000_000))
        up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
        ids.append(up["transfer"]["id"])

    deadline = time.time() + 5
    while time.time() < deadline:
        offsets = [tc.get(f"/api/client/transfers/{t}").json()["transfer"]["offset"] for t in ids]
        if all(o > 0 for o in offsets):
            break
        time.sleep(0.02)

    r = tc.post("/api/client/transfers/pause_all")
    assert r.json()["ok"] is True
    r2 = tc.post("/api/client/transfers/resume_all")
    assert r2.json()["ok"] is True

    for tid in ids:
        final = _wait_terminal(tc, tid, timeout=45)
        assert final["state"] == "completed", final


def test_retry_all_endpoint(api_client, tmp_path):
    tc, _ = api_client
    tc.post("/api/host/settings", json={"blocked_extensions": ".exe"})
    _connect(tc)
    ids = []
    for i in range(2):
        src = tmp_path / f"rall{i}.exe"
        src.write_bytes(os.urandom(5_000))
        up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
        ids.append(up["transfer"]["id"])
    for tid in ids:
        _wait_terminal(tc, tid)

    tc.post("/api/host/settings", json={"blocked_extensions": ""})
    r = tc.post("/api/client/transfers/retry_all")
    assert r.json() == {"ok": True, "count": 2}
    for tid in ids:
        final = _wait_terminal(tc, tid)
        assert final["state"] == "completed", final


def test_bulk_endpoints_without_connection(api_client):
    tc, _ = api_client
    for path in ("cancel_all", "pause_all", "resume_all", "retry_all"):
        r = tc.post(f"/api/client/transfers/{path}")
        body = r.json()
        assert not body['ok'] and body['disconnected'] and not body['recoverable']
        assert 'not connected' in body['error']


def test_bulk_endpoints_return_zero_when_nothing_applies(api_client):
    tc, _ = api_client
    _connect(tc)
    for path in ("cancel_all", "pause_all", "resume_all", "retry_all"):
        r = tc.post(f"/api/client/transfers/{path}")
        assert r.json() == {"ok": True, "count": 0}
