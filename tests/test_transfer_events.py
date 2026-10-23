"""WebSocket transfer-event feed contracts for /api/transfers/events.
Verify distinguishable creation, queueing, starting, pausing, resuming,
cancellation, completion, failure, retry and connection-loss events.
Creation occurs once; recovery can produce additional queue transitions."""

from __future__ import annotations

import os
import shutil
import threading
import time

import pytest

os.environ.setdefault("FT_SERVER_DIR", "evt_server_files")
os.environ.setdefault("FT_CLIENT_DIR", "evt_client_files")
os.environ.setdefault("FT_PENDING_TIMEOUT", "5")
os.environ.setdefault("FT_PIN_EXPIRY_SECONDS", "0")


# ═══════════════════════════════════════════════════════════════════════
# Unit level: _event_name_for and the manager's creation notification,
# with no HTTP/WS involved -- fast, and isolates the actual logic.
# ═══════════════════════════════════════════════════════════════════════

def test_event_name_for_creation_vs_requeue_vs_start_vs_resume():
    from webapp.api import _event_name_for
    from filetransfer.transfer import TransferState as S

    assert _event_name_for(None, S.QUEUED) == "TRANSFER_CREATED"
    assert _event_name_for(S.CONNECTION_LOST, S.QUEUED) == "TRANSFER_QUEUED"
    assert _event_name_for(S.QUEUED, S.ACTIVE) == "TRANSFER_STARTED"
    assert _event_name_for(S.RETRYING, S.ACTIVE) == "TRANSFER_STARTED"
    assert _event_name_for(S.PAUSED, S.ACTIVE) == "TRANSFER_RESUMED"
    assert _event_name_for(S.PAUSING, S.ACTIVE) == "TRANSFER_RESUMED"
    assert _event_name_for(S.ACTIVE, S.PAUSING) == "TRANSFER_PAUSING"
    assert _event_name_for(S.PAUSING, S.PAUSED) == "TRANSFER_PAUSED"
    assert _event_name_for(S.ACTIVE, S.CANCELLED) == "TRANSFER_CANCELLED"
    assert _event_name_for(S.ACTIVE, S.COMPLETED) == "TRANSFER_COMPLETED"
    assert _event_name_for(S.ACTIVE, S.FAILED) == "TRANSFER_FAILED"
    assert _event_name_for(S.FAILED, S.RETRYING) == "TRANSFER_RETRYING"
    assert _event_name_for(S.ACTIVE, S.CONNECTION_LOST) == "TRANSFER_CONNECTION_LOST"


def test_manager_fires_created_event_exactly_once_at_creation():
    """Regression: a Transfer starts life already QUEUED, so without an
    explicit creation notification (required for creation), no listener
    could ever observe a transition INTO it -- TRANSFER_CREATED was
    dead code."""
    from filetransfer.manager import TransferManager
    from filetransfer.transfer import TransferDirection, TransferState

    events = []
    m = TransferManager(max_concurrent=1)
    m.add_listener(lambda t, old, new: events.append((old, new)))
    t = m.create_transfer(TransferDirection.UPLOAD, "x.bin", 10, start_immediately=False)

    assert events == [(None, TransferState.QUEUED)]
    m.start_transfer(t.id)
    assert events[-1] == (TransferState.QUEUED, TransferState.ACTIVE)


def test_listener_registered_at_creation_time_still_sees_it():
    """add_listener() calls happening before create_transfer() (the
    normal case) must see the creation event too -- it's fired from
    inside create_transfer() itself, not from a later set_state()."""
    from filetransfer.manager import TransferManager
    from filetransfer.transfer import TransferDirection

    m = TransferManager(max_concurrent=1)
    seen = []
    m.add_listener(lambda t, old, new: seen.append(new.value))
    m.create_transfer(TransferDirection.UPLOAD, "y.bin", 5, start_immediately=False)
    assert seen[0] == "queued"


def test_listener_does_not_deadlock_the_transition_it_observes():
    """Regression for event-delivery consistency
    before shipping: set_state() calls listeners while still holding
    the transfer's own (non-reentrant) lock, so a listener that reads
    the transfer via a locking accessor (to_dict(), the same object's
    .lock, etc.) deadlocks the very transition it's reacting to. This
    must return promptly, not hang."""
    from filetransfer.manager import TransferManager
    from filetransfer.transfer import TransferDirection
    from webapp.api import _snapshot_without_locking, _event_name_for

    m = TransferManager(max_concurrent=1)

    def listener(transfer, old, new):
        # Exactly what _emit_transfer_event does -- must not touch
        # transfer._lock (e.g. via to_dict()) from in here.
        _snapshot_without_locking(transfer, new)
        _event_name_for(old, new)

    m.add_listener(listener)
    t = m.create_transfer(TransferDirection.UPLOAD, "z.bin", 5, start_immediately=False)

    done = threading.Event()
    def go():
        m.start_transfer(t.id)
        m.pause_transfer(t.id)
        m.confirm_paused(t.id)
        m.resume_transfer(t.id)
        done.set()
    th = threading.Thread(target=go)
    th.start()
    th.join(timeout=3)
    assert done.is_set(), "a listener call appears to have deadlocked set_state()"


# ═══════════════════════════════════════════════════════════════════════
# End-to-end: real WS connection, real ConcurrentScheduler-driven upload.
# ═══════════════════════════════════════════════════════════════════════

@pytest.fixture()
def api_client(tmp_path, monkeypatch):
    from filetransfer import config
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
    from authenticated_client import authenticated_client
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
    assert tc.post("/api/client/connect", json={"host": "127.0.0.1", "pin": info["pin"],
        "fingerprint": info["certificate_fingerprint"]}).json()["ok"]


def _collect(ws, stop, out):
    while not stop.is_set():
        try:
            out.append(ws.receive_json(mode="text"))
        except Exception:
            return


def test_full_upload_event_sequence_in_order(api_client, tmp_path):
    tc, _ = api_client
    _connect(tc)
    with tc.websocket_connect("/api/transfers/events") as ws:
        events, stop = [], threading.Event()
        th = threading.Thread(target=_collect, args=(ws, stop, events), daemon=True)
        th.start()

        src = tmp_path / "seq.bin"
        src.write_bytes(os.urandom(50_000))
        up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
        tid = up["transfer"]["id"]

        deadline = time.time() + 10
        while time.time() < deadline:
            r = tc.get(f"/api/client/transfers/{tid}").json()
            if r["transfer"]["state"] in ("completed", "failed"):
                break
            time.sleep(0.02)
        time.sleep(0.3)
        stop.set()

    # The client's own Transfer and the host's own Transfer for the
    # SAME wire transfer share one ID (by design -- see engine.py's
    # handshake) but live in two separate TransferManagers, each
    # firing its own full CREATED..COMPLETED sequence. Isolate the
    # client side's events for the ordering checks below.
    mine = [e for e in events if e.get("id") == tid and e["side"] == "client"]
    types = [e["type"] for e in mine]
    assert types[0] == "TRANSFER_CREATED", types
    assert "TRANSFER_STARTED" in types
    assert types[-1] == "TRANSFER_COMPLETED", types
    # STARTED must precede COMPLETED, CREATED must precede STARTED.
    assert types.index("TRANSFER_CREATED") < types.index("TRANSFER_STARTED") < types.index("TRANSFER_COMPLETED")
    assert all(e["peer_id"] is None for e in mine)
    completed = [e for e in mine if e["type"] == "TRANSFER_COMPLETED"][0]
    assert completed["old_state"] == "active"
    assert completed["state"] == "completed"

    # The host's side of the SAME transfer id also ran its own full
    # sequence, tagged with a peer_id.
    theirs = [e for e in events if e.get("id") == tid and e["side"] == "host"]
    assert theirs and theirs[0]["type"] == "TRANSFER_CREATED"
    assert theirs[-1]["type"] == "TRANSFER_COMPLETED"
    assert all(e["peer_id"] is not None for e in theirs)


def test_pause_resume_emits_resumed_not_started(api_client, tmp_path):
    tc, _ = api_client
    _connect(tc)
    with tc.websocket_connect("/api/transfers/events") as ws:
        events, stop = [], threading.Event()
        th = threading.Thread(target=_collect, args=(ws, stop, events), daemon=True)
        th.start()

        # Deliberately large: pause must land well before the transfer
        # would finish on its own. If it doesn't, PAUSED -> COMPLETED
        # is itself a legal, pre-existing transition (a pause racing
        # natural completion reports success rather than failure -- see
        # transfer.py) and RESUMED would legitimately never fire; this
        # size margin is what keeps that race rare enough not to flake.
        src = tmp_path / "pr.bin"
        src.write_bytes(os.urandom(25_000_000))
        up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
        tid = up["transfer"]["id"]

        deadline = time.time() + 10
        while time.time() < deadline:
            if tc.get(f"/api/client/transfers/{tid}").json()["transfer"]["offset"] > 1_000_000:
                break
            time.sleep(0.01)
        tc.post(f"/api/client/transfers/{tid}/pause")
        deadline = time.time() + 5
        while time.time() < deadline:
            if tc.get(f"/api/client/transfers/{tid}").json()["transfer"]["state"] == "paused":
                break
            time.sleep(0.02)
        tc.post(f"/api/client/transfers/{tid}/resume")

        deadline = time.time() + 15
        while time.time() < deadline:
            if tc.get(f"/api/client/transfers/{tid}").json()["transfer"]["state"] in ("completed", "failed"):
                break
            time.sleep(0.02)
        time.sleep(0.3)
        stop.set()

    types = [e["type"] for e in events if e.get("id") == tid and e["side"] == "client"]
    assert "TRANSFER_PAUSING" in types and "TRANSFER_PAUSED" in types, types
    assert "TRANSFER_RESUMED" in types, types
    # A fresh start happened exactly once; the second ACTIVE arrival
    # (from PAUSED) must be RESUMED, not a second STARTED.
    assert types.count("TRANSFER_STARTED") == 1, types
    assert types.index("TRANSFER_PAUSED") < types.index("TRANSFER_RESUMED")


def test_failed_upload_emits_failed_event(api_client, tmp_path):
    tc, _ = api_client
    tc.post("/api/host/settings", json={"blocked_extensions": ".exe"})
    _connect(tc)
    with tc.websocket_connect("/api/transfers/events") as ws:
        events, stop = [], threading.Event()
        th = threading.Thread(target=_collect, args=(ws, stop, events), daemon=True)
        th.start()

        src = tmp_path / "bad.exe"
        src.write_bytes(b"nope")
        up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
        tid = up["transfer"]["id"]
        deadline = time.time() + 10
        while time.time() < deadline:
            if tc.get(f"/api/client/transfers/{tid}").json()["transfer"]["state"] == "failed":
                break
            time.sleep(0.02)
        time.sleep(0.3)
        stop.set()

    types = [e["type"] for e in events if e.get("id") == tid]
    assert types[-1] == "TRANSFER_FAILED", types


def test_host_side_events_carry_peer_id(api_client, tmp_path):
    tc, _ = api_client
    _connect(tc)
    with tc.websocket_connect("/api/transfers/events") as ws:
        events, stop = [], threading.Event()
        th = threading.Thread(target=_collect, args=(ws, stop, events), daemon=True)
        th.start()

        src = tmp_path / "host_side.bin"
        src.write_bytes(os.urandom(10_000))
        up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
        tid = up["transfer"]["id"]
        deadline = time.time() + 10
        while time.time() < deadline:
            if tc.get(f"/api/client/transfers/{tid}").json()["transfer"]["state"] == "completed":
                break
            time.sleep(0.02)
        time.sleep(0.3)
        stop.set()

    peer_id = tc.get("/api/host/peers").json()["peers"][0]["peer_id"]
    host_events = [e for e in events if e.get("side") == "host" and e.get("filename") == "host_side.bin"]
    assert host_events, "no host-side events observed at all"
    assert all(e["peer_id"] == peer_id for e in host_events)
    assert any(e["type"] == "TRANSFER_COMPLETED" for e in host_events)


def test_progress_events_have_monotonic_offset(api_client, tmp_path):
    tc, _ = api_client
    _connect(tc)
    with tc.websocket_connect("/api/transfers/events") as ws:
        events, stop = [], threading.Event()
        th = threading.Thread(target=_collect, args=(ws, stop, events), daemon=True)
        th.start()

        src = tmp_path / "prog.bin"
        src.write_bytes(os.urandom(6_000_000))
        up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
        tid = up["transfer"]["id"]
        deadline = time.time() + 15
        while time.time() < deadline:
            if tc.get(f"/api/client/transfers/{tid}").json()["transfer"]["state"] == "completed":
                break
            time.sleep(0.02)
        time.sleep(0.3)
        stop.set()

    offsets = [e["offset"] for e in events
               if e.get("id") == tid and e["side"] == "client" and e["type"] == "TRANSFER_PROGRESS"]
    assert len(offsets) >= 1, [e["type"] for e in events if e.get("id") == tid]
    assert offsets == sorted(offsets)
    assert offsets[-1] == 6_000_000


def test_rest_transfer_summary_includes_progress_and_timestamps(api_client, tmp_path):
    """_transfer_summary() (REST-handler path) now uses to_dict(),
    which is richer than the old hand-rolled dict -- verifies the
    richer fields actually reach the API, not just that nothing
    crashed."""
    tc, _ = api_client
    _connect(tc)
    src = tmp_path / "rich.bin"
    src.write_bytes(os.urandom(20_000))
    up = tc.post("/api/client/transfers/upload", json={"local_path": str(src)}).json()
    tid = up["transfer"]["id"]
    deadline = time.time() + 10
    while time.time() < deadline:
        r = tc.get(f"/api/client/transfers/{tid}").json()["transfer"]
        if r["state"] == "completed":
            break
        time.sleep(0.02)
    assert r["progress"] == 1.0
    assert r["created_at"] is not None
    assert r["started_at"] is not None
    assert r["ended_at"] is not None
    assert r["elapsed_sec"] is not None
