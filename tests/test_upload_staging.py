"""
Non-blocking chunked-upload bridge contracts for the frontend queue.
Tests cover asynchronous submission, scratch-file ownership, lifecycle
controls and the separate blocking HTTP upload adapter.
"""

from __future__ import annotations

import os
import shutil
import threading
import time

import pytest

os.environ.setdefault("FT_SERVER_DIR", "p21_server_files")
os.environ.setdefault("FT_CLIENT_DIR", "p21_client_files")
os.environ.setdefault("FT_PENDING_TIMEOUT", "5")
os.environ.setdefault("FT_PIN_EXPIRY_SECONDS", "0")


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


def _send_chunked(tc, upload_id, filename, data, chunk_size, overwrite=False):
    """Mirrors what the frontend's chunker does: slice, POST each piece
    with the same query params the endpoint expects."""
    total = len(data)
    n_chunks = max(1, (total + chunk_size - 1) // chunk_size)
    last = None
    for i in range(n_chunks):
        piece = data[i * chunk_size: (i + 1) * chunk_size]
        is_final = i == n_chunks - 1
        last = tc.post(
            "/api/client/transfers/upload_stream",
            params={"upload_id": upload_id, "filename": filename, "size": total,
                    "chunk_index": i, "is_final": is_final, "overwrite": overwrite},
            content=piece, headers={"Content-Type": "application/octet-stream"},
        ).json()
        if not is_final:
            assert last == {"ok": True}
    return last


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


def test_final_chunk_responds_immediately_with_a_transfer_id(api_client):
    tc, _ = api_client
    _connect(tc)
    data = os.urandom(300_000)
    result = _send_chunked(tc, "u1", "quick.bin", data, chunk_size=100_000)
    assert result["ok"] is True
    assert "transfer" in result and result["transfer"]["id"]
    assert result["transfer"]["state"] in ("queued", "active")

    final = _wait_terminal(tc, result["transfer"]["id"])
    assert final["state"] == "completed", final
    dest = os.path.join(os.environ["FT_SERVER_DIR"], "quick.bin")
    assert open(dest, "rb").read() == data


def test_response_does_not_block_for_completion(api_client):
    """The whole point of this endpoint over the adapter one: the HTTP
    response for the final chunk must come back fast, independent of
    how long the actual transfer takes."""
    tc, _ = api_client
    _connect(tc)
    data = os.urandom(20_000_000)
    start = time.monotonic()
    result = _send_chunked(tc, "u2", "big.bin", data, chunk_size=4_000_000)
    elapsed = time.monotonic() - start
    assert result["ok"] is True
    # Staging 20MB over loopback plus one non-blocking submit call --
    # generous but should be nowhere near how long the full transfer
    # (chunk-by-chunk ACKed engine send) actually takes.
    assert elapsed < 5.0, f"took {elapsed:.1f}s -- looks like it blocked"
    _wait_terminal(tc, result["transfer"]["id"], timeout=30)


def test_scratch_file_is_cleaned_up_after_completion(api_client):
    tc, _ = api_client
    _connect(tc)
    data = os.urandom(200_000)
    result = _send_chunked(tc, "u3", "clean.bin", data, chunk_size=200_000)
    tid = result["transfer"]["id"]
    scratch_dir = os.path.join(os.environ["FT_CLIENT_DIR"], ".upload_scratch")
    _wait_terminal(tc, tid)
    time.sleep(0.2)
    assert os.listdir(scratch_dir) == [] if os.path.isdir(scratch_dir) else True


def test_scratch_file_is_cleaned_up_after_cancel(api_client):
    tc, _ = api_client
    _connect(tc)
    data = os.urandom(10_000_000)
    result = _send_chunked(tc, "u4", "cancelme.bin", data, chunk_size=2_000_000)
    tid = result["transfer"]["id"]
    tc.post(f"/api/client/transfers/{tid}/cancel")
    final = _wait_terminal(tc, tid)
    assert final["state"] == "cancelled"
    scratch_dir = os.path.join(os.environ["FT_CLIENT_DIR"], ".upload_scratch")
    time.sleep(0.2)
    assert os.listdir(scratch_dir) == [] if os.path.isdir(scratch_dir) else True


def test_pause_and_resume_during_a_streamed_upload(api_client):
    tc, _ = api_client
    _connect(tc)
    data = os.urandom(15_000_000)
    result = _send_chunked(tc, "u5", "pauseme.bin", data, chunk_size=3_000_000)
    tid = result["transfer"]["id"]

    deadline = time.time() + 10
    while time.time() < deadline:
        if tc.get(f"/api/client/transfers/{tid}").json()["transfer"]["offset"] > 0:
            break
        time.sleep(0.01)
    tc.post(f"/api/client/transfers/{tid}/pause")
    deadline = time.time() + 5
    while time.time() < deadline:
        if tc.get(f"/api/client/transfers/{tid}").json()["transfer"]["state"] == "paused":
            break
        time.sleep(0.02)
    tc.post(f"/api/client/transfers/{tid}/resume")

    final = _wait_terminal(tc, tid, timeout=20)
    assert final["state"] == "completed", final
    dest = os.path.join(os.environ["FT_SERVER_DIR"], "pauseme.bin")
    assert open(dest, "rb").read() == data


def test_conflict_without_overwrite_fails_as_an_event_not_a_sync_flag(api_client):
    """Unlike the adapter endpoint, this one can't know about a conflict
    at response time -- it's reported as an ordinary FAILED transfer,
    for the frontend to catch via the WS event / polling, matching how
    every other failure here is already reported."""
    tc, _ = api_client
    _connect(tc)
    data = os.urandom(50_000)
    first = _send_chunked(tc, "u6a", "dup.bin", data, chunk_size=50_000)
    _wait_terminal(tc, first["transfer"]["id"])

    second = _send_chunked(tc, "u6b", "dup.bin", data, chunk_size=50_000, overwrite=False)
    assert second["ok"] is True  # staged and submitted fine; the CONFLICT is what fails
    assert "conflict" not in second
    final = _wait_terminal(tc, second["transfer"]["id"])
    assert final["state"] == "failed"
    assert "already exists" in (final["error"] or "")


def test_conflict_with_overwrite_succeeds(api_client):
    tc, _ = api_client
    _connect(tc)
    first = _send_chunked(tc, "u7a", "ow.bin", b"first version", chunk_size=100)
    _wait_terminal(tc, first["transfer"]["id"])

    second = _send_chunked(tc, "u7b", "ow.bin", b"second, different version", chunk_size=100, overwrite=True)
    final = _wait_terminal(tc, second["transfer"]["id"])
    assert final["state"] == "completed", final
    dest = os.path.join(os.environ["FT_SERVER_DIR"], "ow.bin")
    assert open(dest, "rb").read() == b"second, different version"


def test_failed_transfer_keeps_its_scratch_file_so_retry_works(api_client):
    """scheduler.retry() rebuilds the engine from the SAME local path
    submit_upload() was originally given -- the scratch file -- so it
    must not be deleted just because the transfer failed, unlike the
    completed/cancelled cases above. This is the actual reason for
    that difference, not a test of cleanup in isolation."""
    tc, _ = api_client
    tc.post("/api/host/settings", json={"blocked_extensions": ".exe"})
    _connect(tc)
    data = os.urandom(80_000)
    result = _send_chunked(tc, "u8", "retry.exe", data, chunk_size=80_000)
    tid = result["transfer"]["id"]
    failed = _wait_terminal(tc, tid)
    assert failed["state"] == "failed"

    scratch_dir = os.path.join(os.environ["FT_CLIENT_DIR"], ".upload_scratch")
    assert os.path.isdir(scratch_dir) and os.listdir(scratch_dir), \
        "scratch file was deleted on failure -- retry would now fail too"

    tc.post("/api/host/settings", json={"blocked_extensions": ""})
    r = tc.post(f"/api/client/transfers/{tid}/retry")
    assert r.json()["ok"] is True

    final = _wait_terminal(tc, tid)
    assert final["state"] == "completed", final
    dest = os.path.join(os.environ["FT_SERVER_DIR"], "retry.exe")
    assert open(dest, "rb").read() == data
    # NOW it's cleaned up, since this attempt actually completed.
    time.sleep(0.2)
    assert os.listdir(scratch_dir) == []


def test_without_connection_returns_not_connected(api_client):
    tc, _ = api_client
    r = tc.post(
        "/api/client/transfers/upload_stream",
        params={"upload_id": "x", "filename": "x.bin", "size": 3,
                "chunk_index": 0, "is_final": True, "overwrite": False},
        content=b"abc",
    ).json()
    assert not r['ok'] and r['disconnected'] and not r['recoverable']
    assert 'not connected' in r['error']


def test_blocking_upload_adapter_completes_transfer(api_client):
    """The blocking adapter waits for completion and returns ok
    only once the whole transfer finishes. Not a regression test for
    THIS endpoint; a sanity check that adding the new one alongside it
    didn't disturb the old one's shared scratch-staging helpers."""
    tc, _ = api_client
    _connect(tc)
    data = os.urandom(50_000)
    r = tc.post(
        "/api/client/upload_stream",
        params={"upload_id": "adapter1", "filename": "adapter.bin", "size": len(data),
                "chunk_index": 0, "is_final": True, "overwrite": False},
        content=data, headers={"Content-Type": "application/octet-stream"},
    ).json()
    assert r == {"ok": True}
    dest = os.path.join(os.environ["FT_SERVER_DIR"], "adapter.bin")
    assert open(dest, "rb").read() == data
