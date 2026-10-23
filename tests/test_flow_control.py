"""
flow control — flow control.

Stop-and-wait ACK (acknowledgements) already bounds a single transfer's own
outstanding bytes to one chunk. The session budget bounds the SUM
across every transfer concurrently ACTIVE on one session — scheduling's
max_concurrent caps transfer *count*, not bytes in flight. These tests
prove GlobalWindow actually closes that gap (not just that it exists),
and that WINDOW_UPDATE lets the receiver change the budget mid-transfer.
"""

from __future__ import annotations
from helpers.flow_peers import (CHUNK_SIZE, Peer, _make_peers, _wait_for_state, _write_random_file)

import os
import random
import socket
import threading
import time

import pytest

from filetransfer.engine import FrameRouter, RecvEngine, teardown_session
from filetransfer.flowcontrol import GlobalWindow
from filetransfer.manager import TransferManager
from filetransfer.session_protocol import Session
from filetransfer.scheduler import ConcurrentScheduler
from filetransfer.transfer import TransferState









@pytest.fixture()
def peers(tmp_path):
    window = GlobalWindow(capacity=2 * CHUNK_SIZE)
    client, server = _make_peers(tmp_path, client_window=window)
    yield client, server, window
    client.teardown()
    server.teardown()




def _instrument(window: GlobalWindow) -> dict:
    """Wrap acquire()/release() to track observed peak outstanding
    bytes and catch any moment outstanding exceeds capacity — proof
    the window is actually constraining behavior, not just present."""
    orig_acquire = window.acquire
    orig_release = window.release
    state = {"outstanding": 0, "peak": 0, "violations": [], "lock": threading.Lock()}

    def acquire(nbytes, *a, **kw):
        ok = orig_acquire(nbytes, *a, **kw)
        if ok:
            with state["lock"]:
                state["outstanding"] += nbytes
                state["peak"] = max(state["peak"], state["outstanding"])
                if state["outstanding"] > window.capacity:
                    state["violations"].append(state["outstanding"])
        return ok

    def release(nbytes):
        with state["lock"]:
            state["outstanding"] -= nbytes
        orig_release(nbytes)

    window.acquire = acquire
    window.release = release
    return state


# ═══════════════════════════════════════════════════════════════════════
# GlobalWindow unit-level behavior
# ═══════════════════════════════════════════════════════════════════════

def test_window_acquire_release_basic():
    w = GlobalWindow(capacity=100)
    assert w.available == 100
    assert w.acquire(60) is True
    assert w.available == 40
    assert w.acquire(60, timeout=0.1) is False   # would exceed capacity
    assert w.available == 40                      # failed acquire reserves nothing
    w.release(60)
    assert w.available == 100


def test_window_acquire_blocks_until_release():
    w = GlobalWindow(capacity=10)
    assert w.acquire(10) is True

    results = []

    def waiter():
        results.append(w.acquire(5, timeout=5))

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.1)
    assert results == []  # still blocked — nothing released yet
    w.release(10)
    t.join(timeout=2)
    assert results == [True]


def test_window_acquire_respects_cancel_event():
    w = GlobalWindow(capacity=10)
    w.acquire(10)  # exhaust it
    cancel = threading.Event()

    def canceller():
        time.sleep(0.05)
        cancel.set()

    threading.Thread(target=canceller, daemon=True).start()
    start = time.time()
    ok = w.acquire(5, cancel_event=cancel, timeout=10)
    elapsed = time.time() - start
    assert ok is False
    assert elapsed < 2, "acquire() didn't notice cancel_event promptly"


def test_window_acquire_waits_rather_than_failing_when_request_exceeds_capacity():
    """A request bigger than the current capacity (e.g. after a
    WINDOW_UPDATE shrank it below one chunk's size) must not raise or
    fail the transfer outright — it should just wait, exactly like any
    other insufficient-availability case, since the peer may grow the
    window back shortly. With no growth and a timeout, it times out
    like any other unmet wait — not an exception."""
    w = GlobalWindow(capacity=100)
    ok = w.acquire(200, timeout=0.2)
    assert ok is False
    assert w.available == 100  # nothing was reserved


def test_window_resize_shrink_then_grow():
    w = GlobalWindow(capacity=100)
    w.acquire(80)
    w.resize(50)  # shrink below what's already outstanding
    assert w.available == 0  # never negative
    w.release(80)
    assert w.available == 50  # respects the new, smaller capacity
    w.resize(200)
    assert w.available == 200


# ═══════════════════════════════════════════════════════════════════════
# End-to-end: the window actually bounds concurrent transfers
# ═══════════════════════════════════════════════════════════════════════

def test_global_window_bounds_concurrent_outstanding_bytes(peers, tmp_path):
    """5 concurrent transfers, each capable of having its own chunk
    outstanding (stop-and-wait) — without a global window, that's up
    to 5*CHUNK_SIZE in flight at once. With capacity=2*CHUNK_SIZE, it
    must never exceed that, and it must actually get close to it
    (proving contention really happened, not just an empty test)."""
    client, server, window = peers
    trace = _instrument(window)

    files = []
    for i in range(5):
        p = tmp_path / f"win_{i}.bin"
        data = _write_random_file(str(p), 8 * CHUNK_SIZE + i, seed=50 + i)
        files.append((str(p), data))

    transfers = [
        client.scheduler.submit_upload(path, chunk_size=CHUNK_SIZE)
        for path, _ in files
    ]

    for t in transfers:
        final = _wait_for_state(client.manager, t.id,
                                 {TransferState.COMPLETED, TransferState.FAILED}, timeout=30)
        assert final.state == TransferState.COMPLETED, final.error

    assert not trace["violations"], f"outstanding exceeded capacity: {trace['violations']}"
    assert trace["peak"] <= window.capacity
    assert trace["peak"] >= window.capacity - CHUNK_SIZE, (
        f"peak outstanding ({trace['peak']}) never approached capacity "
        f"({window.capacity}) — the window may not have been exercised"
    )

    for path, data in files:
        dest = os.path.join(server.scheduler._dest_dir, os.path.basename(path))
        with open(dest, "rb") as f:
            assert f.read() == data


def test_transfer_completes_correctly_with_flow_control(peers, tmp_path):
    """Basic regression: a normal single transfer still completes
    byte-exact with a (generously sized, non-throttling) window."""
    client, server, window = peers
    window.resize(64 * 1024)  # plenty — this test isn't about throttling

    size = 15 * CHUNK_SIZE + 321
    src = tmp_path / "normal.bin"
    data = _write_random_file(str(src), size, seed=60)

    transfer = client.scheduler.submit_upload(str(src), chunk_size=CHUNK_SIZE)
    final = _wait_for_state(client.manager, transfer.id,
                             {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
    assert final.state == TransferState.COMPLETED, final.error

    dest = os.path.join(server.scheduler._dest_dir, "normal.bin")
    with open(dest, "rb") as f:
        assert f.read() == data


# ═══════════════════════════════════════════════════════════════════════
# WINDOW_UPDATE — receiver-driven, mid-transfer
# ═══════════════════════════════════════════════════════════════════════

def test_window_update_shrinks_then_grows_mid_transfer(peers, tmp_path):
    client, server, window = peers
    window.resize(64 * 1024)  # start generous so the transfer gets going

    size = 30 * CHUNK_SIZE
    src = tmp_path / "update_me.bin"
    data = _write_random_file(str(src), size, seed=61)

    def slow_cb(offset, total):
        time.sleep(0.01)  # keep it alive long enough to land a mid-transfer update

    transfer = client.scheduler.submit_upload(str(src), chunk_size=CHUNK_SIZE, progress_cb=slow_cb)

    # Let it get partway, then find the server's RecvEngine for this
    # transfer and have it clamp the window down hard.
    deadline = time.time() + 5
    server_transfer = None
    while time.time() < deadline:
        matches = [t for t in server.manager.all_transfers() if t.filename == "update_me.bin"]
        if matches:
            server_transfer = matches[0]
            break
        time.sleep(0.005)
    assert server_transfer is not None

    while client.manager.get(transfer.id).offset < 3 * CHUNK_SIZE:
        time.sleep(0.005)

    server.scheduler.send_window_update(server_transfer.id, CHUNK_SIZE)

    # Wait for the client's window to actually observe the shrink —
    # it applies whenever the next ACK wait sees the frame, not
    # instantly on send.
    deadline = time.time() + 5
    while time.time() < deadline and window.capacity != CHUNK_SIZE:
        time.sleep(0.01)
    assert window.capacity == CHUNK_SIZE

    # Grow it back so the transfer isn't stuck at a 1-chunk trickle
    # for the rest of the test.
    server.scheduler.send_window_update(server_transfer.id, 64 * 1024)

    final = _wait_for_state(client.manager, transfer.id,
                             {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
    assert final.state == TransferState.COMPLETED, final.error

    dest = os.path.join(server.scheduler._dest_dir, "update_me.bin")
    with open(dest, "rb") as f:
        assert f.read() == data


def test_window_shrink_adapts_chunks_without_deadlock(peers, tmp_path, monkeypatch):
    """A sub-chunk window now makes smaller frames, not a deliberate stall.
    Observe frame sizes instead of relying on machine-dependent throughput."""
    client, server, window = peers
    window.resize(64 * 1024)
    from filetransfer.transport import FrameType
    from filetransfer.engine import _decode_data_payload
    observed = []
    original_send = client.session.send
    def send(frame):
        if frame.frame_type == FrameType.TRANSFER_DATA:
            observed.append(len(_decode_data_payload(frame.payload)[1]))
        original_send(frame)
    monkeypatch.setattr(client.session, 'send', send)

    size = 160 * CHUNK_SIZE
    src = tmp_path / "throttle_me.bin"
    _write_random_file(str(src), size, seed=62)

    def slow_cb(offset, total):
        time.sleep(0.002)  # keep it from finishing before we can measure

    transfer = client.scheduler.submit_upload(str(src), chunk_size=CHUNK_SIZE, progress_cb=slow_cb)

    server_transfer = None
    deadline = time.time() + 5
    while time.time() < deadline:
        matches = [t for t in server.manager.all_transfers() if t.filename == "throttle_me.bin"]
        if matches:
            server_transfer = matches[0]
            break
        time.sleep(0.005)
    assert server_transfer is not None

    while client.manager.get(transfer.id).offset < 3 * CHUNK_SIZE:
        time.sleep(0.005)

    # Clamp below the original chunk size. Scoped-flow senders subdivide.
    server.scheduler.send_window_update(server_transfer.id, CHUNK_SIZE // 2)
    deadline = time.time() + 5
    while time.time() < deadline and window.capacity != CHUNK_SIZE // 2:
        time.sleep(0.01)
    assert window.capacity == CHUNK_SIZE // 2

    deadline = time.time() + 5
    while time.time() < deadline and observed.count(CHUNK_SIZE // 2) < 3:
        time.sleep(0.005)
    assert observed.count(CHUNK_SIZE // 2) >= 3

    server.scheduler.send_window_update(server_transfer.id, 64 * 1024)
    deadline = time.time() + 5
    while time.time() < deadline and window.capacity != 64 * 1024:
        time.sleep(0.01)

    final = _wait_for_state(client.manager, transfer.id,
                             {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
    assert final.state == TransferState.COMPLETED, final.error
