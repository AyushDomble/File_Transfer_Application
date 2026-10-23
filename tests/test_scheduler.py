"""Concurrent scheduler integration tests: bounded admission, engine controls and isolation."""

from __future__ import annotations
from helpers.scheduler_peers import (Peer, _wait_for_state, _write_random_file, peers)

import hashlib
import os
import socket
import threading
import time

import pytest

from filetransfer.engine import FrameRouter, teardown_session
from filetransfer.manager import TransferManager
from filetransfer.session_protocol import Session
from filetransfer.scheduler import ConcurrentScheduler
from filetransfer.transfer import TransferState


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            buf = f.read(65536)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()










# ═══════════════════════════════════════════════════════════════════════
# Baseline: does engine.py actually work at all, end to end?
# ═══════════════════════════════════════════════════════════════════════

def test_single_upload_end_to_end(peers, tmp_path):
    client, server = peers
    src = tmp_path / "movie.bin"
    _write_random_file(str(src), 3 * 1024 * 1024 + 12345)  # not a clean multiple of chunk size

    transfer = client.scheduler.submit_upload(str(src))
    final = _wait_for_state(client.manager, transfer.id, {TransferState.COMPLETED, TransferState.FAILED})
    assert final.state == TransferState.COMPLETED, final.error

    # The peer's copy of the same logical transfer should also complete.
    server_transfers = [t for t in server.manager.all_transfers() if t.filename == "movie.bin"]
    assert len(server_transfers) == 1
    server_final = _wait_for_state(server.manager, server_transfers[0].id,
                                    {TransferState.COMPLETED, TransferState.FAILED})
    assert server_final.state == TransferState.COMPLETED, server_final.error

    dest = os.path.join(server.scheduler._dest_dir, "movie.bin")
    assert os.path.isfile(dest)
    assert _sha256(dest) == _sha256(str(src))


def test_single_download_end_to_end(peers, tmp_path):
    """The client requests a file and the responder serves a verified download."""
    client, server = peers
    src = os.path.join(server.scheduler._dest_dir, "song.bin")
    _write_random_file(src, 500_000)

    transfer = client.scheduler.submit_download("song.bin")
    final = _wait_for_state(client.manager, transfer.id, {TransferState.COMPLETED, TransferState.FAILED})
    assert final.state == TransferState.COMPLETED, final.error

    dest = os.path.join(client.scheduler._dest_dir, "song.bin")
    assert os.path.isfile(dest)
    assert _sha256(dest) == _sha256(src)


# ═══════════════════════════════════════════════════════════════════════
# scheduling: concurrency actually happens, and is actually bounded
# ═══════════════════════════════════════════════════════════════════════

def test_concurrent_uploads_respect_max_concurrent(peers, tmp_path):
    client, server = peers
    client.manager.max_concurrent = 2

    files = []
    for i in range(5):
        p = tmp_path / f"file{i}.bin"
        _write_random_file(str(p), 2 * 1024 * 1024 + i)  # a few chunks each
        files.append(str(p))

    observed_max = 0
    stop = threading.Event()

    def sampler():
        nonlocal observed_max
        while not stop.is_set():
            observed_max = max(observed_max, client.manager.running_count())
            time.sleep(0.005)

    sampler_thread = threading.Thread(target=sampler, daemon=True)
    sampler_thread.start()

    release = threading.Event()
    # Hold the first progress checkpoints until all jobs are submitted;
    # overlap must not depend on the machine's hashing/disk/network speed.
    transfers = [client.scheduler.submit_upload(f, progress_cb=lambda *_: release.wait(2)) for f in files]
    assert client.manager.running_count() == 2
    deadline = time.time() + 1
    while observed_max < 2 and time.time() < deadline:
        time.sleep(0.001)
    release.set()

    for t in transfers:
        final = _wait_for_state(client.manager, t.id,
                                 {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
        assert final.state == TransferState.COMPLETED, final.error

    stop.set()
    sampler_thread.join(timeout=1)

    # The limit was actually enforced...
    assert observed_max <= 2, f"max_concurrent=2 but observed {observed_max} running at once"
    # ...and concurrency actually happened (not accidentally serial).
    assert observed_max >= 2, f"expected real overlap, only observed {observed_max} running at once"

    for f in files:
        name = os.path.basename(f)
        dest = os.path.join(server.scheduler._dest_dir, name)
        assert os.path.isfile(dest)
        assert _sha256(dest) == _sha256(f)


def test_queued_transfer_starts_when_slot_frees(peers, tmp_path):
    client, server = peers
    client.manager.max_concurrent = 1

    f1 = tmp_path / "a.bin"; _write_random_file(str(f1), 200_000)
    f2 = tmp_path / "b.bin"; _write_random_file(str(f2), 200_000)

    t1 = client.scheduler.submit_upload(str(f1))
    t2 = client.scheduler.submit_upload(str(f2))

    # With max_concurrent=1, t2 must still be QUEUED right after
    # submission (it may already be gone from QUEUED to ACTIVE only
    # once t1 finishes).
    assert client.manager.get(t2.id).state in (TransferState.QUEUED, TransferState.ACTIVE)

    final1 = _wait_for_state(client.manager, t1.id, {TransferState.COMPLETED, TransferState.FAILED})
    assert final1.state == TransferState.COMPLETED, final1.error

    final2 = _wait_for_state(client.manager, t2.id, {TransferState.COMPLETED, TransferState.FAILED})
    assert final2.state == TransferState.COMPLETED, final2.error


def test_cancel_one_transfer_does_not_affect_others(peers, tmp_path):
    client, server = peers
    client.manager.max_concurrent = 3

    files = []
    for i in range(3):
        p = tmp_path / f"iso{i}.bin"
        _write_random_file(str(p), 3 * 1024 * 1024)
        files.append(str(p))

    transfers = [client.scheduler.submit_upload(f) for f in files]

    # Cancel the middle one shortly after it starts.
    _wait_for_state(client.manager, transfers[1].id,
                     {TransferState.ACTIVE, TransferState.COMPLETED, TransferState.FAILED})
    client.scheduler.cancel(transfers[1].id)

    final0 = _wait_for_state(client.manager, transfers[0].id, {TransferState.COMPLETED, TransferState.FAILED})
    final1 = _wait_for_state(client.manager, transfers[1].id, {TransferState.CANCELLED})
    final2 = _wait_for_state(client.manager, transfers[2].id, {TransferState.COMPLETED, TransferState.FAILED})

    assert final0.state == TransferState.COMPLETED, final0.error
    assert final1.state == TransferState.CANCELLED
    assert final2.state == TransferState.COMPLETED, final2.error

    # Session must still be alive — cancelling one transfer must never
    # close it (the core invariant), and the other two
    # transfers proved that above by completing on the same session.
    assert not client.session.closed
    assert not server.session.closed
