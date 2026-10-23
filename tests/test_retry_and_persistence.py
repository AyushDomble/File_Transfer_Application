"""Integrity, retry and persistence integration tests.
Deliberately corrupted chunks exercise bounded retransmission, failure isolation
and successful recovery. SQLite records track confirmed state and offsets.
Reconstructed managers and schedulers resume using peer-confirmed progress."""

from __future__ import annotations
from helpers.persistent_peers import (CHUNK_SIZE, Peer, _corrupt_data_frames, _make_peers, _wait_for_offset_at_least, _wait_for_persisted, _wait_for_state, _write_random_file)

import os
import random
import socket
import struct
import threading
import time

import pytest

from filetransfer.engine import (
    DEFAULT_CHUNK_SIZE,
    FrameRouter,
    SendEngine,
    teardown_session,
)
from filetransfer.manager import TransferManager
from filetransfer.persistence import PersistenceStore, restore_transfer
from filetransfer.session_protocol import Session
from filetransfer.scheduler import ConcurrentScheduler
from filetransfer.transfer import TransferState
from filetransfer.transport import FrameType








@pytest.fixture()
def peers(tmp_path):
    client, server = _make_peers(tmp_path)
    yield client, server
    client.teardown()
    server.teardown()












# ═══════════════════════════════════════════════════════════════════════
# deliberately corrupted chunks
# ═══════════════════════════════════════════════════════════════════════

def test_transient_chunk_corruption_recovers_via_retry(peers, tmp_path):
    """A chunk is corrupted on its first transmission only. The
    receiver's checksum check must catch it, request a retransmit, and
    the sender's retry must deliver the correct bytes — completing
    the transfer with byte-exact, checksum-correct content."""
    client, server = peers
    size = 10 * CHUNK_SIZE + 123
    src = tmp_path / "transient_corrupt.bin"
    data = _write_random_file(str(src), size, seed=30)

    corruption = _corrupt_data_frames(client.session, target_seq=3, times=1)

    transfer = client.scheduler.submit_upload(str(src), chunk_size=CHUNK_SIZE)
    final = _wait_for_state(client.manager, transfer.id,
                             {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
    assert final.state == TransferState.COMPLETED, final.error
    assert corruption["applied"] == 1, "corruption injection never fired"

    dest = os.path.join(server.scheduler._dest_dir, "transient_corrupt.bin")
    with open(dest, "rb") as f:
        received = f.read()
    assert received == data


def test_persistent_chunk_corruption_exhausts_retries_and_fails(peers, tmp_path):
    """Every transmission of a specific chunk is corrupted — the
    sender should exhaust MAX_CHUNK_RETRIES and report FAILED, not
    hang, not silently succeed, and not affect the session or other
    concurrent transfers."""
    client, server = peers
    size = 10 * CHUNK_SIZE + 55
    src = tmp_path / "always_corrupt.bin"
    _write_random_file(str(src), size, seed=31)

    # A second, healthy transfer running concurrently — proves the
    # failure is isolated (error isolation's invariant), not just that this
    # one transfer fails.
    healthy_src = tmp_path / "healthy.bin"
    healthy_data = _write_random_file(str(healthy_src), 6 * CHUNK_SIZE, seed=32)

    corruption = _corrupt_data_frames(client.session, target_seq=3, times=999)

    bad_transfer = client.scheduler.submit_upload(str(src), chunk_size=CHUNK_SIZE)
    corruption["transfer_id"] = bad_transfer.id  # scope to this transfer only —
    # sequence numbers are per-transfer, so the healthy transfer below
    # will also pass through its own seq=3 and must not be touched.
    good_transfer = client.scheduler.submit_upload(str(healthy_src), chunk_size=CHUNK_SIZE)

    final_bad = _wait_for_state(client.manager, bad_transfer.id,
                                 {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
    assert final_bad.state == TransferState.FAILED
    assert final_bad.error and "checksum" in final_bad.error.lower()
    assert corruption["applied"] >= 1

    final_good = _wait_for_state(client.manager, good_transfer.id,
                                  {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
    assert final_good.state == TransferState.COMPLETED, final_good.error
    dest = os.path.join(server.scheduler._dest_dir, "healthy.bin")
    with open(dest, "rb") as f:
        assert f.read() == healthy_data

    # Session must still be usable — one transfer's failure must never
    # look like a session failure.
    assert not client.session.closed
    assert not server.session.closed


# ═══════════════════════════════════════════════════════════════════════
# retry — persistence actually captures what's happening
# ═══════════════════════════════════════════════════════════════════════

def test_offset_and_state_persisted_during_transfer(tmp_path):
    db_path = str(tmp_path / "client.db")
    store = PersistenceStore(db_path)
    client, server = _make_peers(tmp_path, client_persistence=store)
    try:
        size = 20 * CHUNK_SIZE
        src = tmp_path / "persisted.bin"
        data = _write_random_file(str(src), size, seed=40)

        def slow_cb(offset, total):
            time.sleep(0.003)

        transfer = client.scheduler.submit_upload(
            str(src), chunk_size=CHUNK_SIZE, progress_cb=slow_cb
        )
        _wait_for_offset_at_least(client.manager, transfer.id, 5 * CHUNK_SIZE)

        # The in-memory offset (what _wait_for_offset_at_least just
        # polled) and the persisted one are updated by two separate,
        # sequential calls from the engine's thread — update_progress()
        # then persistence.update_offset() — so there's always a brief
        # window where the former is already visible but the latter
        # hasn't committed yet. Poll rather than reading once.
        row = _wait_for_persisted(
            store, transfer.id,
            lambda r: r is not None and r["offset"] >= 5 * CHUNK_SIZE,
        )
        assert row["filename"] == "persisted.bin"
        assert row["total_size"] == size
        assert row["state"] == "active"

        final = _wait_for_state(client.manager, transfer.id,
                                 {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
        assert final.state == TransferState.COMPLETED, final.error

        dest = os.path.join(server.scheduler._dest_dir, "persisted.bin")
        with open(dest, "rb") as f:
            assert f.read() == data

        # Completed transfers aren't "recoverable" — nothing to resume
        # — so the record is cleaned up, not left behind forever.
        _wait_for_persisted(store, transfer.id, lambda row: row is None)
    finally:
        client.teardown()
        server.teardown()


def test_failed_transfer_remains_persisted_for_retry(tmp_path):
    db_path = str(tmp_path / "client.db")
    store = PersistenceStore(db_path)
    client, server = _make_peers(tmp_path, client_persistence=store)
    try:
        size = 10 * CHUNK_SIZE
        src = tmp_path / "fails.bin"
        _write_random_file(str(src), size, seed=41)

        _corrupt_data_frames(client.session, target_seq=2, times=999)

        transfer = client.scheduler.submit_upload(str(src), chunk_size=CHUNK_SIZE)
        final = _wait_for_state(client.manager, transfer.id,
                                 {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
        assert final.state == TransferState.FAILED

        # Unlike COMPLETED/CANCELLED, a FAILED transfer IS recoverable —
        # RECOVERABLE_STATES includes it — so its record must survive,
        # available for a future retry, not get silently deleted.
        row = _wait_for_persisted(store, transfer.id, lambda r: r is not None)
        assert row["state"] == "failed"

        interrupted = store.load_interrupted()
        assert any(r["id"] == transfer.id for r in interrupted)
    finally:
        client.teardown()
        server.teardown()


def test_restart_and_resume_from_persistence(tmp_path):
    """The core retry claim, proven end-to-end: kill every in-memory
    object the original process had for this transfer (manager,
    scheduler, engine — a real restart would lose all of it), rebuild
    a transfer purely from what PersistenceStore has on disk in a
    brand-new TransferManager, and resume it to completion using the
    peer's confirmed offset. The receiving side is never restarted —
    only the sending side loses its memory, which is exactly the
    scenario persistence exists to protect against.
    """
    db_path = str(tmp_path / "client.db")
    store = PersistenceStore(db_path)
    client, server = _make_peers(tmp_path, client_persistence=store)

    size = 30 * CHUNK_SIZE + 777
    src = tmp_path / "restart_me.bin"
    data = _write_random_file(str(src), size, seed=42)

    def slow_cb(offset, total):
        time.sleep(0.004)

    transfer = client.scheduler.submit_upload(
        str(src), chunk_size=CHUNK_SIZE, progress_cb=slow_cb
    )
    _wait_for_offset_at_least(client.manager, transfer.id, 8 * CHUNK_SIZE)
    client.scheduler.pause(transfer.id)
    paused = _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED})
    saved_offset = paused.offset
    assert 0 < saved_offset < size

    # Confirm the persisted row agrees before we throw away everything
    # that knows about the transfer in memory.
    row = _wait_for_persisted(store, transfer.id, lambda r: r is not None and r["state"] == "paused")
    assert row["offset"] == saved_offset

    # ── simulate a restart: stop only the reader thread (the socket
    # itself survives — a real restart would lose the connection too,
    # but re-establishing one is recovery's job, not retry's) and
    # drop every reference to the old manager/scheduler/engine. ──────
    client.router.stop_reader()
    old_threads = client.scheduler._job_threads()
    for job in list(client.scheduler._jobs.values()):
        if job.engine is not None:
            job.engine.abort_connection_lost()
    for thread in old_threads:
        thread.join(2)
        assert not thread.is_alive()

    new_manager = TransferManager(max_concurrent=3)
    new_router = FrameRouter(client.session)
    new_scheduler = ConcurrentScheduler(
        client.session, new_manager, new_router,
        dest_dir=str(tmp_path / "client_files"), persistence=store,
    )
    new_router.start_reader()

    interrupted = store.load_interrupted()
    assert len(interrupted) == 1
    restored = restore_transfer(interrupted[0], new_manager)
    assert restored is not None
    assert restored.filename == "restart_me.bin"
    assert restored.total_size == size
    assert restored.offset == saved_offset
    assert restored.state == TransferState.PAUSED

    new_scheduler.resume_restored_upload(restored, str(src), chunk_size=CHUNK_SIZE)
    new_scheduler.resume(restored.id)

    final = _wait_for_state(new_manager, restored.id,
                             {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
    assert final.state == TransferState.COMPLETED, final.error

    dest = os.path.join(server.scheduler._dest_dir, "restart_me.bin")
    with open(dest, "rb") as f:
        received = f.read()
    assert received == data
    _wait_for_persisted(store, restored.id, lambda r: r is None)  # cleaned up after completion

    new_scheduler.stop_accept_loop()
    new_router.stop()
    server.teardown()
