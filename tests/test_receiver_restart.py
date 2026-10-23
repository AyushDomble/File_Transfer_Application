"""
persistence — receiver-side persistence: restart and resume.

test_retry_and_persistence.py proves the SENDER can lose every in-memory
object, rebuild a transfer from PersistenceStore and resume it. This
file proves the mirror image: the RECEIVER (RecvEngine) can.

As in the sender test, the socket itself survives the simulated
restart — re-establishing a dropped connection is recovery's job. What
is discarded is every in-memory object on the restarting side: manager,
scheduler, router, engine. Only PersistenceStore and the .part file on
disk remain, and the sender (still alive, paused) is told nothing.
"""

from __future__ import annotations

import os
import time

import pytest

from filetransfer.engine import DEFAULT_CHUNK_SIZE, FrameRouter
from filetransfer.manager import TransferManager
from filetransfer.persistence import (
    PersistenceStore,
    reconcile_offset,
    restore_transfer,
)
from filetransfer.scheduler import ConcurrentScheduler
from filetransfer.transfer import TransferDirection, TransferState
from filetransfer.manager import TransferStateError

from helpers.persistent_peers import (
    CHUNK_SIZE,
    _make_peers,
    _wait_for_offset_at_least,
    _wait_for_persisted,
    _wait_for_state,
    _write_random_file,
)


def _slow(offset, total):
    time.sleep(0.004)


def _restart(side, tmp_path, store, dest_dir):
    """Discard everything in memory on `side` and return a fresh
    (manager, router, scheduler) on the same, still-open socket."""
    side.scheduler.stop_accept_loop()
    side.router.stop_reader()
    # A real process exit closes every file and terminates every engine.
    # Merely discarding a router leaves the old paused writer alive.
    threads = side.scheduler._job_threads()
    for job in list(side.scheduler._jobs.values()):
        if job.engine is not None:
            job.engine.abort_connection_lost()
    for thread in threads:
        thread.join(2)
        assert not thread.is_alive(), 'old engine survived simulated restart'
    from filetransfer.transferfiles import release_destination
    for transfer in side.manager.all_transfers():
        release_destination(transfer)  # a process restart also loses its registry
    manager = TransferManager(max_concurrent=3)
    router = FrameRouter(side.session)
    scheduler = ConcurrentScheduler(
        side.session, manager, router, dest_dir=dest_dir, persistence=store,
    )
    router.start_reader()
    return manager, router, scheduler


# ═══════════════════════════════════════════════════════════════════════
# reconcile_offset — the disk-vs-database disagreement rules
# ═══════════════════════════════════════════════════════════════════════

def test_reconcile_offset_missing_part_file_restarts_from_zero(tmp_path):
    assert reconcile_offset(str(tmp_path / "nope.part"), 5000) == 0


def test_reconcile_offset_file_longer_is_truncated_to_saved_offset(tmp_path):
    p = tmp_path / "a.part"
    p.write_bytes(b"x" * 1000)
    assert reconcile_offset(str(p), 600) == 600
    assert p.stat().st_size == 600


def test_reconcile_offset_file_shorter_wins(tmp_path):
    p = tmp_path / "a.part"
    p.write_bytes(b"x" * 300)
    assert reconcile_offset(str(p), 900) == 300
    assert p.stat().st_size == 300


def test_reconcile_offset_agreeing_values_untouched(tmp_path):
    p = tmp_path / "a.part"
    p.write_bytes(b"x" * 512)
    assert reconcile_offset(str(p), 512) == 512
    assert p.stat().st_size == 512


# ═══════════════════════════════════════════════════════════════════════
# Receiver restarts while an upload to it is paused
# ═══════════════════════════════════════════════════════════════════════

def _start_paused_upload(tmp_path, name="recv_restart.bin", chunks=30, seed=7):
    """Client uploads to a persistence-enabled server; returns everything
    once both sides are PAUSED and the server's row reflects it."""
    server_store = PersistenceStore(str(tmp_path / "server.db"))
    client, server = _make_peers(tmp_path, server_persistence=server_store)

    size = chunks * CHUNK_SIZE + 555
    src = tmp_path / name
    data = _write_random_file(str(src), size, seed=seed)

    transfer = client.scheduler.submit_upload(
        str(src), chunk_size=CHUNK_SIZE, progress_cb=_slow
    )
    _wait_for_offset_at_least(client.manager, transfer.id, 8 * CHUNK_SIZE)
    client.scheduler.pause(transfer.id)
    _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED})
    server_t = _wait_for_state(server.manager, transfer.id, {TransferState.PAUSED})
    row = _wait_for_persisted(
        server_store, transfer.id,
        lambda r: r is not None and r["state"] == "paused",
    )
    assert row["direction"] == "download"
    assert row["part_path"] and row["part_path"].endswith(".part")
    assert 0 < row["offset"] < size
    return client, server, server_store, transfer, server_t, row, data, size


def test_receiver_restart_resumes_upload_to_completion(tmp_path):
    client, server, store, transfer, _st, row, data, size = _start_paused_upload(tmp_path)
    try:
        saved_offset = row["offset"]
        part_path = row["part_path"]
        assert os.path.isfile(part_path)

        manager, router, scheduler = _restart(
            server, tmp_path, store, str(tmp_path / "server_files"))

        interrupted = store.load_interrupted()
        assert [r["id"] for r in interrupted] == [transfer.id]
        restored = restore_transfer(interrupted[0], manager)
        assert restored is not None
        assert restored.state == TransferState.PAUSED
        assert restored.direction == TransferDirection.DOWNLOAD
        assert restored.offset == saved_offset

        offset = scheduler.resume_restored_download(restored, row["part_path"])
        assert offset == saved_offset
        scheduler.resume(restored.id)

        final = _wait_for_state(
            manager, restored.id,
            {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
        assert final.state == TransferState.COMPLETED, final.error

        dest = os.path.join(str(tmp_path / "server_files"), "recv_restart.bin")
        assert open(dest, "rb").read() == data
        assert not os.path.exists(part_path)          # .part promoted, not left behind
        _wait_for_persisted(store, restored.id, lambda r: r is None)

        # The sender also finished, and neither session was disturbed.
        cfinal = _wait_for_state(client.manager, transfer.id,
                                 {TransferState.COMPLETED, TransferState.FAILED})
        assert cfinal.state == TransferState.COMPLETED, cfinal.error
        assert not server.session.closed if hasattr(server.session, "closed") else True

        scheduler.stop_accept_loop()
        router.stop()
    finally:
        client.teardown()
        server.teardown()


def test_receiver_restart_with_lost_part_file_restarts_from_zero(tmp_path):
    client, server, store, transfer, _st, row, data, size = _start_paused_upload(
        tmp_path, name="lost_part.bin", seed=11)
    try:
        manager, router, scheduler = _restart(
            server, tmp_path, store, str(tmp_path / "server_files"))
        os.remove(row["part_path"])  # after the old process released its Windows handle
        restored = restore_transfer(store.load_interrupted()[0], manager)
        assert scheduler.resume_restored_download(restored, row["part_path"]) == 0
        scheduler.resume(restored.id)

        final = _wait_for_state(
            manager, restored.id,
            {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
        assert final.state == TransferState.COMPLETED, final.error
        dest = os.path.join(str(tmp_path / "server_files"), "lost_part.bin")
        assert open(dest, "rb").read() == data   # whole file re-sent, byte-exact

        scheduler.stop_accept_loop()
        router.stop()
    finally:
        client.teardown()
        server.teardown()


def test_receiver_restart_discards_unrecorded_tail_bytes(tmp_path):
    """A crash can leave bytes in the .part file the database never
    recorded. They must be truncated, not trusted."""
    client, server, store, transfer, _st, row, data, size = _start_paused_upload(
        tmp_path, name="tail.bin", seed=13)
    try:
        with open(row["part_path"], "ab") as fh:
            fh.write(b"\xde\xad\xbe\xef" * 100)      # garbage past the saved offset

        manager, router, scheduler = _restart(
            server, tmp_path, store, str(tmp_path / "server_files"))
        restored = restore_transfer(store.load_interrupted()[0], manager)
        assert scheduler.resume_restored_download(restored, row["part_path"]) == row["offset"]
        assert os.path.getsize(row["part_path"]) == row["offset"]
        scheduler.resume(restored.id)

        final = _wait_for_state(
            manager, restored.id,
            {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
        assert final.state == TransferState.COMPLETED, final.error
        dest = os.path.join(str(tmp_path / "server_files"), "tail.bin")
        assert open(dest, "rb").read() == data

        scheduler.stop_accept_loop()
        router.stop()
    finally:
        client.teardown()
        server.teardown()


def test_restored_download_can_be_cancelled_and_cleans_up(tmp_path):
    client, server, store, transfer, _st, row, data, size = _start_paused_upload(
        tmp_path, name="cancel_me.bin", seed=17)
    try:
        manager, router, scheduler = _restart(
            server, tmp_path, store, str(tmp_path / "server_files"))
        restored = restore_transfer(store.load_interrupted()[0], manager)
        scheduler.resume_restored_download(restored, row["part_path"])
        scheduler.resume(restored.id)
        _wait_for_state(manager, restored.id, {TransferState.ACTIVE, TransferState.COMPLETED})
        scheduler.cancel(restored.id)

        final = _wait_for_state(
            manager, restored.id,
            {TransferState.CANCELLED, TransferState.COMPLETED}, timeout=10)
        if final.state == TransferState.CANCELLED:
            _wait_for_persisted(store, restored.id, lambda r: r is None)
            deadline = time.time() + 5
            while os.path.exists(row["part_path"]) and time.time() < deadline:
                time.sleep(0.02)
            assert not os.path.exists(row["part_path"])
        # The client's transfer is cancelled too, and its session survives.
        cfinal = _wait_for_state(
            client.manager, transfer.id,
            {TransferState.CANCELLED, TransferState.COMPLETED}, timeout=10)
        assert cfinal.state in (TransferState.CANCELLED, TransferState.COMPLETED)

        scheduler.stop_accept_loop()
        router.stop()
    finally:
        client.teardown()
        server.teardown()


# ═══════════════════════════════════════════════════════════════════════
# Receiver restarts while a download it PULLED is paused
# ═══════════════════════════════════════════════════════════════════════

def test_receiver_restart_resumes_pulled_download(tmp_path):
    """Same guarantee where the restarting side initiated the transfer
    (submit_download) rather than accepting an inbound upload."""
    client_store = PersistenceStore(str(tmp_path / "client.db"))
    client, server = _make_peers(tmp_path, client_persistence=client_store)

    # The serving side builds its SendEngine with the default chunk
    # size (the requested chunk_size only reaches engines this side
    # creates), so the file must span several DEFAULT-size chunks for a
    # pause to land mid-transfer; the receiver's callback throttles it.
    size = 6 * DEFAULT_CHUNK_SIZE + 321
    served = os.path.join(server.scheduler._dest_dir, "pull_me.bin")
    data = _write_random_file(served, size, seed=23)

    dl = client.scheduler.submit_download(
        "pull_me.bin", dest_dir=str(tmp_path / "client_files"),
        progress_cb=lambda o, t: time.sleep(0.15))
    _wait_for_offset_at_least(client.manager, dl.id, 2 * DEFAULT_CHUNK_SIZE)
    client.scheduler.pause(dl.id)
    _wait_for_state(client.manager, dl.id, {TransferState.PAUSED})
    row = _wait_for_persisted(
        client_store, dl.id, lambda r: r is not None and r["state"] == "paused")
    assert row["part_path"] and 0 < row["offset"] < size
    try:
        manager, router, scheduler = _restart(
            client, tmp_path, client_store, str(tmp_path / "client_files"))
        restored = restore_transfer(client_store.load_interrupted()[0], manager)
        scheduler.resume_restored_download(restored, row["part_path"])
        scheduler.resume(restored.id)

        final = _wait_for_state(
            manager, restored.id,
            {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
        assert final.state == TransferState.COMPLETED, final.error
        got = os.path.join(str(tmp_path / "client_files"), "pull_me.bin")
        assert open(got, "rb").read() == data
        _wait_for_persisted(client_store, restored.id, lambda r: r is None)

        scheduler.stop_accept_loop()
        router.stop()
    finally:
        client.teardown()
        server.teardown()


# ═══════════════════════════════════════════════════════════════════════
# Guard rails
# ═══════════════════════════════════════════════════════════════════════

def test_resume_restored_download_rejects_non_paused_transfer(tmp_path):
    client, server = _make_peers(tmp_path)
    try:
        t = client.manager.create_transfer(
            direction=TransferDirection.DOWNLOAD, filename="x.bin",
            total_size=10, start_immediately=False)
        with pytest.raises(TransferStateError):
            client.scheduler.resume_restored_download(t, str(tmp_path / "x.bin.part"))
    finally:
        client.teardown()
        server.teardown()


def test_resume_restored_download_rejects_upload_direction(tmp_path):
    client, server = _make_peers(tmp_path)
    try:
        t = client.manager.create_transfer(
            direction=TransferDirection.UPLOAD, filename="x.bin",
            total_size=10, start_immediately=False)
        client.manager.start_transfer(t.id)
        client.manager.pause_transfer(t.id)
        client.manager.confirm_paused(t.id)
        assert t.state == TransferState.PAUSED
        with pytest.raises(TransferStateError):
            client.scheduler.resume_restored_download(t, str(tmp_path / "x.bin.part"))
    finally:
        client.teardown()
        server.teardown()


def test_persistence_upsert_refreshes_total_size_but_never_with_zero(tmp_path):
    """A transfer first saved with an unknown size (0) must pick up the
    real size on a later save, and a later 0 must not erase it."""
    store = PersistenceStore(str(tmp_path / "s.db"))
    now = time.time()
    args = dict(direction="download", filename="f.bin", checksum=None,
                state="active", error=None, created_at=now)
    store.save_snapshot("tid1", total_size=0, offset=0, **args)
    assert store.load("tid1")["total_size"] == 0
    store.save_snapshot("tid1", total_size=1000, offset=10, **args)
    assert store.load("tid1")["total_size"] == 1000
    store.save_snapshot("tid1", total_size=0, offset=20, **args)
    row = store.load("tid1")
    assert row["total_size"] == 1000 and row["offset"] == 20
