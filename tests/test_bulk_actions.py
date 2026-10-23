"""
bulk actions -- global (apply-to-every-transfer) actions alongside the
existing per-transfer ones: scheduler.cancel_all()/pause_all()/
resume_all()/retry_all(), the engine-aware counterparts to
TransferManager.cancel_all(). Regression tests verify that manager
cancellation reaches live engines and that bulk actions preserve
per-transfer lifecycle guarantees.
"""

from __future__ import annotations

import os
import time

import pytest

from filetransfer.manager import TransferNotFound
from filetransfer.transfer import TransferState
from filetransfer.transferfiles import partial_path

from helpers.scheduler_peers import peers, _wait_for_state, _write_random_file  # noqa: F401 (fixture)


def _slow(offset, total):
    time.sleep(0.03)


def _submit_many(client, tmp_path, n, chunks=12, chunk_size=256 * 1024, prefix="f", **kw):
    items = []
    for i in range(n):
        src = tmp_path / f"{prefix}{i}.bin"
        _write_random_file(str(src), chunks * chunk_size)
        t = client.scheduler.submit_upload(str(src), chunk_size=chunk_size, progress_cb=_slow, **kw)
        items.append((t, src))
    return items


# ═══════════════════════════════════════════════════════════════════════
# Bulk cancellation invariant: TransferManager.cancel_all() used
# directly on a live connection leaves the engine running, oblivious.
# ═══════════════════════════════════════════════════════════════════════

def test_manager_cancel_all_stops_a_live_engine(peers, tmp_path):
    """The scheduler listener now propagates manager cancellation to I/O."""
    client, server = peers
    src = tmp_path / "oblivious.bin"
    _write_random_file(str(src), 6 * 1024 * 1024)
    t = client.scheduler.submit_upload(str(src), progress_cb=_slow)
    _wait_for_state(client.manager, t.id, {TransferState.ACTIVE})

    n = client.manager.cancel_all()
    assert n == 1
    assert client.manager.get(t.id).state == TransferState.CANCELLED

    part = partial_path(os.path.join(str(tmp_path / "server_files"), "oblivious.bin"), t.id)
    done = os.path.join(str(tmp_path / "server_files"), "oblivious.bin")
    assert t.io_finished.wait(2)
    deadline = time.time() + 2
    while os.path.exists(part) and time.time() < deadline:
        time.sleep(0.01)
    assert not os.path.exists(part) and not os.path.exists(done)
    assert not client.session.closed and not server.session.closed


# ═══════════════════════════════════════════════════════════════════════
# scheduler.cancel_all() -- the fix
# ═══════════════════════════════════════════════════════════════════════

def test_scheduler_cancel_all_actually_stops_live_engines(peers, tmp_path):
    client, server = peers
    items = _submit_many(client, tmp_path, 3, prefix="cxall")
    for t, _ in items:
        _wait_for_state(client.manager, t.id, {TransferState.ACTIVE, TransferState.QUEUED})

    n = client.scheduler.cancel_all()
    assert n == 3
    for t, src in items:
        final = _wait_for_state(client.manager, t.id, {TransferState.CANCELLED})
        assert final.state == TransferState.CANCELLED
    # Unlike the bare-manager version above, the .part files must
    # actually be gone -- proof the real cancel cleanup ran.
    time.sleep(0.3)
    for transfer, src in items:
        part = partial_path(os.path.join(str(tmp_path / "server_files"), src.name), transfer.id)
        assert not os.path.exists(part), f"{part} was left behind"


def test_scheduler_cancel_all_ignores_already_terminal_transfers(peers, tmp_path):
    client, server = peers
    done_src = tmp_path / "already_done.bin"
    _write_random_file(str(done_src), 10_000)
    done_t = client.scheduler.submit_upload(str(done_src))
    _wait_for_state(client.manager, done_t.id, {TransferState.COMPLETED})

    live_src = tmp_path / "still_going.bin"
    _write_random_file(str(live_src), 4 * 1024 * 1024)
    live_t = client.scheduler.submit_upload(str(live_src), progress_cb=_slow)
    _wait_for_state(client.manager, live_t.id, {TransferState.ACTIVE})

    n = client.scheduler.cancel_all()
    assert n == 1  # only the live one
    assert client.manager.get(done_t.id).state == TransferState.COMPLETED  # untouched
    assert client.manager.get(live_t.id).state == TransferState.CANCELLED


def test_scheduler_cancel_all_returns_zero_when_nothing_to_cancel(peers):
    client, server = peers
    assert client.scheduler.cancel_all() == 0


# ═══════════════════════════════════════════════════════════════════════
# scheduler.pause_all() / resume_all()
# ═══════════════════════════════════════════════════════════════════════

def test_pause_all_then_resume_all(peers, tmp_path):
    client, server = peers
    items = _submit_many(client, tmp_path, 3, prefix="pzall", chunks=60, chunk_size=256 * 1024)
    for t, _ in items:
        _wait_for_state(client.manager, t.id, {TransferState.ACTIVE, TransferState.QUEUED})
    # let at least the active ones make real progress before pausing
    deadline = time.time() + 5
    while time.time() < deadline and not all(t.offset > 0 for t, _ in items if t.state == TransferState.ACTIVE):
        time.sleep(0.02)

    n = client.scheduler.pause_all()
    assert n == 3
    for t, _ in items:
        final = _wait_for_state(client.manager, t.id, {TransferState.PAUSED})
        assert final.state == TransferState.PAUSED

    n2 = client.scheduler.resume_all()
    assert n2 == 3
    for t, src in items:
        final = _wait_for_state(client.manager, t.id,
                                {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
        assert final.state == TransferState.COMPLETED, final.error
        dest = os.path.join(str(tmp_path / "server_files"), src.name)
        assert open(dest, "rb").read() == src.read_bytes()


def test_pause_all_skips_already_paused_and_terminal(peers, tmp_path):
    client, server = peers
    done_src = tmp_path / "finished.bin"
    _write_random_file(str(done_src), 5_000)
    done_t = client.scheduler.submit_upload(str(done_src))
    _wait_for_state(client.manager, done_t.id, {TransferState.COMPLETED})

    live_src = tmp_path / "running.bin"
    _write_random_file(str(live_src), 4 * 1024 * 1024)
    live_t = client.scheduler.submit_upload(str(live_src), progress_cb=_slow)
    _wait_for_state(client.manager, live_t.id, {TransferState.ACTIVE})

    assert client.scheduler.pause_all() == 1
    _wait_for_state(client.manager, live_t.id, {TransferState.PAUSED})

    # Calling it again with nothing newly QUEUED/ACTIVE should do
    # nothing -- the one transfer is already PAUSED now.
    assert client.scheduler.pause_all() == 0
    assert client.manager.get(done_t.id).state == TransferState.COMPLETED


def test_resume_all_catches_a_transfer_still_pausing(peers, tmp_path):
    """Regression for the bulk-action race: pause_all()
    returning success only means pause was REQUESTED -- reaching
    PAUSED is asynchronous (the engine needs a safe checkpoint first).
    Calling resume_all() in that narrow window, while a transfer is
    still PAUSING, must still catch and resume it rather than silently
    skipping it (engine.resume() has no state gate of its own and is
    built to handle exactly this -- see scheduler.resume_all()'s
    docstring) -- otherwise a user clicking "Resume all" shortly after
    "Pause all" could strand a transfer in PAUSED with nothing left to
    resume it."""
    client, server = peers
    src = tmp_path / "racey.bin"
    _write_random_file(str(src), 6 * 1024 * 1024)
    t = client.scheduler.submit_upload(str(src), progress_cb=_slow)
    deadline = time.time() + 5
    while time.time() < deadline and t.offset == 0:
        time.sleep(0.01)
    assert t.offset > 0

    assert client.scheduler.pause_all() == 1
    # Deliberately NOT waiting for PAUSED here -- that's the race.
    assert t.state in (TransferState.PAUSING, TransferState.PAUSED)
    n = client.scheduler.resume_all()
    assert n == 1, f"resume_all() skipped a transfer still in {t.state} -- the race was not actually caught"

    final = _wait_for_state(client.manager, t.id,
                            {TransferState.COMPLETED, TransferState.FAILED}, timeout=15)
    assert final.state == TransferState.COMPLETED, final.error
    dest = os.path.join(str(tmp_path / "server_files"), "racey.bin")
    assert open(dest, "rb").read() == src.read_bytes()


def test_resume_all_returns_zero_when_nothing_paused(peers):
    client, server = peers
    assert client.scheduler.resume_all() == 0


def test_resume_all_skips_passive_dormant_transfers(tmp_path):
    """A transfer restored after THIS side's own restart, left PAUSED
    by a peer it hasn't reconnected to yet, is "passive dormant" (see
    scheduler.resume()'s guard) -- not this scheduler's to resume on
    its own; only the peer's reconnection can revive it. resume_all()
    delegates to resume() for each transfer and swallows exactly the
    TransferStateError that guard raises, so this is really confirming
    that delegation reaches the real guard, not a parallel one -- the
    guard's own correctness (and the full reconnect dance that
    produces this state) is already proven end-to-end by
    test_reconnection.py's test_paused_transfer_stays_paused_across_reconnect.
    """
    import socket
    from filetransfer.engine import FrameRouter
    from filetransfer.manager import TransferManager
    from filetransfer.persistence import PersistenceStore, restore_transfer
    from filetransfer.session_protocol import Session
    from filetransfer.scheduler import ConcurrentScheduler

    store = PersistenceStore(str(tmp_path / "s.db"))
    now = time.time()
    store.save_snapshot(
        "deadbeef0000111122223333444455556666", total_size=1000, offset=400,
        direction="download", filename="f.bin", checksum=None,
        state="paused", error=None, created_at=now, part_path=str(tmp_path / "f.bin.part"),
    )
    manager = TransferManager(max_concurrent=3)
    a, b = socket.socketpair()
    session = Session(a, ("127.0.0.1", 0), name="client")
    router = FrameRouter(session)
    scheduler = ConcurrentScheduler(session, manager, router, dest_dir=str(tmp_path), persistence=store)

    restored = restore_transfer(store.load_interrupted()[0], manager, connection_lost=True)
    manager.requeue_transfer(restored.id)
    manager.start_transfer(restored.id)
    manager.pause_transfer(restored.id)
    manager.confirm_paused(restored.id)
    scheduler._passive_dormant.add(restored.id)  # exactly what attach_session() would do for this case

    assert scheduler.resume_all() == 0
    assert manager.get(restored.id).state == TransferState.PAUSED
    router.stop()
    b.close()


# ═══════════════════════════════════════════════════════════════════════
# scheduler.retry_all()
# ═══════════════════════════════════════════════════════════════════════

def test_retry_all(peers, tmp_path):
    client, server = peers
    srcs = []
    for i in range(2):
        src = tmp_path / f"corrupt{i}.bin"
        _write_random_file(str(src), 3 * 1024 * 1024)
        srcs.append(src)

    # Force both into FAILED without a flaky real network corruption:
    # cancel the connection's recovery path isn't needed here -- just
    # use the manager directly to land two transfers in FAILED.
    ts = []
    for src in srcs:
        t = client.manager.create_transfer(
            direction=client.scheduler.submit_upload.__self__ and __import__(
                "filetransfer.transfer", fromlist=["TransferDirection"]).TransferDirection.UPLOAD,
            filename=src.name, total_size=os.path.getsize(src), start_immediately=False)
        client.manager.start_transfer(t.id)
        client.manager.fail_transfer(t.id, "synthetic failure for retry_all test")
        ts.append(t)

    assert client.scheduler.retry_all() == 0  # these have no scheduler job at all
    # (retry_all only re-arms jobs the scheduler itself created via
    # submit_upload/submit_download; a transfer that only ever existed
    # at the manager level was never this scheduler's to retry.)
    for t in ts:
        with pytest.raises(TransferNotFound):
            client.scheduler.retry(t.id)


def test_retry_all_with_real_scheduler_jobs(peers, tmp_path):
    """retry_all() against transfers that actually went through
    submit_upload and then genuinely failed (a blocked extension, via
    the host's own settings) and recover once the obstruction is
    cleared -- the realistic path, unlike the synthetic one above."""
    client, server = peers
    server.scheduler.update_settings(blocked_extensions=".exe")
    srcs = []
    for i in range(2):
        src = tmp_path / f"blocked{i}.exe"
        _write_random_file(str(src), 50_000)
        srcs.append(src)
    items = [(client.scheduler.submit_upload(str(s)), s) for s in srcs]
    for t, _ in items:
        _wait_for_state(client.manager, t.id, {TransferState.FAILED})

    server.scheduler.update_settings(blocked_extensions="")
    n = client.scheduler.retry_all()
    assert n == 2
    for t, src in items:
        final = _wait_for_state(client.manager, t.id,
                                {TransferState.COMPLETED, TransferState.FAILED}, timeout=15)
        assert final.state == TransferState.COMPLETED, final.error
        dest = os.path.join(str(tmp_path / "server_files"), src.name)
        assert open(dest, "rb").read() == src.read_bytes()


def test_retry_all_returns_zero_when_nothing_failed(peers):
    client, server = peers
    assert client.scheduler.retry_all() == 0
