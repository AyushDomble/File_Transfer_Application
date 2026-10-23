"""
recovery — connection loss, reconnection and recovery.

A connection dying is not a transfer failing. These tests drop the link
under live transfers and check that: every affected transfer ends up
CONNECTION_LOST (not FAILED/CANCELLED) with its offset and .part file
intact; a new session can be attached; and the transfers continue from
the receiver's confirmed offset — never from zero — to a byte-exact
result. Also: half-open detection (keepalive), isolation, paused
transfers staying paused, recovery through process restarts, and
refusal of hijacked/mismatched recovery attempts.
"""

from __future__ import annotations

import os
import socket
import threading
import time

import pytest

from filetransfer.engine import FrameRouter
from filetransfer.manager import TransferManager
from filetransfer.persistence import PersistenceStore, restore_transfer
from filetransfer.session_protocol import Session
from filetransfer.scheduler import ConcurrentScheduler
from filetransfer.transfer import (
    Transfer, TransferDirection, TransferState, TransferStateError,
)

from helpers.persistent_peers import (
    CHUNK_SIZE,
    _make_peers,
    _wait_for_offset_at_least,
    _wait_for_persisted,
    _wait_for_state,
    _write_random_file,
)

TERMINAL_OR_OK = {TransferState.COMPLETED, TransferState.FAILED, TransferState.CANCELLED}


def _slow(offset, total):
    time.sleep(0.004)


def _drop(client, server):
    """Kill the link abruptly (no CANCEL/close frames): shut the raw
    socket down. Both ends see EOF."""
    sock = client.session._sock
    getattr(sock, "_s", sock).shutdown(socket.SHUT_RDWR)


def _wait_lost(peer, tid, timeout=10):
    return _wait_for_state(peer.manager, tid, {TransferState.CONNECTION_LOST,
                                               TransferState.COMPLETED,
                                               TransferState.FAILED,
                                               TransferState.CANCELLED}, timeout)


def _new_link():
    """A fresh, negotiated pair of Sessions on a new socketpair."""
    a, b = socket.socketpair()
    cs = Session(a, ("127.0.0.1", 0), name="client")
    ss = Session(b, ("127.0.0.1", 0), name="server")
    errs = {}

    def _c():
        try: cs.negotiate_as_client()
        except Exception as e: errs["c"] = e

    def _s():
        try: ss.negotiate_as_server()
        except Exception as e: errs["s"] = e

    tc, ts = threading.Thread(target=_c), threading.Thread(target=_s)
    tc.start(); ts.start(); tc.join(3); ts.join(3)
    assert not errs, errs
    cs.mark_authenticated()
    ss.mark_authenticated()
    return cs, ss


def _reconnect(client, server, keepalive=None):
    """Attach a new link to both peers (server first, so its accept loop
    is ready), returning what the client re-queued."""
    cs, ss = _new_link()
    sr, cr = FrameRouter(ss), FrameRouter(cs)
    server.scheduler.attach_session(ss, sr, keepalive=keepalive)
    server.session, server.router = ss, sr
    recovered = client.scheduler.attach_session(cs, cr, keepalive=keepalive)
    client.session, client.router = cs, cr
    return recovered


@pytest.fixture()
def peers(tmp_path):
    made = {}

    def _mk(**kw):
        c, s = _make_peers(tmp_path, **kw)
        made["p"] = (c, s)
        return c, s

    yield _mk
    if "p" in made:
        for side in made["p"]:
            try:
                side.teardown()
            except Exception:
                pass


def _start_upload(client, tmp_path, name, chunks=60, seed=1, **kw):
    size = chunks * CHUNK_SIZE + 321
    src = tmp_path / name
    data = _write_random_file(str(src), size, seed=seed)
    t = client.scheduler.submit_upload(str(src), chunk_size=CHUNK_SIZE,
                                       progress_cb=_slow, **kw)
    return t, data, size


# ═══════════════════════════════════════════════════════════════════════
# State machine
# ═══════════════════════════════════════════════════════════════════════

def test_connection_lost_transitions():
    for src in (TransferState.ACTIVE, TransferState.PAUSING,
                TransferState.PAUSED, TransferState.RETRYING):
        t = Transfer(state=src)
        t.set_state(TransferState.CONNECTION_LOST)
    t = Transfer(state=TransferState.CONNECTION_LOST)
    assert not t.is_terminal and not t.is_running
    t.set_state(TransferState.QUEUED)
    for dst in (TransferState.CANCELLED, TransferState.FAILED):
        Transfer(state=TransferState.CONNECTION_LOST).set_state(dst)
    # Illegal: can't jump straight to ACTIVE/COMPLETED; queued can't be "lost".
    for dst in (TransferState.ACTIVE, TransferState.COMPLETED, TransferState.PAUSED):
        with pytest.raises(TransferStateError):
            Transfer(state=TransferState.CONNECTION_LOST).set_state(dst)
    with pytest.raises(TransferStateError):
        Transfer(state=TransferState.QUEUED).set_state(TransferState.CONNECTION_LOST)


def test_mark_connection_lost_is_idempotent_and_spares_terminal(tmp_path):
    m = TransferManager(max_concurrent=2)
    t = m.create_transfer(TransferDirection.UPLOAD, "a.bin", 10, start_immediately=False)
    assert m.mark_connection_lost(t.id) is False          # QUEUED: nothing to lose
    m.start_transfer(t.id)
    assert m.mark_connection_lost(t.id) is True
    assert m.mark_connection_lost(t.id) is False           # already lost
    done = m.create_transfer(TransferDirection.UPLOAD, "b.bin", 10, start_immediately=False)
    m.start_transfer(done.id); m.complete_transfer(done.id)
    assert m.mark_connection_lost(done.id) is False
    assert done.state == TransferState.COMPLETED


# ═══════════════════════════════════════════════════════════════════════
# Drop → CONNECTION_LOST → reconnect → resume
# ═══════════════════════════════════════════════════════════════════════

def test_upload_survives_connection_drop(peers, tmp_path):
    client, server = peers()
    t, data, size = _start_upload(client, tmp_path, "up.bin")
    _wait_for_offset_at_least(client.manager, t.id, 15 * CHUNK_SIZE)

    _drop(client, server)
    assert _wait_lost(client, t.id).state == TransferState.CONNECTION_LOST
    st = _wait_lost(server, t.id)
    assert st.state == TransferState.CONNECTION_LOST
    lost_at = t.offset
    assert 0 < lost_at < size
    part = st.part_path
    assert os.path.isfile(part)                            # kept, not deleted
    assert client.session.is_lost

    assert _reconnect(client, server) == [t.id]

    final = _wait_for_state(client.manager, t.id, TERMINAL_OR_OK, timeout=30)
    assert final.state == TransferState.COMPLETED, final.error
    got = os.path.join(str(tmp_path / "server_files"), "up.bin")
    assert open(got, "rb").read() == data
    assert _wait_for_state(server.manager, t.id, TERMINAL_OR_OK).state == TransferState.COMPLETED
    assert not os.path.exists(part)


def test_recovery_resumes_from_confirmed_offset_not_zero(peers, tmp_path):
    client, server = peers()
    seen = []
    t, data, size = _start_upload(client, tmp_path, "nz.bin", chunks=80, seed=2)
    _wait_for_offset_at_least(client.manager, t.id, 20 * CHUNK_SIZE)
    _drop(client, server)
    _wait_lost(client, t.id); _wait_lost(server, t.id)
    lost_at = t.offset

    # Spy on what the recovered engine actually sends.
    orig = client.scheduler._upload_build
    def spy(*a, **kw):
        make = orig(*a, **kw)
        def wrapped(engine_t):
            e = make(engine_t)
            seen.append(e._resume_offset if hasattr(e, "_resume_offset") else engine_t.offset)
            return e
        return wrapped
    # Rebuild the job through the spy so we can observe the resume offset.
    client.scheduler._jobs[t.id].build = lambda et=t: spy(
        os.path.join(str(tmp_path), "nz.bin"), CHUNK_SIZE, False, _slow)(et)

    _reconnect(client, server)
    final = _wait_for_state(client.manager, t.id, TERMINAL_OR_OK, timeout=30)
    assert final.state == TransferState.COMPLETED, final.error
    assert seen and seen[0] >= lost_at - CHUNK_SIZE, (seen, lost_at)
    assert seen[0] > 0
    got = os.path.join(str(tmp_path / "server_files"), "nz.bin")
    assert open(got, "rb").read() == data


def test_pulled_download_resumes_via_have_offset(peers, tmp_path):
    """have_offset lets the serving side continue from confirmed download progress."""
    client, server = peers()
    size = 60 * CHUNK_SIZE + 77
    served = os.path.join(str(tmp_path / "server_files"), "pull.bin")
    data = _write_random_file(served, size, seed=3)

    dl = client.scheduler.submit_download(
        "pull.bin", dest_dir=str(tmp_path / "client_files"),
        chunk_size=CHUNK_SIZE, progress_cb=lambda o, t: time.sleep(0.02))
    _wait_for_offset_at_least(client.manager, dl.id, 5 * CHUNK_SIZE)
    _drop(client, server)
    _wait_lost(client, dl.id); _wait_lost(server, dl.id)
    lost_at = dl.offset
    assert lost_at > 0

    first_after = []
    job = client.scheduler._jobs[dl.id]
    orig_build = job.build
    def build_spy():
        e = orig_build()
        first_after.append(e._resume_offset if hasattr(e, "_resume_offset") else None)
        return e
    job.build = build_spy

    assert _reconnect(client, server) == [dl.id]
    final = _wait_for_state(client.manager, dl.id, TERMINAL_OR_OK, timeout=30)
    assert final.state == TransferState.COMPLETED, final.error
    assert first_after and first_after[0] is not None and first_after[0] > 0, first_after
    got = os.path.join(str(tmp_path / "client_files"), "pull.bin")
    assert open(got, "rb").read() == data


def test_multiple_transfers_all_recover_and_respect_max_concurrent(peers, tmp_path):
    client, server = peers(max_concurrent=2)
    items = [_start_upload(client, tmp_path, f"m{i}.bin", chunks=40, seed=10 + i)
             for i in range(4)]                       # 2 active, 2 queued
    _wait_for_offset_at_least(client.manager, items[0][0].id, 8 * CHUNK_SIZE)
    _drop(client, server)
    # Outages now freeze admission. Never-started work stays QUEUED instead
    # of starting a doomed handshake just to become CONNECTION_LOST.
    for t, _, _ in items[:2]:
        _wait_lost(client, t.id)
    assert all(t.state == TransferState.QUEUED for t, _, _ in items[2:])

    recovered = _reconnect(client, server)
    assert set(recovered) == {t.id for t, _, _ in items[:2]}

    peak = 0
    deadline = time.time() + 40
    while time.time() < deadline:
        peak = max(peak, client.manager.running_count())
        if all(t.state in TERMINAL_OR_OK for t, _, _ in items):
            break
        time.sleep(0.01)
    assert peak <= 2
    for i, (t, data, _) in enumerate(items):
        assert t.state == TransferState.COMPLETED, (i, t.state, t.error)
        assert open(os.path.join(str(tmp_path / "server_files"), f"m{i}.bin"), "rb").read() == data


def test_completed_transfer_untouched_by_a_later_drop(peers, tmp_path):
    client, server = peers()
    done, ddata, _ = _start_upload(client, tmp_path, "done.bin", chunks=3, seed=4)
    _wait_for_state(client.manager, done.id, TERMINAL_OR_OK)
    live, ldata, _ = _start_upload(client, tmp_path, "live.bin", chunks=60, seed=5)
    _wait_for_offset_at_least(client.manager, live.id, 10 * CHUNK_SIZE)
    _drop(client, server)
    _wait_lost(client, live.id)
    assert done.state == TransferState.COMPLETED
    _reconnect(client, server)
    assert _wait_for_state(client.manager, live.id, TERMINAL_OR_OK, 30).state == TransferState.COMPLETED
    assert done.state == TransferState.COMPLETED


def test_loss_never_fails_or_cancels_anything(peers, tmp_path):
    """No reconnect at all: transfers wait, indefinitely, as CONNECTION_LOST."""
    client, server = peers()
    t, _, _ = _start_upload(client, tmp_path, "wait.bin", chunks=60, seed=6)
    _wait_for_offset_at_least(client.manager, t.id, 10 * CHUNK_SIZE)
    _drop(client, server)
    _wait_lost(client, t.id)
    time.sleep(1.0)
    assert t.state == TransferState.CONNECTION_LOST and t.error is None
    assert server.manager.get(t.id).state == TransferState.CONNECTION_LOST


# ═══════════════════════════════════════════════════════════════════════
# Half-open connections: keepalive
# ═══════════════════════════════════════════════════════════════════════

def test_keepalive_detects_silent_peer(tmp_path):
    a, b = socket.socketpair()                # b: a peer that never answers
    sess = Session(a, ("127.0.0.1", 0), name="client")
    router = FrameRouter(sess)
    router.start_reader()
    lost = threading.Event()
    router.add_loss_listener(lost.set)
    router.start_keepalive(interval=0.2, timeout=0.8)
    assert lost.wait(4), "silent peer was never declared lost"
    assert sess.is_lost and "keepalive" in (router._loss_reason or "")
    b.close(); router.stop(); sess.close_session()


def test_keepalive_does_not_trip_on_a_live_peer(tmp_path):
    cs, ss = _new_link()
    cr, sr = FrameRouter(cs), FrameRouter(ss)
    cr.start_reader(); sr.start_reader()
    cr.start_keepalive(interval=0.2, timeout=1.0)
    time.sleep(2.5)                           # idle, but PING/PONG keeps it alive
    assert not cr.connection_lost.is_set()
    cr.stop(); sr.stop(); cs.close_session(); ss.close_session()


def test_transfers_suspend_when_keepalive_fires(peers, tmp_path):
    client, server = peers()
    client.router.start_keepalive(interval=0.15, timeout=0.6)
    t, _, _ = _start_upload(client, tmp_path, "ka.bin", chunks=200, seed=7)
    _wait_for_offset_at_least(client.manager, t.id, 5 * CHUNK_SIZE)
    server.router.stop_reader()               # server goes deaf: nothing answers PING
    assert _wait_lost(client, t.id, timeout=15).state == TransferState.CONNECTION_LOST


# ═══════════════════════════════════════════════════════════════════════
# Paused transfers stay paused; cancel while lost
# ═══════════════════════════════════════════════════════════════════════

def test_paused_transfer_stays_paused_across_reconnect(peers, tmp_path):
    client, server = peers()
    t, data, size = _start_upload(client, tmp_path, "pz.bin", chunks=60, seed=8)
    _wait_for_offset_at_least(client.manager, t.id, 10 * CHUNK_SIZE)
    client.scheduler.pause(t.id)
    _wait_for_state(client.manager, t.id, {TransferState.PAUSED})
    _wait_for_state(server.manager, t.id, {TransferState.PAUSED})
    paused_at = t.offset

    _drop(client, server)
    time.sleep(0.5)
    assert t.state == TransferState.PAUSED                 # the user's intent survives
    _reconnect(client, server)
    time.sleep(0.5)
    assert t.state == TransferState.PAUSED and t.offset == paused_at

    # The peer (passive side) cannot resume it; the initiator can.
    with pytest.raises(TransferStateError):
        server.scheduler.resume(t.id)
    client.scheduler.resume(t.id)
    final = _wait_for_state(client.manager, t.id, TERMINAL_OR_OK, 30)
    assert final.state == TransferState.COMPLETED, final.error
    assert open(os.path.join(str(tmp_path / "server_files"), "pz.bin"), "rb").read() == data


def test_cancel_while_connection_lost_cleans_up_part_file(peers, tmp_path):
    client, server = peers()
    t, _, _ = _start_upload(client, tmp_path, "cx.bin", chunks=60, seed=9)
    _wait_for_offset_at_least(client.manager, t.id, 10 * CHUNK_SIZE)
    _drop(client, server)
    _wait_lost(client, t.id); _wait_lost(server, t.id)
    part = server.manager.get(t.id).part_path
    assert os.path.isfile(part)
    server.scheduler.cancel(t.id)
    assert server.manager.get(t.id).state == TransferState.CANCELLED
    assert not os.path.exists(part)
    client.scheduler.cancel(t.id)
    assert t.state == TransferState.CANCELLED


# ═══════════════════════════════════════════════════════════════════════
# Recovery edge cases and refusals
# ═══════════════════════════════════════════════════════════════════════

def test_already_complete_on_receiver_finishes_sender_without_resending(peers, tmp_path):
    """The link died after the receiver verified the last byte but before
    the sender saw TRANSFER_COMPLETE."""
    client, server = peers()
    t, data, size = _start_upload(client, tmp_path, "ac.bin", chunks=4, seed=12)
    _wait_for_state(client.manager, t.id, TERMINAL_OR_OK)
    assert server.manager.get(t.id).state == TransferState.COMPLETED

    # Rebuild the client's side as if it never heard COMPLETE.
    lost = client.manager.create_transfer(
        TransferDirection.UPLOAD, "ac.bin", size, checksum=t.checksum,
        transfer_id="f" * 32, start_immediately=False)
    # (a different id would not be known to the server — use the real one)
    client.manager._transfers.pop("f" * 32)
    ghost = client.manager.create_transfer(
        TransferDirection.UPLOAD, "ac2.bin", size, checksum=t.checksum,
        transfer_id=t.id + "x"[:0], start_immediately=False) if False else None

    m2 = TransferManager(max_concurrent=2)
    g = m2.create_transfer(TransferDirection.UPLOAD, "ac.bin", size,
                           checksum=t.checksum, transfer_id=t.id, start_immediately=False)
    g.set_state(TransferState.ACTIVE); g.set_state(TransferState.CONNECTION_LOST)
    cs, ss = _new_link()
    sr, cr = FrameRouter(ss), FrameRouter(cs)
    server.scheduler.attach_session(ss, sr); server.session, server.router = ss, sr
    sched2 = ConcurrentScheduler(cs, m2, cr, dest_dir=str(tmp_path / "client_files"))
    cr.start_reader()
    sched2.reattach_upload(g, str(tmp_path / "ac.bin"), chunk_size=CHUNK_SIZE)
    sched2.recover_transfer(g.id)
    assert _wait_for_state(m2, g.id, TERMINAL_OR_OK, 10).state == TransferState.COMPLETED
    assert g.offset == size
    cr.stop(); cs.close_session()


def test_recreate_with_mismatched_metadata_is_rejected(peers, tmp_path):
    client, server = peers()
    t, _, size = _start_upload(client, tmp_path, "mm.bin", chunks=60, seed=13)
    _wait_for_offset_at_least(client.manager, t.id, 10 * CHUNK_SIZE)
    _drop(client, server)
    _wait_lost(server, t.id)

    cs, ss = _new_link()
    sr, cr = FrameRouter(ss), FrameRouter(cs)
    server.scheduler.attach_session(ss, sr); server.session, server.router = ss, sr
    cr.start_reader()
    # An attacker (or a bug) re-CREATEs the same id under another filename.
    from filetransfer.transport import Frame, FrameType
    cr.register(t.id)  # Install response routing before the peer can reply.
    cs.send_json(FrameType.TRANSFER_CREATE,
                 {"filename": "other.bin", "size": size, "checksum": None,
                  "chunk_size": CHUNK_SIZE, "direction": "upload", "overwrite": False},
                 transfer_id=t.id)
    reply = cr.get_queue(t.id).get(timeout=5)
    assert reply.frame_type == FrameType.TRANSFER_REJECT
    assert server.manager.get(t.id).state == TransferState.CONNECTION_LOST   # untouched
    cr.stop(); cs.close_session()


def test_recreate_of_a_transfer_with_a_live_engine_is_rejected(peers, tmp_path):
    """Replaying the ID of a healthy, running transfer must not hijack it."""
    client, server = peers()
    t, data, size = _start_upload(client, tmp_path, "hj.bin", chunks=80, seed=14)
    _wait_for_offset_at_least(client.manager, t.id, 10 * CHUNK_SIZE)

    from filetransfer.transport import FrameType
    client.session.send_json(
        FrameType.TRANSFER_CREATE,
        {"filename": "hj.bin", "size": size, "checksum": t.checksum,
         "chunk_size": CHUNK_SIZE, "direction": "upload", "overwrite": False},
        transfer_id=t.id)
    final = _wait_for_state(client.manager, t.id, TERMINAL_OR_OK, 30)
    assert final.state == TransferState.COMPLETED, final.error   # unharmed
    assert open(os.path.join(str(tmp_path / "server_files"), "hj.bin"), "rb").read() == data


# ═══════════════════════════════════════════════════════════════════════
# Restart + reconnect (persistence)
# ═══════════════════════════════════════════════════════════════════════

def _fresh_peer(sess, router, manager_dir, store):
    m = TransferManager(max_concurrent=3)
    sch = ConcurrentScheduler(sess, m, router, dest_dir=manager_dir, persistence=store)
    return m, sch


def test_server_restart_then_reconnect_resumes_upload(peers, tmp_path):
    sstore = PersistenceStore(str(tmp_path / "server.db"))
    client, server = peers(server_persistence=sstore)
    t, data, size = _start_upload(client, tmp_path, "sr.bin", chunks=60, seed=15)
    _wait_for_offset_at_least(client.manager, t.id, 12 * CHUNK_SIZE)
    _drop(client, server)
    _wait_lost(client, t.id); _wait_lost(server, t.id)
    row = _wait_for_persisted(sstore, t.id, lambda r: r is not None and r["state"] == "connection_lost")

    # The server process dies and restarts: everything in memory is gone.
    server.scheduler.stop_accept_loop()
    cs, ss = _new_link()
    sr = FrameRouter(ss)
    m2, sch2 = _fresh_peer(ss, sr, str(tmp_path / "server_files"), sstore)
    sr.start_reader()
    restored = restore_transfer(sstore.load_interrupted()[0], m2, connection_lost=True)
    assert restored.state == TransferState.CONNECTION_LOST
    sch2.start_accept_loop()

    cr = FrameRouter(cs)
    assert client.scheduler.attach_session(cs, cr) == [t.id]
    client.session, client.router = cs, cr

    final = _wait_for_state(client.manager, t.id, TERMINAL_OR_OK, 30)
    assert final.state == TransferState.COMPLETED, final.error
    assert open(os.path.join(str(tmp_path / "server_files"), "sr.bin"), "rb").read() == data
    assert _wait_for_state(m2, t.id, TERMINAL_OR_OK, 10).state == TransferState.COMPLETED
    sch2.stop_accept_loop(); sr.stop(); ss.close_session()


def test_client_restart_then_reconnect_resumes_upload(peers, tmp_path):
    cstore = PersistenceStore(str(tmp_path / "client.db"))
    client, server = peers(client_persistence=cstore)
    t, data, size = _start_upload(client, tmp_path, "cr.bin", chunks=60, seed=16)
    _wait_for_offset_at_least(client.manager, t.id, 12 * CHUNK_SIZE)
    _drop(client, server)
    _wait_lost(client, t.id); _wait_lost(server, t.id)
    _wait_for_persisted(cstore, t.id, lambda r: r is not None and r["state"] == "connection_lost")

    # The client process dies and restarts; the server just sees a new link.
    cs, ss = _new_link()
    cr = FrameRouter(cs)
    m2, sch2 = _fresh_peer(cs, cr, str(tmp_path / "client_files"), cstore)
    cr.start_reader()
    restored = restore_transfer(cstore.load_interrupted()[0], m2, connection_lost=True)
    assert restored.state == TransferState.CONNECTION_LOST
    sch2.reattach_upload(restored, str(tmp_path / "cr.bin"), chunk_size=CHUNK_SIZE)

    sr = FrameRouter(ss)
    server.scheduler.attach_session(ss, sr); server.session, server.router = ss, sr
    sch2.recover_transfer(restored.id)

    final = _wait_for_state(m2, restored.id, TERMINAL_OR_OK, 30)
    assert final.state == TransferState.COMPLETED, final.error
    assert open(os.path.join(str(tmp_path / "server_files"), "cr.bin"), "rb").read() == data
    cr.stop(); cs.close_session()


def test_recover_transfer_refuses_passive_transfers(peers, tmp_path):
    client, server = peers()
    t, _, _ = _start_upload(client, tmp_path, "ps.bin", chunks=60, seed=17)
    _wait_for_offset_at_least(client.manager, t.id, 10 * CHUNK_SIZE)
    _drop(client, server)
    _wait_lost(server, t.id)
    with pytest.raises(TransferStateError):
        server.scheduler.recover_transfer(t.id)            # the peer must re-CREATE
    assert server.scheduler.recover_all() == []
