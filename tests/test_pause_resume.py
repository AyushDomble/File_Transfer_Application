"""End-to-end pause and resume tests using running transfers over real sockets.
Coverage includes pending pause/cancel intent during engine construction,
safe-point acknowledgement, repeated controls and resume-confirmation races."""

from __future__ import annotations

import hashlib
import os
import random
import socket
import threading
import time

import pytest

from filetransfer.engine import FrameRouter, teardown_session
from filetransfer.manager import TransferManager
from filetransfer.session_protocol import Session, SessionError
from filetransfer.scheduler import ConcurrentScheduler
from filetransfer.transfer import TERMINAL_STATES, TransferState, TransferStateError
from filetransfer.transport import FrameType


def _write_random_file(path: str, size: int, seed: int) -> bytes:
    data = random.Random(seed).randbytes(size)
    with open(path, "wb") as f:
        f.write(data)
    return data


class Peer:
    def __init__(self, sock, name: str, dest_dir: str, max_concurrent: int = 3):
        self.session = Session(sock, ("127.0.0.1", 0), name=name)
        self.session.mark_authenticated()  # This fixture tests post-pairing transfer behavior.
        self.router = FrameRouter(self.session)
        self.manager = TransferManager(max_concurrent=max_concurrent)
        self.scheduler = ConcurrentScheduler(
            self.session, self.manager, self.router, dest_dir=dest_dir
        )

    def start(self) -> None:
        self.router.start_reader()

    def teardown(self) -> None:
        self.scheduler.stop_accept_loop()
        teardown_session(self.session, self.router)


def _make_peers(tmp_path, max_concurrent=3):
    a_sock, b_sock = socket.socketpair()
    client_dir = tmp_path / "client_files"
    server_dir = tmp_path / "server_files"
    client_dir.mkdir(exist_ok=True)
    server_dir.mkdir(exist_ok=True)

    client = Peer(a_sock, "client", str(client_dir), max_concurrent=max_concurrent)
    server = Peer(b_sock, "server", str(server_dir), max_concurrent=max_concurrent)

    results, errors = {}, {}

    def do_client():
        try:
            results["client"] = client.session.negotiate_as_client()
        except Exception as e:
            errors["client"] = e

    def do_server():
        try:
            results["server"] = server.session.negotiate_as_server()
        except Exception as e:
            errors["server"] = e

    tc = threading.Thread(target=do_client)
    ts = threading.Thread(target=do_server)
    tc.start(); ts.start()
    tc.join(timeout=3); ts.join(timeout=3)
    assert not errors, errors

    client.start()
    server.start()
    server.scheduler.start_accept_loop()
    return client, server


@pytest.fixture()
def peers(tmp_path):
    client, server = _make_peers(tmp_path)
    yield client, server
    client.teardown()
    server.teardown()


def _wait_for_state(manager: TransferManager, transfer_id: str,
                     states, timeout: float = 15.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        t = manager.get(transfer_id)
        last = t.state
        if t.state in states:
            return t
        time.sleep(0.01)
    raise AssertionError(
        f"transfer {transfer_id[:8]} never reached {states} (last={last})"
    )


def _wait_for_offset_at_least(manager: TransferManager, transfer_id: str,
                               min_offset: int, timeout: float = 15.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        t = manager.get(transfer_id)
        last = (t.state, t.offset)
        if t.offset >= min_offset or t.state in TERMINAL_STATES:
            return t
        time.sleep(0.005)
    raise AssertionError(
        f"transfer {transfer_id[:8]} never reached offset {min_offset} (last={last})"
    )


def _server_transfer_for(server, filename: str):
    matches = [t for t in server.manager.all_transfers() if t.filename == filename]
    assert len(matches) == 1, f"expected exactly one server-side transfer for {filename}"
    return matches[0]


def _slow_progress_cb(delay: float = 0.004):
    """A same-process loopback transfer of a few dozen small chunks can
    complete in single-digit milliseconds — faster than a test's own
    pause() call, issued from a separate thread, can reliably land.
    This throttles the sender between chunks (which also paces the
    receiver, since it's blocked waiting on the next DATA frame
    either way) so pause() has a real window to land mid-transfer
    instead of racing the transfer's own natural completion."""
    def _cb(offset: int, total: int) -> None:
        time.sleep(delay)
    return _cb


def _delay_next_resuming_ack(session, delay: float):
    """Monkeypatch session.send_json so the next 'resuming' ACK it
    sends is delayed by `delay` seconds before actually going out —
    sent from a background thread so the caller (the engine that
    would otherwise send it inline) isn't itself blocked, same as
    real network latency wouldn't block it. Intercepts exactly one
    matching call, then restores normal behavior."""
    original = session.send_json
    state = {"armed": True}

    def wrapper(frame_type, data, transfer_id=0, sequence=0, flags=0):
        if (state["armed"] and frame_type == FrameType.TRANSFER_ACK
                and data.get("resuming")):
            state["armed"] = False
            session.send_json = original  # restore for all subsequent calls

            def _delayed():
                time.sleep(delay)
                if session.is_lost:
                    return
                try:
                    original(frame_type, data, transfer_id=transfer_id,
                             sequence=sequence, flags=flags)
                except (OSError, SessionError):
                    if not session.is_lost:
                        raise

            threading.Thread(target=_delayed, daemon=True).start()
            return
        return original(frame_type, data, transfer_id=transfer_id,
                         sequence=sequence, flags=flags)

    session.send_json = wrapper


# ═══════════════════════════════════════════════════════════════════════
# Basic pause -> resume -> complete, sender-initiated
# ═══════════════════════════════════════════════════════════════════════

CHUNK_SIZE = 4096

def test_sender_initiated_pause_then_resume_completes_correctly(peers, tmp_path):
    client, server = peers
    size = 40 * CHUNK_SIZE + 111
    src = tmp_path / "pause_upload.bin"
    data = _write_random_file(str(src), size, seed=10)

    transfer = client.scheduler.submit_upload(
        str(src), chunk_size=CHUNK_SIZE, progress_cb=_slow_progress_cb()
    )

    # Let a handful of chunks go through, then pause mid-flight.
    _wait_for_offset_at_least(client.manager, transfer.id, 5 * CHUNK_SIZE)
    client.scheduler.pause(transfer.id)

    paused = _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED})
    assert 0 < paused.offset < size, (
        "pause should land mid-transfer, not at the very start or the very end"
    )

    # The receiver's own view of the same logical transfer must also
    # have confirmed PAUSED, and at a matching (or later, if a chunk
    # was already in flight) offset.
    server_transfer = _server_transfer_for(server, "pause_upload.bin")
    server_paused = _wait_for_state(server.manager, server_transfer.id, {TransferState.PAUSED})
    assert server_paused.offset >= paused.offset

    # Partial .part file should already contain exactly what was acked.
    part_path = server.manager.get(transfer.id).part_path
    assert os.path.isfile(part_path)
    assert os.path.getsize(part_path) == server_paused.offset

    client.scheduler.resume(transfer.id)
    final = _wait_for_state(client.manager, transfer.id,
                             {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
    assert final.state == TransferState.COMPLETED, final.error

    dest = os.path.join(server.scheduler._dest_dir, "pause_upload.bin")
    with open(dest, "rb") as f:
        received = f.read()
    assert received == data


def test_receiver_initiated_pause_then_resume(peers, tmp_path):
    """Exercises the other half of pause: the *receiver* requesting the
    pause (engine.py lines ~1290-1320), not the sender."""
    client, server = peers
    size = 30 * CHUNK_SIZE + 50
    src = tmp_path / "recv_pause.bin"
    data = _write_random_file(str(src), size, seed=11)

    transfer = client.scheduler.submit_upload(
        str(src), chunk_size=CHUNK_SIZE, progress_cb=_slow_progress_cb()
    )
    server_transfer = None
    # The server-side transfer only appears once the CREATE handshake
    # lands — poll for it briefly.
    deadline = time.time() + 5
    while time.time() < deadline:
        matches = [t for t in server.manager.all_transfers() if t.filename == "recv_pause.bin"]
        if matches:
            server_transfer = matches[0]
            break
        time.sleep(0.005)
    assert server_transfer is not None

    _wait_for_offset_at_least(server.manager, server_transfer.id, 4 * CHUNK_SIZE)
    server.scheduler.pause(server_transfer.id)

    server_paused = _wait_for_state(server.manager, server_transfer.id, {TransferState.PAUSED})
    client_paused = _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED})
    assert 0 < client_paused.offset

    server.scheduler.resume(server_transfer.id)
    final_client = _wait_for_state(client.manager, transfer.id,
                                    {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
    assert final_client.state == TransferState.COMPLETED, final_client.error

    dest = os.path.join(server.scheduler._dest_dir, "recv_pause.bin")
    with open(dest, "rb") as f:
        received = f.read()
    assert received == data


# ═══════════════════════════════════════════════════════════════════════
# Rapid pause/resume cycling (race testing's own race list: "resume
# immediately after pause")
# ═══════════════════════════════════════════════════════════════════════

def test_multiple_pause_resume_cycles(peers, tmp_path):
    client, server = peers
    size = 60 * CHUNK_SIZE + 200
    src = tmp_path / "cycles.bin"
    data = _write_random_file(str(src), size, seed=12)

    transfer = client.scheduler.submit_upload(
        str(src), chunk_size=CHUNK_SIZE, progress_cb=_slow_progress_cb()
    )

    server_transfer = None
    deadline = time.time() + 5
    while time.time() < deadline:
        matches = [t for t in server.manager.all_transfers() if t.filename == "cycles.bin"]
        if matches:
            server_transfer = matches[0]
            break
        time.sleep(0.005)
    assert server_transfer is not None

    for cycle in range(3):
        target = (cycle + 1) * 10 * CHUNK_SIZE
        t = _wait_for_offset_at_least(client.manager, transfer.id, target, timeout=15)
        if t.state in {TransferState.COMPLETED, TransferState.FAILED}:
            break
        client.scheduler.pause(transfer.id)
        # With the fix, ACTIVE -> ... -> PAUSED never involves waiting
        # on the network beyond one local round trip: PAUSED -> ACTIVE
        # (see below) now only ever happens on a confirmed resuming
        # ACK, so a fresh pause() has nothing to wait out — it's
        # honored at the very next safe point. 5s is generous slack
        # for a loopback socket; it should never get close to it.
        _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED}, timeout=5)

        client.scheduler.resume(transfer.id)
        # ACTIVE now genuinely means "the resume handshake is fully
        # settled and we have the receiver-confirmed offset" — it's
        # only reached from SendEngine._wait_for_resume's resume-confirmation stage on a
        # matching confirmation, never just from sending/seeing RESUME.
        # Waiting for it here is therefore a real synchronization
        # point, not a hopeful guess.
        _wait_for_state(client.manager, transfer.id, {TransferState.ACTIVE, TransferState.COMPLETED}, timeout=5)
        _wait_for_state(server.manager, server_transfer.id, {TransferState.ACTIVE, TransferState.COMPLETED}, timeout=5)

    final = _wait_for_state(client.manager, transfer.id,
                             {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
    assert final.state == TransferState.COMPLETED, final.error

    dest = os.path.join(server.scheduler._dest_dir, "cycles.bin")
    with open(dest, "rb") as f:
        received = f.read()
    assert received == data


def test_resume_called_immediately_after_pause_no_gap(peers, tmp_path):
    """No wait between pause() and resume() at all — pause() may not
    have taken effect on the engine thread yet when resume() fires."""
    client, server = peers
    size = 25 * CHUNK_SIZE
    src = tmp_path / "immediate.bin"
    data = _write_random_file(str(src), size, seed=13)

    transfer = client.scheduler.submit_upload(
        str(src), chunk_size=CHUNK_SIZE, progress_cb=_slow_progress_cb()
    )
    _wait_for_offset_at_least(client.manager, transfer.id, 3 * CHUNK_SIZE)

    client.scheduler.pause(transfer.id)
    client.scheduler.resume(transfer.id)  # no wait at all

    final = _wait_for_state(client.manager, transfer.id,
                             {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
    assert final.state == TransferState.COMPLETED, final.error

    dest = os.path.join(server.scheduler._dest_dir, "immediate.bin")
    with open(dest, "rb") as f:
        received = f.read()
    assert received == data


# ═══════════════════════════════════════════════════════════════════════
# The pending_pause/pending_cancel race: control call arrives before
# the engine has finished its handshake
# ═══════════════════════════════════════════════════════════════════════

def test_pause_called_before_engine_finishes_building(peers, tmp_path):
    """pause() the instant submit_upload() returns — the transfer may
    already be ACTIVE (slot free) but its engine's TRANSFER_CREATE
    handshake hasn't completed yet. Without pending_pause this would
    set PAUSING and then never resolve it (see scheduler._Job)."""
    client, server = peers
    size = 10 * CHUNK_SIZE
    src = tmp_path / "race.bin"
    data = _write_random_file(str(src), size, seed=14)

    transfer = client.scheduler.submit_upload(str(src), chunk_size=CHUNK_SIZE)
    client.scheduler.pause(transfer.id)  # no wait — race the handshake

    paused = _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED}, timeout=10)
    assert paused.state == TransferState.PAUSED

    client.scheduler.resume(transfer.id)
    final = _wait_for_state(client.manager, transfer.id,
                             {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
    assert final.state == TransferState.COMPLETED, final.error

    dest = os.path.join(server.scheduler._dest_dir, "race.bin")
    with open(dest, "rb") as f:
        received = f.read()
    assert received == data


def test_cancel_called_before_engine_finishes_building(peers, tmp_path):
    """Same race as above, but for cancel(): without pending_cancel the
    in-flight build() would hand back a live engine that runs the full
    transfer regardless of the transfer already being CANCELLED."""
    client, server = peers
    size = 10 * CHUNK_SIZE
    src = tmp_path / "race_cancel.bin"
    _write_random_file(str(src), size, seed=15)

    transfer = client.scheduler.submit_upload(str(src), chunk_size=CHUNK_SIZE)
    client.scheduler.cancel(transfer.id)  # no wait — race the handshake

    final = _wait_for_state(client.manager, transfer.id, {TransferState.CANCELLED}, timeout=10)
    assert final.state == TransferState.CANCELLED

    # Give any wrongly-still-running engine a moment, then confirm no
    # completed file ever landed on the server side.
    time.sleep(0.5)
    dest = os.path.join(server.scheduler._dest_dir, "race_cancel.bin")
    assert not os.path.isfile(dest)


# ═══════════════════════════════════════════════════════════════════════
# Cancel while paused (never resumed)
# ═══════════════════════════════════════════════════════════════════════

def test_cancel_while_paused(peers, tmp_path):
    client, server = peers
    size = 20 * CHUNK_SIZE
    src = tmp_path / "cancel_paused.bin"
    _write_random_file(str(src), size, seed=16)

    transfer = client.scheduler.submit_upload(
        str(src), chunk_size=CHUNK_SIZE, progress_cb=_slow_progress_cb()
    )
    _wait_for_offset_at_least(client.manager, transfer.id, 3 * CHUNK_SIZE)
    server_transfer = _server_transfer_for(server, "cancel_paused.bin")
    client.scheduler.pause(transfer.id)
    _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED})

    client.scheduler.cancel(transfer.id)
    final = _wait_for_state(client.manager, transfer.id, {TransferState.CANCELLED})
    assert final.state == TransferState.CANCELLED

    # The client reaching CANCELLED only means ITS side is done — the
    # actual .part file deletion happens on the server's own thread,
    # which can lag behind by a few milliseconds. Wait for the
    # server's own transfer to confirm CANCELLED too (a real
    # synchronization point) before checking its cleanup.
    _wait_for_state(server.manager, server_transfer.id, {TransferState.CANCELLED})
    assert server_transfer.io_finished.wait(2), 'receiver did not finish cancellation cleanup'

    # .part file must be cleaned up on the receiving side.
    part_path = server_transfer.part_path
    assert not os.path.isfile(part_path)

    # Session must still be usable afterward.
    assert not client.session.closed
    assert not server.session.closed


# ═══════════════════════════════════════════════════════════════════════
# Pause isolation under concurrency
# ═══════════════════════════════════════════════════════════════════════

def test_pause_one_transfer_does_not_affect_concurrent_others(tmp_path):
    client, server = _make_peers(tmp_path, max_concurrent=3)
    try:
        files = []
        for i in range(3):
            p = tmp_path / f"iso_{i}.bin"
            data = _write_random_file(str(p), 15 * CHUNK_SIZE + i, seed=100 + i)
            files.append((str(p), data))

        transfers = [
            client.scheduler.submit_upload(path, chunk_size=CHUNK_SIZE,
                                            progress_cb=_slow_progress_cb())
            for path, _ in files
        ]

        # Pause only the middle one, shortly after it starts.
        _wait_for_offset_at_least(client.manager, transfers[1].id, CHUNK_SIZE)
        client.scheduler.pause(transfers[1].id)
        _wait_for_state(client.manager, transfers[1].id, {TransferState.PAUSED})

        # The other two must complete without ever being touched.
        final0 = _wait_for_state(client.manager, transfers[0].id,
                                  {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
        final2 = _wait_for_state(client.manager, transfers[2].id,
                                  {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
        assert final0.state == TransferState.COMPLETED, final0.error
        assert final2.state == TransferState.COMPLETED, final2.error

        # Paused one is still waiting — not accidentally cancelled or
        # completed by whatever freed a slot for the others.
        assert client.manager.get(transfers[1].id).state == TransferState.PAUSED

        client.scheduler.resume(transfers[1].id)
        final1 = _wait_for_state(client.manager, transfers[1].id,
                                  {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
        assert final1.state == TransferState.COMPLETED, final1.error

        for (path, data) in files:
            dest = os.path.join(server.scheduler._dest_dir, os.path.basename(path))
            with open(dest, "rb") as f:
                assert f.read() == data
    finally:
        client.teardown()
        server.teardown()


# ═══════════════════════════════════════════════════════════════════════
# Focused regressions for the control-plane race: a new pause must
# preempt an in-progress resume handshake promptly, and a stale
# confirmation must never reactivate a transfer that's moved on.
# ═══════════════════════════════════════════════════════════════════════

def test_pause_preempts_delayed_resume_confirmation(peers, tmp_path):
    """The receiver's resuming ACK is delayed well past when a second
    pause() arrives. Before the fix, SendEngine._wait_for_resume's
    resume-confirmation stage didn't check for a new pause at all — it would sit waiting
    for that ACK for up to ACK_TIMEOUT_SECONDS (30s) before ever
    honoring the new pause. Assert it's honored almost immediately
    instead, well under that bound."""
    client, server = peers
    size = 20 * CHUNK_SIZE
    src = tmp_path / "delayed_ack.bin"
    _write_random_file(str(src), size, seed=20)

    transfer = client.scheduler.submit_upload(
        str(src), chunk_size=CHUNK_SIZE, progress_cb=_slow_progress_cb()
    )
    _wait_for_offset_at_least(client.manager, transfer.id, 3 * CHUNK_SIZE)
    client.scheduler.pause(transfer.id)
    _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED})

    # Delay the receiver's reply to the upcoming resume by well beyond
    # what a "promptly preempted" pause should ever wait out.
    _delay_next_resuming_ack(server.session, delay=8.0)

    client.scheduler.resume(transfer.id)
    time.sleep(0.05)  # let resume() actually reach the RESUME-sent state
    client.scheduler.pause(transfer.id)

    start = time.time()
    paused = _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED}, timeout=5)
    elapsed = time.time() - start
    assert paused.state == TransferState.PAUSED
    assert elapsed < 5, f"pause took {elapsed:.2f}s — the delayed ACK blocked it"


def test_stale_resume_ack_does_not_reactivate(peers, tmp_path):
    """A resume attempt's confirmation arrives late — after a second
    pause has already abandoned it. That stale ACK must be discarded,
    never mistaken for confirmation of the (different) resume attempt
    that matters now, and must never put the transfer back in ACTIVE
    behind the caller's back."""
    client, server = peers
    size = 20 * CHUNK_SIZE
    src = tmp_path / "stale_ack.bin"
    _write_random_file(str(src), size, seed=21)

    transfer = client.scheduler.submit_upload(
        str(src), chunk_size=CHUNK_SIZE, progress_cb=_slow_progress_cb()
    )
    _wait_for_offset_at_least(client.manager, transfer.id, 3 * CHUNK_SIZE)
    client.scheduler.pause(transfer.id)
    _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED})

    # This ACK (for the resume attempt about to be started) will be
    # delivered ~2s from now — after the immediately-following pause()
    # has already abandoned that attempt.
    _delay_next_resuming_ack(server.session, delay=2.0)
    client.scheduler.resume(transfer.id)
    time.sleep(0.05)
    client.scheduler.pause(transfer.id)
    _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED}, timeout=5)

    # Now let the stale, delayed ACK actually arrive and be processed.
    time.sleep(2.5)
    still = client.manager.get(transfer.id)
    assert still.state == TransferState.PAUSED, (
        f"a stale resume confirmation reactivated the transfer: {still.state}"
    )

    # The transfer must still be fully resumable and completable via a
    # fresh, un-stale attempt afterward — the abandoned attempt didn't
    # leave anything broken.
    client.scheduler.resume(transfer.id)
    final = _wait_for_state(client.manager, transfer.id,
                             {TransferState.COMPLETED, TransferState.FAILED}, timeout=15)
    assert final.state == TransferState.COMPLETED, final.error


def test_receiver_initiated_resume_confirmation_also_preemptible(peers, tmp_path):
    """Same preemption guarantee, but for a receiver-initiated resume:
    the server generates the resume_id and the client (sender) is the
    one waiting on resume-confirmation stage for the matching confirmation. A fresh
    pause — from either side — must still cut that wait short."""
    client, server = peers
    size = 20 * CHUNK_SIZE
    src = tmp_path / "recv_delayed_ack.bin"
    _write_random_file(str(src), size, seed=22)

    transfer = client.scheduler.submit_upload(
        str(src), chunk_size=CHUNK_SIZE, progress_cb=_slow_progress_cb()
    )
    server_transfer = None
    deadline = time.time() + 5
    while time.time() < deadline:
        matches = [t for t in server.manager.all_transfers()
                   if t.filename == "recv_delayed_ack.bin"]
        if matches:
            server_transfer = matches[0]
            break
        time.sleep(0.005)
    assert server_transfer is not None

    _wait_for_offset_at_least(server.manager, server_transfer.id, 3 * CHUNK_SIZE)
    server.scheduler.pause(server_transfer.id)
    _wait_for_state(server.manager, server_transfer.id, {TransferState.PAUSED})
    _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED})

    # This time it's the RECEIVER replying to its own resume trigger —
    # RecvEngine sends this ACK right after its local resume_event
    # fires, so delaying it here simulates the confirmation being slow
    # to reach the sender specifically.
    _delay_next_resuming_ack(server.session, delay=8.0)
    server.scheduler.resume(server_transfer.id)
    time.sleep(0.05)
    # The client (sender) re-pauses — it's the one sitting in resume-confirmation stage
    # waiting for this exact confirmation.
    client.scheduler.pause(transfer.id)

    start = time.time()
    paused = _wait_for_state(client.manager, transfer.id, {TransferState.PAUSED}, timeout=5)
    elapsed = time.time() - start
    assert paused.state == TransferState.PAUSED
    assert elapsed < 5, f"pause took {elapsed:.2f}s — the delayed ACK blocked it"


def test_pause_coincident_with_final_chunk_still_completes(peers, tmp_path):
    """The completion-race test that motivated PAUSING/PAUSED -> COMPLETED
    becoming legal transitions: pause() lands while the last chunk is
    already in flight, so the engine finishes the whole transfer before
    it ever gets back around to checking pause_flag. That must still be
    reported as a successful completion, never FAILED."""
    client, server = peers
    size = 3 * CHUNK_SIZE
    src = tmp_path / "final_chunk_race.bin"
    data = _write_random_file(str(src), size, seed=23)

    transfer = client.scheduler.submit_upload(str(src), chunk_size=CHUNK_SIZE)
    # Wait until only the last chunk remains, then pause — the send of
    # that final chunk (and its ACK) is already effectively unstoppable
    # by the time pause_flag gets checked again.
    _wait_for_offset_at_least(client.manager, transfer.id, 2 * CHUNK_SIZE)
    try:
        client.scheduler.pause(transfer.id)
    except TransferStateError:
        # The transfer won the race outright before pause() even ran
        # (COMPLETED -> PAUSING is rightly illegal) — that's the same
        # race, just resolved a step earlier than usual; nothing left
        # to do but confirm it below.
        pass

    final = _wait_for_state(client.manager, transfer.id,
                             {TransferState.COMPLETED, TransferState.PAUSED,
                              TransferState.FAILED}, timeout=10)
    if final.state == TransferState.PAUSED:
        # Pause won the race this time — resume and let it finish.
        client.scheduler.resume(transfer.id)
        final = _wait_for_state(client.manager, transfer.id,
                                 {TransferState.COMPLETED, TransferState.FAILED}, timeout=10)

    assert final.state == TransferState.COMPLETED, final.error
    dest = os.path.join(server.scheduler._dest_dir, "final_chunk_race.bin")
    with open(dest, "rb") as f:
        assert f.read() == data

