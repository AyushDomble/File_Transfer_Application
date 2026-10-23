"""Shared persistent peers test support."""
from __future__ import annotations
import random
import socket
import struct
import threading
import time
from filetransfer.engine import FrameRouter, teardown_session
from filetransfer.manager import TransferManager
from filetransfer.persistence import PersistenceStore
from filetransfer.session_protocol import Session
from filetransfer.scheduler import ConcurrentScheduler
from filetransfer.transport import FrameType


def _write_random_file(path: str, size: int, seed: int) -> bytes:
    data = random.Random(seed).randbytes(size)
    with open(path, "wb") as f:
        f.write(data)
    return data

class Peer:
    def __init__(self, sock, name: str, dest_dir: str, max_concurrent: int = 3,
                 persistence: PersistenceStore = None):
        self.session = Session(sock, ("127.0.0.1", 0), name=name)
        self.session.mark_authenticated()  # This fixture tests post-pairing transfer behavior.
        self.router = FrameRouter(self.session)
        self.manager = TransferManager(max_concurrent=max_concurrent)
        self.scheduler = ConcurrentScheduler(
            self.session, self.manager, self.router, dest_dir=dest_dir,
            persistence=persistence,
        )

    def start(self) -> None:
        self.router.start_reader()

    def teardown(self) -> None:
        self.scheduler.stop_accept_loop()
        teardown_session(self.session, self.router)

def _make_peers(tmp_path, max_concurrent=3, client_persistence=None, server_persistence=None):
    a_sock, b_sock = socket.socketpair()
    client_dir = tmp_path / "client_files"
    server_dir = tmp_path / "server_files"
    client_dir.mkdir(exist_ok=True)
    server_dir.mkdir(exist_ok=True)

    client = Peer(a_sock, "client", str(client_dir), max_concurrent, client_persistence)
    server = Peer(b_sock, "server", str(server_dir), max_concurrent, server_persistence)

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
    while time.time() < deadline:
        t = manager.get(transfer_id)
        if t.offset >= min_offset or t.state.value in ("completed", "failed"):
            return t
        time.sleep(0.005)
    raise AssertionError(f"transfer {transfer_id[:8]} never reached offset {min_offset}")

def _wait_for_persisted(store: PersistenceStore, transfer_id: str,
                         predicate, timeout: float = 3.0):
    """Poll store.load(transfer_id) until `predicate(row)` is true.
    Needed because ConcurrentScheduler dispatches persistence writes
    to a background thread (see its _on_state_change) rather than
    writing them inline on the same thread a test's _wait_for_state
    just observed the in-memory state change on — the two aren't
    synchronized with each other, only eventually consistent."""
    deadline = time.time() + timeout
    last = "<never read>"
    while time.time() < deadline:
        row = store.load(transfer_id)
        last = row
        if predicate(row):
            return row
        time.sleep(0.01)
    raise AssertionError(f"persisted row for {transfer_id[:8]} never matched (last={last})")

def _corrupt_data_frames(session: Session, target_seq: int, times: int,
                          target_transfer_id: str = None):
    """Monkeypatch session.send so the payload of up to `times`
    TRANSFER_DATA frames with sequence == target_seq (and, when given,
    transfer_id == target_transfer_id — sequence numbers are per-
    transfer, not global, so multiple concurrent transfers can easily
    collide on the same seq at different points in their own streams)
    get one byte flipped in the chunk portion (after the length-
    prefixed JSON header — see engine._encode_data_payload), regardless
    of how many times that chunk gets (re)sent. Returns a dict tracking
    how many times corruption was actually applied, so the test can
    confirm the injection really fired rather than passing trivially.
    `target_transfer_id` may be set on the returned dict after this
    call if the transfer_id isn't known yet at call time (e.g. before
    submit_upload() returns it)."""
    original = session.send
    state = {"remaining": times, "applied": 0, "transfer_id": target_transfer_id}
    prefix_struct = struct.Struct(">I")

    def wrapper(frame):
        if (state["remaining"] > 0
                and frame.frame_type == FrameType.TRANSFER_DATA
                and frame.sequence == target_seq
                and (state["transfer_id"] is None or frame.transfer_id == state["transfer_id"])):
            state["remaining"] -= 1
            state["applied"] += 1
            payload = bytearray(frame.payload)
            (prefix_len,) = prefix_struct.unpack_from(payload, 0)
            chunk_start = 4 + prefix_len
            if chunk_start < len(payload):
                payload[chunk_start] ^= 0xFF
                frame.payload = bytes(payload)
        return original(frame)

    session.send = wrapper
    return state

CHUNK_SIZE = 4096
