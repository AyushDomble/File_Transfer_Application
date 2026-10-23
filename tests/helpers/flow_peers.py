"""Shared flow peers test support."""
from __future__ import annotations
import random
import socket
import threading
import time
from filetransfer.engine import FrameRouter, teardown_session
from filetransfer.flowcontrol import GlobalWindow
from filetransfer.manager import TransferManager
from filetransfer.session_protocol import Session
from filetransfer.scheduler import ConcurrentScheduler


CHUNK_SIZE = 4096

def _write_random_file(path: str, size: int, seed: int) -> bytes:
    data = random.Random(seed).randbytes(size)
    with open(path, "wb") as f:
        f.write(data)
    return data

class Peer:
    def __init__(self, sock, name: str, dest_dir: str, max_concurrent: int = 5,
                 send_window: GlobalWindow = None):
        self.session = Session(sock, ("127.0.0.1", 0), name=name)
        self.session.mark_authenticated()  # This fixture tests post-pairing transfer behavior.
        self.router = FrameRouter(self.session)
        self.manager = TransferManager(max_concurrent=max_concurrent)
        self.scheduler = ConcurrentScheduler(
            self.session, self.manager, self.router, dest_dir=dest_dir,
            send_window=send_window,
        )

    def start(self) -> None:
        self.router.start_reader()

    def teardown(self) -> None:
        self.scheduler.stop_accept_loop()
        teardown_session(self.session, self.router)

def _make_peers(tmp_path, max_concurrent=5, client_window=None):
    a_sock, b_sock = socket.socketpair()
    client_dir = tmp_path / "client_files"
    server_dir = tmp_path / "server_files"
    client_dir.mkdir(exist_ok=True)
    server_dir.mkdir(exist_ok=True)

    client = Peer(a_sock, "client", str(client_dir), max_concurrent, client_window)
    server = Peer(b_sock, "server", str(server_dir), max_concurrent)

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
                     states, timeout: float = 20.0):
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
