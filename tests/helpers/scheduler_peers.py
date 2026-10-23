"""Shared scheduler peers test support."""
from __future__ import annotations
import os
import socket
import threading
import time
import pytest
from filetransfer.engine import FrameRouter, teardown_session
from filetransfer.manager import TransferManager
from filetransfer.session_protocol import Session
from filetransfer.scheduler import ConcurrentScheduler


def _write_random_file(path: str, size: int) -> None:
    with open(path, "wb") as f:
        f.write(os.urandom(size))

class Peer:
    """One side of a connected session: socket + Session + FrameRouter
    (reader thread actually running) + TransferManager + ConcurrentScheduler."""

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

@pytest.fixture()
def peers(tmp_path):
    """A connected (client, server) pair, each with its own manager/
    scheduler/dest_dir, negotiated and with reader threads running."""
    a_sock, b_sock = socket.socketpair()
    client_dir = tmp_path / "client_files"
    server_dir = tmp_path / "server_files"
    client_dir.mkdir()
    server_dir.mkdir()

    client = Peer(a_sock, "client", str(client_dir))
    server = Peer(b_sock, "server", str(server_dir))

    # Negotiate HELLO/HELLO_ACK before either reader thread starts —
    # negotiate_as_client/_server do their own synchronous recv().
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

    yield client, server

    client.teardown()
    server.teardown()
    for d in (client_dir, server_dir):
        for f in os.listdir(d):
            pass  # tmp_path cleans itself up

def _wait_for_state(manager: TransferManager, transfer_id: str,
                     states, timeout: float = 10.0):
    """Poll until the transfer reaches one of `states` (or raise)."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        t = manager.get(transfer_id)
        last = t.state
        if t.state in states:
            return t
        time.sleep(0.02)
    raise AssertionError(
        f"transfer {transfer_id[:8]} never reached {states} (last={last})"
    )
