"""Shared app peers test support."""
import hashlib
import socket
import threading
import time
import pytest
from filetransfer import config
from filetransfer.host import TransferHost
from filetransfer.hostlink import HostLink


def wait(predicate, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    assert predicate(), 'condition did not become true'

class AppPair:
    def __init__(self, root, monkeypatch):
        self.root, self.monkeypatch = root, monkeypatch
        self.server_dir, self.client_dir = root / 'server', root / 'client'
        self.server_dir.mkdir(); self.client_dir.mkdir()
        self.new_host()
        self.new_client()

    def new_host(self):
        self.host = TransferHost(dest_dir=str(self.server_dir))
        self.host._regenerate_pin('test')
        self.host.add_pending_listener(lambda info: self.host.accept_pending(info['id']))

    def new_client(self):
        # The fixture substitutes _open_socket; keep the production secure
        # endpoint namespace so API restoration/reuse tests cover current policy.
        self.link = HostLink('test-host', port=5000, use_tls=True, dest_dir=str(self.client_dir))

    def connect(self, pin=None):
        a, b = socket.socketpair()
        self.monkeypatch.setattr(self.link, '_open_socket', lambda: a)
        errors = []
        def serve():
            try:
                self.host._handle_connection(b, ('test-client', 1))
            except Exception as exc:
                errors.append(exc)
        worker = threading.Thread(target=serve, daemon=True)
        worker.start()
        try:
            recovered = self.link.connect_with_pin(pin if pin is not None else self.host.current_pin)
        finally:
            worker.join(8)
        assert not worker.is_alive() and not errors, errors
        return recovered

    @property
    def peer(self):
        key = hashlib.sha256(self.link._recovery_key.encode()).hexdigest()
        return self.host.get_peer(key)

    def drop(self):
        self.link.router.declare_lost('deliberate test drop')
        wait(lambda: self.peer.session.is_lost)
        for transfer in self.link.manager.all_transfers():
            assert transfer.io_finished.wait(5)
        for transfer in self.peer.manager.all_transfers():
            assert transfer.io_finished.wait(5)

    def close(self):
        self.link.close()
        self.host.stop()

@pytest.fixture
def app_pair(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'STATE_DIR', None)
    app = AppPair(tmp_path, monkeypatch)
    yield app
    app.close()
