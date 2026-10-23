"""Adversarial boundaries plus real pinned TLS integration; no security bypasses."""
import asyncio
import json
import socket
import ssl
import threading
import time
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient

from filetransfer import config, discovery
from filetransfer.host import TransferHost
from filetransfer.hostlink import HostLink, ConnectionRejected
from filetransfer.session_protocol import Session, NegotiatedSession, ALL_FEATURES
from filetransfer.rate_limit import AttemptLimiter
from filetransfer.transport import Frame, FrameError, FrameType, send_frame, _STRUCT_HEADER, MAGIC, VERSION
from filetransfer.tls_identity import server_context
from webapp.local_security import LocalSecurity, BOOTSTRAP_TOKEN


def wait(predicate, timeout=10):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(.02)
    assert predicate()


@pytest.fixture
def secure_host(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'HOST', '127.0.0.1')
    monkeypatch.setattr(config, 'PORT', 0)
    monkeypatch.setattr(config, 'STATE_DIR', None)
    monkeypatch.setattr(config, 'TLS_ENABLED', True)
    monkeypatch.setattr(config, 'TLS_CERT_FILE', None)
    monkeypatch.setattr(config, 'TLS_KEY_FILE', None)
    host = TransferHost(str(tmp_path / 'server'))
    monkeypatch.setattr(host._discovery, 'start', lambda: None)
    host.add_pending_listener(lambda item: host.accept_pending(item['id']))
    host.start()
    yield host, host._sock.getsockname()[1]
    host.stop()


def test_tls_transfer_chat_delete_permission_and_saved_trust(secure_host, tmp_path):
    host, port = secure_host
    link = HostLink('127.0.0.1', port, dest_dir=str(tmp_path / 'client'), fingerprint=host.certificate_fingerprint)
    try:
        link.connect_with_pin(host.current_pin)
        source = tmp_path / 'sample.txt'; source.write_bytes(b'secure transfer' * 12000)
        transfer = link.scheduler.submit_upload(str(source))
        wait(lambda: transfer.is_terminal)
        assert transfer.state.value == 'completed'
        assert (Path(host.dest_dir) / source.name).read_bytes() == source.read_bytes()
        assert not link.delete(source.name)
        assert (Path(host.dest_dir) / source.name).exists()
        host.allow_remote_delete = True
        assert link.delete(source.name)
        link.close()
        link.fingerprint = None
        link.connect_with_pin(host.current_pin)
        assert link.connected
    finally:
        link.close()


def test_missing_or_wrong_fingerprint_never_sends_pin(secure_host, tmp_path):
    host, port = secure_host
    original = host.current_pin
    link = HostLink('127.0.0.1', port, dest_dir=str(tmp_path / 'client'))
    with pytest.raises(ConnectionRejected, match='First pairing'):
        link.connect_with_pin(original)
    link.fingerprint = '0' * 64
    with pytest.raises(ssl.SSLError, match='fingerprint'):
        link.connect_with_pin(original)
    assert host.current_pin == original and not host.list_pending() and not host.list_peers()


def test_tls_noise_does_not_kill_acceptor(secure_host, tmp_path):
    host, port = secure_host
    with socket.create_connection(('127.0.0.1', port)) as bad:
        bad.sendall(b'not a TLS client\r\n')
    link = HostLink('127.0.0.1', port, dest_dir=str(tmp_path / 'client'), fingerprint=host.certificate_fingerprint)
    try:
        link.connect_with_pin(host.current_pin)
        assert host._accept_thread.is_alive() and link.connected
    finally:
        link.close()


def test_pin_throttling_preserves_pin_and_does_not_log_it(secure_host, tmp_path, caplog):
    host, port = secure_host
    pin = host.current_pin
    link = HostLink('127.0.0.1', port, dest_dir=str(tmp_path / 'client'), fingerprint=host.certificate_fingerprint)
    for _ in range(5):
        with pytest.raises(ConnectionRejected, match='invalid PIN'):
            link.connect_with_pin('not-the-pin')
    with pytest.raises(ConnectionRejected, match='Too many'):
        link.connect_with_pin(pin)
    assert host.current_pin == pin and not host.list_pending()
    assert pin not in caplog.text


def test_saved_trust_cannot_be_silently_overridden(secure_host, tmp_path):
    host, port = secure_host
    link = HostLink('127.0.0.1', port, dest_dir=str(tmp_path / 'client'), fingerprint=host.certificate_fingerprint)
    link.connect_with_pin(host.current_pin); link.close()
    link.fingerprint = '0' * 64
    with pytest.raises(ConnectionRejected, match='Saved host fingerprint differs'):
        link.connect_with_pin(host.current_pin)


def test_pending_connections_bounded_and_shutdown_closes_them(secure_host, monkeypatch):
    host, port = secure_host
    monkeypatch.setattr(config, 'MAX_CLIENTS', 2)
    clients = [socket.create_connection(('127.0.0.1', port)) for _ in range(5)]
    try:
        time.sleep(.15)
        assert len(host._connections) == 2
        host.stop()
        wait(lambda: not host._connections)
    finally:
        for client in clients: client.close()


def test_identity_persists_without_rotation(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'STATE_DIR', None)
    assert server_context(tmp_path)[1] == server_context(tmp_path)[1]


@pytest.mark.parametrize('size,kind', [(64 * 1024 * 1024, FrameType.TRANSFER_DATA), (1, FrameType.TRANSFER_DATA), (65537, FrameType.AUTH)])
def test_pre_auth_rejects_header_without_reading_payload(size, kind):
    a, b = socket.socketpair(); b.settimeout(.5)
    session = Session(b, ('test', 1))
    session._negotiation = NegotiatedSession(VERSION, ALL_FEATURES)
    try:
        a.sendall(_STRUCT_HEADER.pack(MAGIC, VERSION, int(kind), 0, 0, b'0' * 32, 0, size))
        with pytest.raises(FrameError): session.recv()
    finally:
        a.close(); b.close()


def test_pending_approval_is_not_authenticated():
    a, b = socket.socketpair()
    try:
        session = Session(b, ('test', 1))
        send_frame(a, Frame.json_frame(FrameType.AUTH_OK, {'ok': True, 'pending': True}))
        assert session.recv_auth_result()['pending']
        assert not session.authenticated
    finally:
        a.close(); b.close()


def test_attempt_limiter_global_per_source_expiry_and_bounded_memory(monkeypatch):
    now = [100.]
    monkeypatch.setattr('filetransfer.rate_limit.time.monotonic', lambda: now[0])
    limiter = AttemptLimiter(per_source=2, global_limit=3, capacity=2)
    assert limiter.allow('a') and limiter.allow('a')
    assert not limiter.allow('a')
    assert limiter.allow('b') and not limiter.allow('c')
    now[0] += 61
    assert limiter.allow('c') and len(limiter.sources) <= 2


@pytest.mark.parametrize('packet', [b'[]', b'null', b'7', b'"hello"', b'{', b'\xff'])
def test_discovery_bad_packets_do_not_stop_responder(packet):
    responder = discovery.DiscoveryResponder(lambda: 5000, lambda: '123456')
    class FakeSocket:
        def __init__(self): self.packets = iter([packet, json.dumps({'magic': config.DISCOVERY_MAGIC, 'type':'discover'}).encode()]); self.replies = []
        def recvfrom(self, size):
            try: return next(self.packets), ('127.0.0.1', 12345)
            except StopIteration: raise OSError('closed')
        def sendto(self, data, address): self.replies.append(data)
    responder._sock = FakeSocket()
    responder._loop()
    assert len(responder._sock.replies) == 1 and b'123456' not in responder._sock.replies[0]


@pytest.fixture
def local_ui():
    app = FastAPI()
    app.add_middleware(LocalSecurity)
    @app.get('/api/secret')
    def secret(): return {'pin': '123456'}
    @app.post('/api/change')
    def change(): return {'ok': True}
    @app.websocket('/api/events')
    async def events(ws: WebSocket): await ws.accept(); await ws.send_text('authorized'); await ws.close()
    with TestClient(app, base_url=f'http://127.0.0.1:{config.WEBAPP_PORT}',
                    backend_options={'loop_factory': asyncio.SelectorEventLoop}) as client:
        yield client


def test_local_api_requires_launch_cookie_and_origin(local_ui):
    client = local_ui; origin = str(client.base_url).rstrip('/')
    assert client.get('/api/secret').status_code == 403
    response = client.get('/launch/' + BOOTSTRAP_TOKEN, follow_redirects=False)
    assert response.status_code == 303 and 'HttpOnly' in response.headers['set-cookie']
    assert client.get('/api/secret').json()['pin'] == '123456'
    assert client.post('/api/change').status_code == 403
    assert client.post('/api/change', headers={'Origin':'https://evil.invalid'}).status_code == 403
    assert client.post('/api/change', headers={'Origin':origin}).status_code == 200
    assert client.get('/api/secret', headers={'Host':'evil.invalid'}).status_code == 403
    assert client.get('/api/secret', headers={'Origin':'null'}).status_code == 403
    assert client.get('/launch/' + BOOTSTRAP_TOKEN).status_code == 404


def test_websocket_rejects_unauthorized_and_cross_origin(local_ui):
    from starlette.websockets import WebSocketDisconnect
    client = local_ui; origin = str(client.base_url).rstrip('/')
    url = origin.replace('http:', 'ws:') + '/api/events'
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect(url, headers={'Origin': origin}): pass
    client.get('/launch/' + BOOTSTRAP_TOKEN, follow_redirects=False)
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect(url, headers={'Origin':'https://evil.invalid'}): pass
    with client.websocket_connect(url, headers={'Origin':origin}) as ws:
        assert ws.receive_text() == 'authorized'
