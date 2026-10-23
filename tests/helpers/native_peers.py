"""Shared native peers test support."""
import asyncio
import socket
import threading
import httpx
import pytest
import uvicorn
from filetransfer import config
from filetransfer.host import TransferHost
from filetransfer.hostlink import HostLink
from helpers.app_peers import wait


@pytest.fixture
def native_pair(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'STATE_DIR', None)
    monkeypatch.setattr(config, 'HOST', '127.0.0.1')
    monkeypatch.setattr(config, 'PORT', 0)
    monkeypatch.setattr(config, 'TLS_ENABLED', True)
    monkeypatch.setattr(config, 'PIN_EXPIRY_SECONDS', 0)
    host = TransferHost(dest_dir=str(tmp_path / 'server'))
    # UDP discovery is outside these TCP/recovery contracts.
    monkeypatch.setattr(host._discovery, 'start', lambda: None)
    host.add_pending_listener(lambda info: host.accept_pending(info['id']))
    host.start()
    port = host._sock.getsockname()[1]
    monkeypatch.setattr(config, 'PORT', port)  # restart on the same durable endpoint
    link = HostLink('127.0.0.1', port=port, use_tls=True, dest_dir=str(tmp_path / 'client'), fingerprint=host.certificate_fingerprint)
    link.connect_with_pin(host.current_pin)
    wait(lambda: len(host.list_peers()) == 1)
    try:
        yield host, link
    finally:
        link.close()
        host.stop()

@pytest.fixture
def native_http(native_pair, tmp_path, monkeypatch):
    from webapp import api
    from webapp.events import TransferEvents
    host, link = native_pair
    monkeypatch.setattr(api, 'transfer_host', host)
    monkeypatch.setattr(api, 'client_state', {'link': link})
    monkeypatch.setattr(api, '_transfer_events', TransferEvents())
    monkeypatch.setattr(api, '_UPLOAD_SCRATCH_DIR', str(tmp_path / 'scratch'))
    api._bind_client_events(link)
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    port = listener.getsockname()[1]
    monkeypatch.setattr(config, 'WEBAPP_PORT', port)
    api.app.middleware_stack = None  # new launch capability/session for this test server
    server = uvicorn.Server(uvicorn.Config(api.app, host='127.0.0.1', port=port, log_level='warning'))
    # Use Python's native selector TCP transport: Python 3.14's Windows
    # proactor can leave reset browser sockets pending during reload/teardown.
    def serve():
        loop = asyncio.SelectorEventLoop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(server.serve(sockets=[listener]))
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
            loop.close()
            asyncio.set_event_loop(None)
    worker = threading.Thread(target=serve, daemon=True)
    worker.start()
    wait(lambda: server.started or not worker.is_alive(), timeout=10)
    assert server.started
    from webapp.local_security import BOOTSTRAP_TOKEN
    base = f'http://127.0.0.1:{port}'
    launch = httpx.get(base + '/launch/' + BOOTSTRAP_TOKEN, follow_redirects=False)
    assert launch.status_code == 303
    api._test_http_headers = {'Origin': base, 'Cookie': 'ft_session=' + launch.cookies['ft_session']}
    api._test_browser_cookie = {'name': 'ft_session', 'value': launch.cookies['ft_session'], 'url': base, 'httpOnly': True, 'sameSite': 'Strict'}
    try:
        yield f'http://127.0.0.1:{port}', api, host, link
    finally:
        server.should_exit = True
        worker.join(10)
        listener.close()
        assert not worker.is_alive()
