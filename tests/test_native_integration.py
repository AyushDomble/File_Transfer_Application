"""Native TCP/HTTP/WebSocket/Chromium tier; not run by --memory-transport runner."""
from helpers.native_peers import (native_http, native_pair)
import asyncio
import hashlib
import socket
import threading
import time

import httpx
import pytest
import uvicorn

from filetransfer import config
from filetransfer.engine import RecvEngine
from filetransfer.host import TransferHost
from filetransfer.hostlink import HostLink
from filetransfer.session_protocol import SessionError
from filetransfer.transfer import TransferState
from helpers.app_peers import wait
from helpers.browser import browser_page




def test_native_host_restart_waits_for_real_receiver_then_recovers_same_transfer(native_pair, tmp_path, monkeypatch):
    host, link = native_pair
    peer = host.get_peer(host.list_peers()[0]['peer_id'])
    host.shutdown_join_timeout = 0.02
    entered, release = threading.Event(), threading.Event()
    original = RecvEngine._receive_chunk
    def delayed_receive(engine, *args):
        if engine._session is peer.session and not entered.is_set():
            entered.set()
            assert release.wait(12)
        return original(engine, *args)
    monkeypatch.setattr(RecvEngine, '_receive_chunk', delayed_receive)
    source = tmp_path / 'restart.bin'
    source.write_bytes(bytes(range(256)) * 1024)
    transfer = link.scheduler.submit_upload(str(source), chunk_size=4096)
    try:
        assert entered.wait(5)
        with pytest.raises(SessionError, match='still stopping'):
            host.stop()
        with pytest.raises(SessionError, match='still stopping'):
            host.start()
        assert any(item is peer for item in host._shutdown_peers)
    finally:
        release.set()
    assert peer.manager.get(transfer.id).io_finished.wait(5)
    assert transfer.io_finished.wait(5)
    host.stop()
    host.start()
    recovered = link.connect_with_pin(host.current_pin)
    assert transfer.id in recovered
    wait(lambda: transfer.is_terminal)
    assert transfer.state == TransferState.COMPLETED
    assert (tmp_path / 'server/restart.bin').read_bytes() == source.read_bytes()
    new_peer = host.get_peer(peer.peer_id)
    assert new_peer is not peer and new_peer.manager.get(transfer.id).state == TransferState.COMPLETED


def test_native_repeated_reconnect_keeps_single_reader_and_upload_download_integrity(native_pair, tmp_path):
    host, link = native_pair
    manager = link.manager
    for cycle in range(3):
        link.close()
        link.connect_with_pin(host.current_pin)
        assert link.manager is manager
        assert link.router._reader_thread.is_alive()
        first_reader = link.router._reader_thread
        link.router.start_reader()
        assert link.router._reader_thread is first_reader
        source = tmp_path / f'cycle-{cycle}.bin'
        content = bytes(range(256)) * 32
        source.write_bytes(content)
        upload = link.scheduler.submit_upload(str(source), chunk_size=1024)
        wait(lambda: upload.is_terminal)
        assert upload.state == TransferState.COMPLETED
        download = link.scheduler.submit_download(source.name, chunk_size=1024)
        wait(lambda: download.is_terminal)
        assert download.state == TransferState.COMPLETED
        received = (tmp_path / 'client' / source.name).read_bytes()
        assert hashlib.sha256(received).digest() == hashlib.sha256(content).digest()




def test_real_browser_withdraw_resume_native_rest_ws_and_reconnect(native_http, browser_page, tmp_path):
    base, api, host, link = native_http
    link.manager.suspended = True  # deterministic admission wait, not slow-click timing
    source = tmp_path / 'browser-intent.bin'
    source.write_bytes(b'preserve the queued upload')
    created = httpx.post(base + '/api/transfers', headers=api._test_http_headers, json={'direction': 'upload', 'local_path': str(source)})
    assert created.status_code == 202
    tid = created.json()['transfer']['id']
    page = browser_page
    page.context.add_cookies([api._test_browser_cookie])
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.add_init_script('''
      window.transferMessages=[]; window.transferSockets=[];
      const NativeWebSocket=window.WebSocket;
      window.WebSocket=class extends NativeWebSocket {
        constructor(url,...args) {
          super(url,...args);
          if(String(url).includes('/api/transfers/events')) {
            transferSockets.push(this);
            this.addEventListener('message',e=>transferMessages.push(JSON.parse(e.data)));
          }
        }
      };
    ''')
    page.goto(base + '/client.html')
    queue = page.locator('#transferQueue')
    queue.get_by_role('button', name='Pause', exact=True).click()
    queue.get_by_role('button', name='Resume', exact=True).wait_for()
    page.wait_for_function("transferMessages.some(m=>m.type==='TRANSFER_PAUSED')")
    queue.get_by_role('button', name='Resume', exact=True).click()
    queue.locator('.state').filter(has_text='waiting for a slot').wait_for()
    queue.get_by_role('button', name='Pause', exact=True).click()
    queue.get_by_role('button', name='Resume', exact=True).wait_for()
    page.wait_for_function("transferMessages.filter(m=>m.type==='TRANSFER_PAUSED').length>=2")
    current = httpx.get(base + f'/api/transfers/{tid}', headers=api._test_http_headers).json()['transfer']
    assert current['state'] == 'paused' and not current['resume_requested']
    page.evaluate("transferSockets.at(-1).close()")
    page.wait_for_function("transferMessages.filter(m=>m.type==='TRANSFER_SNAPSHOT').length>=2")
    assert queue.locator('.state').inner_text() == 'Paused'
    assert queue.get_by_role('button', name='Resume', exact=True).is_enabled()
    page.reload()
    queue.get_by_role('button', name='Resume', exact=True).wait_for()
    assert queue.locator('.state').inner_text() == 'Paused'
    assert not errors
