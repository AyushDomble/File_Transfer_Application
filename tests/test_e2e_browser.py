"""Current authenticated browser upload/approval/pause/resume/disconnect flow."""
import hashlib
import os
import time
from pathlib import Path

from playwright.sync_api import expect
from helpers.native_peers import native_pair, native_http
from helpers.browser import browser_page
from filetransfer.session_protocol import Session
from filetransfer.transport import FrameType


def test_connect_upload_pause_resume_list_disconnect(native_http, browser_page, tmp_path, monkeypatch):
    base, api, host, link = native_http
    host.require_batch_approval = True
    original = Session.send
    def paced(session, frame):
        if frame.frame_type == FrameType.TRANSFER_ACK:
            time.sleep(.1)
        return original(session, frame)
    monkeypatch.setattr(Session, 'send', paced)
    page = browser_page
    page.context.add_cookies([api._test_browser_cookie])
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    host_page = page.context.new_page()
    host_page.on('pageerror', lambda error: errors.append(str(error)))
    host_page.goto(base + '/host.html')
    page.goto(base + '/client.html')
    expect(page.locator('#filesCard')).to_be_visible()
    source = tmp_path / 'e2e_upload.bin'
    source.write_bytes(os.urandom(16 * 1048576))
    page.set_input_files('#fileInput', str(source))
    host_page.locator('#batchRequests button').filter(has_text='Receive group').click(timeout=15000)
    page.locator('#transferQueue').get_by_role('button', name='Pause', exact=True).click(timeout=15000)
    expect(page.locator('#transferQueue .state')).to_have_text('Paused', timeout=10000)
    page.locator('#transferQueue').get_by_role('button', name='Resume', exact=True).click()
    destination = Path(host.dest_dir) / source.name
    deadline = time.monotonic() + 30
    while not destination.exists():
        assert time.monotonic() < deadline
        page.wait_for_timeout(100)
    assert hashlib.sha256(destination.read_bytes()).digest() == hashlib.sha256(source.read_bytes()).digest()
    page.get_by_text('Browse files on host', exact=True).click()
    page.locator('#refreshBtn').click()
    expect(page.locator('#fileList')).to_contain_text(source.name)
    page.locator('#disconnectBtn').click()
    expect(page.locator('#connectCard')).to_be_visible()
    assert not errors
