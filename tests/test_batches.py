import hashlib
import io
import uuid
import zipfile

import httpx
import pytest
from playwright.sync_api import expect
from helpers.native_peers import native_pair, native_http
from helpers.browser import browser_page
from helpers.app_peers import wait
from filetransfer.transfer import TransferState


def manifest(name, content):
    return dict(filename=name, size=len(content), checksum=hashlib.sha256(content).hexdigest())


def test_strict_gate_and_approved_group(native_pair, tmp_path):
    host, link = native_pair
    host.require_batch_approval = True
    path = tmp_path / 'hello.txt'; path.write_bytes(b'hello')
    denied = link.scheduler.submit_upload(str(path))
    wait(lambda: denied.is_terminal)
    assert denied.state == TransferState.FAILED
    assert not (tmp_path / 'server/hello.txt').exists()
    bid = uuid.uuid4().hex
    result = link.batch('request', dict(batch_id=bid, files=[manifest(path.name, b'hello')]))
    assert result['status'] == 'pending'
    pending = host.batches.pending()
    assert len(pending) == 1
    host.batches.decide(pending[0]['peer_id'], bid, True)
    transfer = link.scheduler.submit_upload(str(path), batch_id=bid)
    wait(lambda: transfer.is_terminal)
    assert transfer.state == TransferState.COMPLETED, transfer.error
    assert (tmp_path / 'server/hello.txt').read_bytes() == b'hello'
    replay = link.scheduler.submit_upload(str(path), batch_id=bid)
    wait(lambda: replay.is_terminal)
    assert replay.state == TransferState.FAILED


def test_prepared_checksum_reuse_still_rejects_modified_source(native_pair, tmp_path, monkeypatch):
    from filetransfer import scheduler
    host, link = native_pair
    host.require_batch_approval = True
    path = tmp_path / 'changed.txt'
    path.write_bytes(b'original')
    expected = hashlib.sha256(b'original').hexdigest()
    bid = uuid.uuid4().hex
    link.batch('request', dict(batch_id=bid, files=[manifest(path.name, b'original')]))
    pending = host.batches.pending()[0]
    host.batches.decide(pending['peer_id'], bid, True)
    with pytest.raises(ValueError, match='owned batch'):
        link.scheduler.submit_upload(str(path), _prepared_checksum=expected)
    path.write_bytes(b'modified')
    def unnecessary_hash(*args):
        raise AssertionError('Prepared sources must not repeat the submission hash')
    monkeypatch.setattr(scheduler, '_compute_sha256', unnecessary_hash)
    transfer = link.scheduler.submit_upload(str(path), source_owned=True, batch_id=bid, _prepared_checksum=expected)
    wait(lambda: transfer.is_terminal)
    assert transfer.state == TransferState.FAILED
    from pathlib import Path
    assert not (Path(host.dest_dir) / path.name).exists()


@pytest.mark.parametrize('zipped', [False, True])
def test_local_batch_flow(native_http, zipped):
    base, api, host, link = native_http
    host.require_batch_approval = True
    files = [('one.txt', b'one' * 200), ('two.txt', b'two' * 100)]
    with httpx.Client(base_url=base, timeout=20, headers=api._test_http_headers) as client:
        bid = client.post('/api/client/batches', json={'files': [dict(filename=n, size=len(b)) for n,b in files], 'zipped':zipped}).json()['batch_id']
        for index, (_, content) in enumerate(files):
            response = client.put(f'/api/client/batches/{bid}/files/{index}?chunk_index=0&final=true', content=content)
            assert response.status_code == 200, response.text
        response = client.post(f'/api/client/batches/{bid}/request')
        assert response.status_code == 202, response.text
        wait(lambda: bool(host.batches.pending()))
        pending = host.batches.pending()
        assert len(pending) == 1 and len(pending[0]['members'] or pending[0]['files']) == 2
        assert not link.manager.all_transfers()
        response = client.post(f"/api/host/batches/{pending[0]['peer_id']}/{bid}/accept")
        assert response.status_code == 200, response.text
        response = client.get(f'/api/client/batches/{bid}')
        assert response.json()['status'] == 'sending', response.text
        wait(lambda: all(t.is_terminal for t in link.manager.all_transfers()))
        assert all(t.state == TransferState.COMPLETED for t in link.manager.all_transfers())
        if zipped:
            with zipfile.ZipFile(host.dest_dir + '/Selection-' + bid[:8] + '.zip') as archive:
                for name, content in files: assert archive.read(name) == content
        else:
            from pathlib import Path
            for name, content in files: assert (Path(host.dest_dir) / name).read_bytes() == content


def test_archive_rules_and_tampered_members(native_pair, tmp_path):
    host, link = native_pair; host.require_batch_approval = True
    host.batches.configure('allow', ['txt'], 10)
    bid = uuid.uuid4().hex
    with pytest.raises(ValueError, match='not allowed'):
        link.batch('request', dict(batch_id=bid, files=[manifest('files.zip', b'x')], members=[manifest('bad.exe', b'x')]))
    path = tmp_path / 'files.zip'
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as archive: archive.writestr('one.txt', b'bad')
    link.batch('request', dict(batch_id=bid, files=[manifest(path.name, path.read_bytes())], members=[manifest('one.txt', b'yes')]))
    pending = host.batches.pending()[0]; host.batches.decide(pending['peer_id'], bid, True)
    transfer = link.scheduler.submit_upload(str(path), batch_id=bid)
    wait(lambda: transfer.is_terminal)
    assert transfer.state == TransferState.FAILED
    assert not (tmp_path / 'server/files.zip').exists()


def test_rules_disk_validation(native_http):
    base, api, host, link = native_http
    with httpx.Client(base_url=base, headers=api._test_http_headers) as client:
        data = client.get('/api/host/settings').json()
        assert data['free_bytes'] > 0
        assert client.post('/api/host/settings', json={'permission':'allow','extensions':[],'max_upload_mb':1}).status_code == 400
        assert client.post('/api/host/settings', json={'permission':'deny','extensions':['exe'],'max_upload_mb':data['free_bytes'] / 1048576 + 1}).status_code == 400
        response = client.post('/api/host/settings', json={'permission':'deny','extensions':['custom'],'max_upload_mb':8})
        assert response.status_code == 200 and response.json()['extensions'] == ['.custom']


@pytest.mark.parametrize('decision', ['reject', 'cancel'])
def test_batch_rejection_and_cancellation(native_http, decision):
    base, api, host, link = native_http
    with httpx.Client(base_url=base, headers=api._test_http_headers) as client:
        bid = client.post('/api/client/batches', json={'files':[{'filename':'empty.txt','size':0}]}).json()['batch_id']
        assert client.put(f'/api/client/batches/{bid}/files/0?chunk_index=0&final=true', content=b'').status_code == 200
        assert client.post(f'/api/client/batches/{bid}/request').json()['status'] == 'preparing'
        wait(lambda: bool(host.batches.pending()))
        item = host.batches.pending()[0]
        if decision == 'reject':
            assert client.post(f"/api/host/batches/{item['peer_id']}/{bid}/reject").status_code == 200
            assert client.get(f'/api/client/batches/{bid}').json()['status'] == 'rejected'
        else:
            assert client.delete(f'/api/client/batches/{bid}').status_code == 200
            assert host.batches.status(item['peer_id'], bid)['status'] == 'cancelled'
        assert not link.manager.all_transfers()
        from pathlib import Path
        assert not list(Path(api._UPLOAD_SCRATCH_DIR).rglob('*'))


def test_approval_identity_and_persistence(native_pair):
    from filetransfer.batches import HostBatches
    host, link = native_pair
    bid = uuid.uuid4().hex
    payload = dict(batch_id=bid, files=[manifest('a.txt', b'a')])
    link.batch('request', payload)
    assert link.batch('request', payload)['status'] == 'pending'
    assert len(host.batches.pending()) == 1
    with pytest.raises(ValueError, match='different selection'):
        link.batch('request', dict(batch_id=bid, files=[manifest('b.txt', b'b')]))
    item = host.batches.pending()[0]
    host.batches.decide(item['peer_id'], bid, True)
    restored = HostBatches(host)
    assert restored.status(item['peer_id'], bid)['status'] == 'approved'
    with pytest.raises(ValueError, match='Unknown batch'):
        restored.status('another-peer', bid)


def test_browser_rules_and_real_group_approval(native_http, browser_page):
    base, api, host, link = native_http; host.require_batch_approval = True
    page = browser_page; errors = []; page.on('pageerror', lambda error: errors.append(str(error)))
    page.context.add_cookies([api._test_browser_cookie])
    page.goto(base + '/host.html'); page.locator('.settings-card > summary').click()
    page.locator('.extension-picker summary').click(); page.locator('#extensionSearch').fill('custom')
    page.locator('#addExtension').click(); expect(page.locator('#extensionChips')).to_contain_text('.custom')
    page.locator('#sizeNumber').fill('12'); expect(page.locator('#sizeSlider')).to_have_value('12')
    page.locator('#saveRules').click(); expect(page.locator('#rulesStatus')).to_contain_text('Rules saved')
    client = page.context.browser.new_page(); client.context.add_cookies([api._test_browser_cookie]); client.goto(base + '/client.html')
    expect(client.locator('#filesCard')).to_be_visible(); client.locator('#zipSelection').check()
    client.locator('#fileInput').set_input_files([{'name':'a.txt','mimeType':'text/plain','buffer':b'alpha'}, {'name':'b.txt','mimeType':'text/plain','buffer':b'beta'}])
    expect(page.locator('#batchRequests')).to_contain_text('2 files')
    page.get_by_role('button', name='Receive group').click()
    expect(client.locator('#transferQueue [data-state=completed]')).to_have_count(1, timeout=20000)
    assert not errors
    client.close()
