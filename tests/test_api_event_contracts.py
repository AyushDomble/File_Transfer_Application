"""Real in-process ASGI HTTP/WS plus app byte-stream contracts; no TCP listener.

The polling loop deliberately needs no OS self-pipe in restricted sandboxes.
It does not replace any FastAPI, Starlette, HTTPX or application route behavior.
"""
from helpers.asgi import (NoSocketLoop, PollSelector, app_api, open_ws, request, run)
import asyncio
import json
import os
from pathlib import Path
import selectors
import threading
import time
from types import SimpleNamespace

import httpx
import pytest

from filetransfer.manager import TransferManager
from filetransfer.transfer import TransferDirection, TransferState
from webapp.events import TransferEvents
from helpers.app_peers import AppPair, wait












@pytest.mark.parametrize('prefix', ['/api/transfers', '/transfers'])
def test_canonical_create_get_list_and_cancel_are_real_asgi_resources(app_api, tmp_path, prefix):
    api, app = app_api; app.connect(); app.link.manager.suspended = True
    source = tmp_path / 'source'; source.write_bytes(b'abcd')
    async def check():
        created = await request(api, 'POST', prefix, json={'direction': 'upload', 'local_path': str(source)})
        assert created.status_code == 202
        transfer = created.json()['transfer']; tid = transfer['id']
        assert transfer['state'] == 'queued' and transfer['revision'] >= 0
        assert (await request(api, 'GET', prefix + '/' + tid)).json()['transfer']['id'] == tid
        assert len((await request(api, 'GET', prefix)).json()['transfers']) == 1
        cancelled = await request(api, 'POST', prefix + '/' + tid + '/cancel')
        assert cancelled.status_code == 202 and cancelled.json()['transfer']['state'] == 'cancelled'
        assert app.link.connected and source.exists()
    run(check())


@pytest.mark.parametrize('body', [
    {}, {'direction': 'other'}, {'direction': 'upload'}, {'direction': 'download'},
    {'direction': 'download', 'filename': 'a', 'local_path': 'b'},
    {'direction': 'upload', 'local_path': 'a', 'dest_path': 'b'},
    {'direction': 'upload', 'local_path': 'a', 'extra': True},
])
def test_resource_request_validation_returns_422(app_api, body):
    api, _ = app_api
    assert run(request(api, 'POST', '/api/transfers', json=body)).status_code == 422


@pytest.mark.parametrize('action', ['pause', 'resume', 'cancel', 'retry', 'recover'])
def test_missing_resource_actions_are_404_not_false_success(app_api, action):
    api, app = app_api; app.connect()
    response = run(request(api, 'POST', '/api/transfers/unknown/' + action))
    assert response.status_code == 404 and not response.json()['ok']


def test_offline_and_conflict_have_distinct_http_status(app_api):
    api, app = app_api
    assert run(request(api, 'POST', '/api/transfers', json={'direction': 'download', 'filename': 'a'})).status_code == 503
    app.connect()
    transfer = app.link.manager.create_transfer(TransferDirection.UPLOAD, 'queued', 4, start_immediately=False)
    response = run(request(api, 'POST', f'/api/transfers/{transfer.id}/resume'))
    assert response.status_code == 409
    missing = run(request(api, 'POST', '/api/transfers', json={'direction': 'upload', 'local_path': 'not-here'}))
    assert missing.status_code == 404


def test_canonical_upload_and_download_complete_through_real_app(app_api, tmp_path):
    api, app = app_api; app.connect()
    source = tmp_path / 'source.bin'; source.write_bytes(b'actual ASGI app transfer')
    up = run(request(api, 'POST', '/api/transfers', json={'direction': 'upload', 'local_path': str(source)}))
    tid = up.json()['transfer']['id']
    wait(lambda: app.link.manager.get(tid).is_terminal)
    assert app.link.manager.get(tid).state == TransferState.COMPLETED
    down = run(request(api, 'POST', '/api/transfers', json={'direction': 'download', 'filename': source.name}))
    did = down.json()['transfer']['id']
    wait(lambda: app.link.manager.get(did).is_terminal)
    assert app.link.manager.get(did).state == TransferState.COMPLETED
    content = run(request(api, 'GET', f'/api/transfers/{did}/content'))
    assert content.status_code == 200 and content.content == source.read_bytes()
    assert run(request(api, 'GET', f'/api/transfers/{tid}/content')).status_code == 409


def test_upload_staging_is_separate_resource_and_nonblocking(app_api):
    api, app = app_api; app.connect(); app.link.manager.suspended = True
    async def check():
        params = dict(filename='browser.bin', size=4, chunk_index=0, is_final=False)
        first = await request(api, 'POST', '/api/uploads/browser/chunks', params=params, content=b'ab')
        assert first.status_code == 200 and not app.link.manager.all_transfers()
        params.update(chunk_index=1, is_final=True)
        final = await request(api, 'POST', '/api/uploads/browser/chunks', params=params, content=b'cd')
        assert final.status_code == 202 and final.json()['transfer']['state'] == 'queued'
        tid = final.json()['transfer']['id']
        cancel = await request(api, 'DELETE', '/api/uploads/browser')
        assert cancel.status_code == 200 and app.link.manager.get(tid).state == TransferState.CANCELLED
    run(check())


def test_staging_rejects_oversize_stream_without_consuming_rest(app_api):
    api, app = app_api; app.connect()
    yielded = []
    async def body():
        for index in range(20):
            yielded.append(index)
            yield b'a' * (1024 * 1024)
    params = dict(filename='big', size=20 * 1024 * 1024, chunk_index=0, is_final=True)
    response = run(request(api, 'POST', '/api/uploads/big/chunks', params=params, content=body()))
    assert response.status_code == 422 and len(yielded) <= 9
    assert 'big' not in api._upload_sessions and not app.link.manager.all_transfers()


def test_schema_marks_adapters_deprecated_and_has_canonical_resources(app_api):
    api, _ = app_api
    paths = api.app.openapi()['paths']
    for path in ('/api/transfers', '/api/transfers/{transfer_id}', '/api/transfers/{transfer_id}/content',
                 '/api/uploads/{upload_id}/chunks'):
        assert path in paths
    assert paths['/api/client/cancel_transfer']['post']['deprecated']
    assert not paths['/api/transfers']['post'].get('deprecated', False)




def test_asgi_websocket_snapshot_lifecycle_progress_and_reconnect(app_api):
    api, app = app_api
    async def check():
        receive, close = await open_ws(api, 'client')
        assert (await receive())['type'] == 'TRANSFER_SNAPSHOT'
        transfer = app.link.manager.create_transfer(TransferDirection.UPLOAD, 'events', 4, start_immediately=False)
        transfer.update_progress(2)
        api._on_host_progress('other-peer', transfer)
        created, progress = await receive(), await receive()
        assert created['type'] == 'TRANSFER_CREATED'
        assert progress['type'] == 'TRANSFER_PROGRESS' and progress['id'] == transfer.id
        assert progress['offset'] == 2 and progress['revision'] > created['revision']
        assert progress['event_id'] > created['event_id'] and progress['peer_id'] is None
        await close()
        assert not api._transfer_events.subscribers
        app.link.manager.cancel_transfer(transfer.id)
        receive, close = await open_ws(api, 'client')
        snapshot = await receive()
        assert snapshot['transfers'][0]['state'] == 'cancelled'
        await close()
    run(check())


def test_resume_intent_is_emitted_before_admitted_resume(app_api):
    api, app = app_api
    async def check():
        subscriber, _ = api._transfer_events.subscribe('client')
        manager = app.link.manager
        transfer = manager.create_transfer(TransferDirection.UPLOAD, 'resume', 4, start_immediately=False)
        manager.pause_transfer(transfer.id)
        manager.resume_transfer(transfer.id)  # offline: intent waits for admission
        records = list(subscriber.pending)
        assert records[-1]['type'] == 'TRANSFER_RESUMING' and records[-1]['resume_requested']
        api._transfer_events.unsubscribe(subscriber)
    run(check())


@pytest.mark.parametrize('direction', ['upload', 'download'])
def test_both_host_and_client_progress_comes_from_managers_not_submit_callbacks(app_api, tmp_path, direction):
    api, app = app_api; app.connect()
    async def check():
        subscriber, _ = api._transfer_events.subscribe()
        source = (tmp_path if direction == 'upload' else app.server_dir) / 'direct'
        source.write_bytes(b'direct scheduler progress')
        transfer = (app.link.scheduler.submit_upload(str(source)) if direction == 'upload'
                    else app.link.scheduler.submit_download(source.name))  # no progress_cb
        while not transfer.is_terminal:
            await asyncio.sleep(0.005)
        records = list(subscriber.pending)
        progress = [item for item in records if item['type'] == 'TRANSFER_PROGRESS']
        assert {item['side'] for item in progress} == {'client', 'host'}
        assert all(item['id'] == transfer.id and item['offset'] > 0 for item in progress)
        assert all(item['peer_id'] for item in progress if item['side'] == 'host')
        api._transfer_events.unsubscribe(subscriber)
    run(check())


def test_bounded_broker_coalesces_and_resyncs_slow_subscriber():
    async def check():
        hub = TransferEvents(capacity=4)
        slow, _ = hub.subscribe('client'); healthy, _ = hub.subscribe('host')
        for index in range(1000):
            hub.publish(dict(type='TRANSFER_PROGRESS', side='client', peer_id=None, id='same', offset=index))
        assert len(slow.pending) == 1 and slow.pending[-1]['offset'] == 999
        for index in range(100):
            hub.publish(dict(type='TRANSFER_CREATED', side='client', id=str(index)))
        assert len(slow.pending) <= 4 and any(item['type'] == 'TRANSFER_RESYNC' for item in slow.pending)
        assert not healthy.pending
        hub.publish(dict(type='TRANSFER_COMPLETED', side='host', id='other'))
        assert (await healthy.get())['id'] == 'other'
        hub.unsubscribe(slow); hub.unsubscribe(healthy)
    run(check())


def test_threaded_event_ids_are_serialized_and_subscriptions_cleanup():
    async def check():
        hub = TransferEvents(capacity=128)
        subscriber, _ = hub.subscribe()
        threads = [threading.Thread(target=lambda: [hub.publish(dict(type='TRANSFER_CREATED', side='client', id=str(i))) for i in range(20)]) for _ in range(4)]
        for thread in threads: thread.start()
        for thread in threads: thread.join(3)
        records = list(subscriber.pending)
        assert [item['event_id'] for item in records] == list(range(1, 81))
        hub.close()
        assert subscriber.closed and not hub.subscribers
    run(check())


def test_invalid_websocket_side_is_rejected_before_accept(app_api):
    api, _ = app_api
    async def check():
        messages = []
        scope = dict(type='websocket', asgi={'version': '3.0'}, scheme='ws', path='/api/transfers/events',
                     raw_path=b'/api/transfers/events', query_string=b'side=invalid', headers=[(k.lower().encode(), v.encode()) for k,v in api._test_auth_headers.items()],
                     client=('test', 1), server=('test', 80), subprotocols=[], root_path='')
        async def receive(): return {'type': 'websocket.connect'}
        async def send(message): messages.append(message)
        await api.app(scope, receive, send)
        assert messages == [{'type': 'websocket.close', 'code': 1008, 'reason': 'invalid side'}]
        assert not api._transfer_events.subscribers
    run(check())


def test_slow_websocket_times_out_without_blocking_healthy_subscriber(app_api, monkeypatch):
    api, _ = app_api
    monkeypatch.setattr(api, 'WS_SEND_TIMEOUT', 0.03)
    async def check():
        class SlowSocket:
            query_params = {'side': 'client'}
            async def accept(self): pass
            async def send_json(self, payload): await asyncio.Event().wait()
            async def receive_text(self): await asyncio.Event().wait()
            async def close(self, **kwargs): pass
        healthy, _ = api._transfer_events.subscribe('client')
        slow = asyncio.create_task(api.transfer_events_ws(SlowSocket()))
        await asyncio.sleep(0.005)
        api._transfer_events.publish(dict(type='TRANSFER_CREATED', side='client', id='healthy'))
        assert (await healthy.get())['id'] == 'healthy'
        await asyncio.wait_for(slow, 1)
        assert api._transfer_events.subscribers == {healthy}
        api._transfer_events.unsubscribe(healthy)
    run(check())


def test_slow_subscriber_overflow_sends_fresh_snapshot_not_partial_history(app_api):
    api, app = app_api
    async def check():
        receive, close = await open_ws(api, 'client')
        await receive()
        # No event-loop yield: deliberately fill the subscriber queue before
        # its writer can run. Every missing lifecycle must be recoverable.
        transfers = [app.link.manager.create_transfer(TransferDirection.UPLOAD, f'file-{i}', 4,
                     start_immediately=False) for i in range(30)]
        app.link.manager.cancel_transfer(transfers[-1].id)
        snapshot = await receive()
        assert snapshot['type'] == 'TRANSFER_SNAPSHOT'
        assert len(snapshot['transfers']) == 30
        assert snapshot['transfers'][-1]['state'] == 'cancelled'
        await close()
    run(check())


def test_bulk_resources_and_adapters_share_manager(app_api, tmp_path):
    api, app = app_api; app.connect(); app.link.manager.suspended = True
    source = tmp_path / 'adapter'; source.write_bytes(b'abcd')
    async def check():
        created = await request(api, 'POST', '/api/client/transfers/upload', json={'local_path': str(source)})
        assert created.status_code == 200  # compatibility, not canonical status
        tid = created.json()['transfer']['id']
        bulk = await request(api, 'POST', '/api/transfers/actions/cancel_all')
        assert bulk.status_code == 202 and bulk.json()['count'] == 1
        assert app.link.manager.get(tid).state == TransferState.CANCELLED and app.link.connected
    run(check())


def test_missing_content_and_nonterminal_download_are_not_served(app_api):
    api, app = app_api
    assert run(request(api, 'GET', '/api/transfers/missing/content')).status_code == 404
    transfer = app.link.manager.create_transfer(TransferDirection.DOWNLOAD, 'waiting', 4, start_immediately=False)
    assert run(request(api, 'GET', f'/api/transfers/{transfer.id}/content')).status_code == 409


def test_resource_validation_is_transfer_local_and_no_500_for_unsafe_name(app_api, tmp_path):
    api, app = app_api; app.connect(); app.link.manager.suspended = True
    source = tmp_path / 'safe'; source.write_bytes(b'abcd')
    bad = run(request(api, 'POST', '/api/transfers', json={
        'direction': 'upload', 'local_path': str(source), 'filename': '../escape'}))
    assert bad.status_code == 422 and app.link.connected
    good = run(request(api, 'POST', '/api/transfers', json={'direction': 'upload', 'local_path': str(source)}))
    assert good.status_code == 202


def test_actual_lifespan_binds_events_and_cleans_up_loop_and_subscribers(app_api):
    api, app = app_api
    async def check():
        async with api.app.router.lifespan_context(api.app):
            assert api._main_loop is asyncio.get_running_loop()
            subscriber, _ = api._transfer_events.subscribe('client')
            app.link.manager.create_transfer(TransferDirection.UPLOAD, 'lifecycle', 4, start_immediately=False)
            assert (await subscriber.get())['type'] == 'TRANSFER_CREATED'
            # The static actions prefix must precede the parameterized ID path.
            invalid = await request(api, 'POST', '/api/transfers/actions/pause')
            assert invalid.status_code == 422
        assert api._main_loop is None and not api._transfer_events.subscribers
        assert subscriber.closed
    run(check())


def test_snapshot_generation_changes_when_same_id_is_reconstructed():
    from filetransfer.persistence import restore_transfer
    first = TransferManager().create_transfer(TransferDirection.UPLOAD, 'same', 4, start_immediately=False)
    row = dict(id=first.id, filename=first.filename, total_size=4, offset=0,
               state='connection_lost', direction='upload', checksum=None)
    restored = restore_transfer(row, TransferManager(), connection_lost=True)
    assert restored.id == first.id
    assert restored.to_dict()['generation'] != first.to_dict()['generation']
