"""Audit-gap regressions: worker lifetime and revisioned resume withdrawal."""
import threading
import asyncio
import anyio
from types import SimpleNamespace

import pytest

from filetransfer.engine import FrameRouter, RecvEngine, _resume_with_slot
from filetransfer.manager import TransferManager
from filetransfer.session_protocol import SessionError
from filetransfer.transfer import TransferDirection, TransferState, TransferStateError
from filetransfer.transport import FrameType
from helpers.app_peers import app_pair, wait
from helpers.asgi import app_api, open_ws, request, run


@pytest.mark.parametrize('kind', ['transfer', 'control', 'inbound'])
def test_host_retains_stuck_workers_and_refuses_restart_or_rebind(app_pair, kind):
    app = app_pair
    app.connect()
    app.drop()
    host, peer = app.host, app.peer
    host.shutdown_join_timeout = 0.01
    class Worker:
        alive = True
        def join(self, timeout=None): pass
        def is_alive(self): return self.alive
    worker = Worker()
    if kind == 'transfer':
        peer.scheduler._worker_threads.add(worker)
    elif kind == 'control':
        peer.control_thread.join(2)
        peer.control_thread = worker
    else:
        assert peer.scheduler.stop_accept_loop()
        peer.scheduler._accept_thread = worker
    try:
        with pytest.raises(SessionError, match='still stopping'):
            host.stop()
        assert not host.running and host.stopping
        assert any(item is peer for item in host._shutdown_peers)
        with pytest.raises(SessionError, match='still stopping'):
            host.start()  # must fail BEFORE opening a listener
        with pytest.raises(SessionError, match='old host workers'):
            host._install_peer(SimpleNamespace(recovery_id=peer.peer_id), ('new', 1))
    finally:
        worker.alive = False
    host.stop()
    assert not host.stopping and not host._shutdown_peers


def test_real_receiver_worker_must_exit_before_host_shutdown_finishes(app_pair, monkeypatch):
    app = app_pair
    app.connect()
    host, peer = app.host, app.peer
    host.shutdown_join_timeout = 0.01
    entered, release = threading.Event(), threading.Event()
    original = RecvEngine._receive_chunk
    def delayed_receive(engine, *args):
        if engine._session is peer.session and not entered.is_set():
            entered.set()
            assert release.wait(10)
        return original(engine, *args)
    monkeypatch.setattr(RecvEngine, '_receive_chunk', delayed_receive)
    source = app.root / 'slow-receive.bin'
    source.write_bytes(b'unchanged source for recovery')
    transfer = app.link.scheduler.submit_upload(str(source))
    try:
        assert entered.wait(5)
        with pytest.raises(SessionError, match='still stopping'):
            host.stop()
        with pytest.raises(SessionError, match='still stopping'):
            host.start()
        assert source.read_bytes() == b'unchanged source for recovery'
    finally:
        release.set()
    assert peer.manager.get(transfer.id).io_finished.wait(5)
    host.stop()
    assert not host._shutdown_peers


def test_stale_host_generation_cannot_attach_after_stop_and_restart(app_pair):
    host = app_pair.host
    generation = host._generation
    host.stop()
    host._stop_flag.clear()  # emulate a new listener, without binding TCP
    with pytest.raises(SessionError, match='incarnation stopped'):
        host._install_peer(SimpleNamespace(), ('old-handler', 1), generation=generation)
    assert not host.list_peers()


def test_scheduler_retains_live_inbound_thread_and_refuses_session_rebind(app_pair):
    app = app_pair
    app.connect()
    scheduler = app.link.scheduler
    assert scheduler.stop_accept_loop()
    class Worker:
        alive = True
        def join(self, timeout=None): pass
        def is_alive(self): return self.alive
    worker = Worker()
    scheduler._accept_thread = worker
    old_session = scheduler._session
    try:
        assert not scheduler.stop_accept_loop(timeout=0)
        assert scheduler._accept_thread is worker
        with pytest.raises(TransferStateError, match='old inbound worker'):
            scheduler.attach_session(SimpleNamespace(), SimpleNamespace())
        assert scheduler._session is old_session
    finally:
        worker.alive = False
    assert scheduler.stop_accept_loop() and scheduler._accept_thread is None


def test_repeated_concurrent_router_start_has_exactly_one_reader(monkeypatch):
    router = FrameRouter(SimpleNamespace())
    entered, release = threading.Event(), threading.Event()
    calls = []
    def read():
        calls.append(threading.current_thread())
        entered.set()
        release.wait(5)
    monkeypatch.setattr(router, '_reader_loop', read)
    callers = [threading.Thread(target=router.start_reader) for _ in range(12)]
    try:
        for caller in callers: caller.start()
        for caller in callers: caller.join(2)
        assert entered.wait(2) and len(calls) == 1
        assert router._reader_thread is calls[0]
    finally:
        release.set()
        router._reader_thread.join(2)


def test_resume_withdrawal_is_revisioned_notified_and_idempotent():
    manager = TransferManager(max_concurrent=1)
    first = manager.create_transfer(TransferDirection.UPLOAD, 'first', 4)
    manager.pause_transfer(first.id)
    manager.confirm_paused(first.id)
    manager.create_transfer(TransferDirection.UPLOAD, 'occupying-slot', 4)
    manager.resume_transfer(first.id)
    assert first.state == TransferState.PAUSED and first.resume_requested
    before = first.revision
    events = []
    manager.add_listener(lambda t, old, new: events.append((t.to_dict(), old, new)))
    manager.pause_transfer(first.id)
    assert first.revision == before + 1 and not first.resume_requested
    assert len(events) == 1
    assert events[0][1:] == (TransferState.PAUSED, TransferState.PAUSED)
    assert not events[0][0]['resume_requested']
    manager.pause_transfer(first.id)
    assert first.revision == before + 1 and len(events) == 1


def test_simultaneous_resume_admission_does_not_hold_other_transfer_locks(monkeypatch):
    import queue
    manager = TransferManager(max_concurrent=2)
    manager.suspended = True
    transfers = [manager.create_transfer(TransferDirection.UPLOAD, str(i), 4,
                                         start_immediately=False) for i in range(2)]
    for transfer in transfers: manager.pause_transfer(transfer.id)
    manager.suspended = False
    barrier = threading.Barrier(2)
    original = manager.resume_transfer
    def synchronized_resume(tid):
        barrier.wait(3)
        return original(tid)
    monkeypatch.setattr(manager, 'resume_transfer', synchronized_resume)
    results = []
    engines = [SimpleNamespace(_manager=manager, _transfer=t, _queue=queue.Queue(),
                              _pause_flag=threading.Event(), _cancel_event=threading.Event())
               for t in transfers]
    workers = [threading.Thread(target=lambda engine=e: results.append(_resume_with_slot(engine, engine._transfer.id)),
                                daemon=True) for e in engines]
    for worker in workers: worker.start()
    for worker in workers: worker.join(4)
    assert all(not worker.is_alive() for worker in workers), 'cross-transfer admission deadlock'
    assert results == [True, True]


@pytest.mark.parametrize('limited_side', ['sender', 'receiver'])
def test_live_resume_withdrawal_stays_paused_then_fresh_resume_completes(app_pair, monkeypatch, limited_side):
    app = app_pair
    app.connect()
    peer = app.peer
    limited = app.link.manager if limited_side == 'sender' else peer.manager
    limited.max_concurrent = 1
    original_send = app.link.session.send
    entered = [threading.Event(), threading.Event()]
    release = [threading.Event(), threading.Event()]
    ordinal = []
    def gated_send(frame):
        if frame.frame_type == FrameType.TRANSFER_DATA and frame.sequence == 0:
            if frame.transfer_id not in ordinal:
                ordinal.append(frame.transfer_id)
            index = ordinal.index(frame.transfer_id)
            if index < 2:
                entered[index].set()
                assert release[index].wait(10)
        return original_send(frame)
    monkeypatch.setattr(app.link.session, 'send', gated_send)
    first_source = app.root / 'first.bin'; first_source.write_bytes(b'abcd' * 2048)
    second_source = app.root / 'second.bin'; second_source.write_bytes(b'efgh' * 1024)
    first = app.link.scheduler.submit_upload(str(first_source), chunk_size=1024)
    try:
        assert entered[0].wait(3)
        app.link.scheduler.pause(first.id)
        release[0].set()
        wait(lambda: first.state == peer.manager.get(first.id).state == TransferState.PAUSED, timeout=5)
        second = app.link.scheduler.submit_upload(str(second_source), chunk_size=1024)
        assert entered[1].wait(3)
        app.link.scheduler.resume(first.id)
        wait(lambda: limited.get(first.id).resume_requested, timeout=5)
        app.link.scheduler.pause(first.id)
        wait(lambda: not limited.get(first.id).resume_requested, timeout=5)
        release[1].set()
        wait(lambda: second.is_terminal, timeout=5)
        assert second.state == TransferState.COMPLETED
        assert first.state == TransferState.PAUSED
        assert not first.resume_requested and not peer.manager.get(first.id).resume_requested
        app.link.scheduler.resume(first.id)
        wait(lambda: first.is_terminal, timeout=5)
        assert first.state == TransferState.COMPLETED, first.error
        assert (app.server_dir / first_source.name).read_bytes() == first_source.read_bytes()
    finally:
        for gate in release: gate.set()


@pytest.mark.parametrize('side', ['client', 'host'])
def test_resume_withdrawal_delivered_over_asgi_websocket_and_reconnect(app_api, side):
    api, app = app_api
    manager = app.link.manager
    if side == 'host':
        manager = TransferManager()
        manager.namespace = 'peer:audit'
        peer = SimpleNamespace(manager=manager, peer_id='audit', addr=('test-peer', 1), session=None)
        app.host._peers['audit'] = peer
        manager.add_listener(lambda t, old, new: api._on_host_transfer_change('audit', t, old, new))
    manager.suspended = True
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'pending-resume', 4,
                                       start_immediately=False)
    manager.pause_transfer(transfer.id)
    async def check():
        receive, close = await open_ws(api, side)
        assert (await receive())['type'] == 'TRANSFER_SNAPSHOT'
        manager.resume_transfer(transfer.id)
        resuming = await receive()
        manager.pause_transfer(transfer.id)
        paused = await receive()
        assert resuming['type'] == 'TRANSFER_RESUMING' and resuming['resume_requested']
        assert paused['type'] == 'TRANSFER_PAUSED' and not paused['resume_requested']
        assert paused['revision'] > resuming['revision']
        assert paused['event_id'] > resuming['event_id'] and paused['side'] == side
        await close()
        receive, close = await open_ws(api, side)
        snapshot = await receive()
        assert not snapshot['transfers'][0]['resume_requested']
        assert snapshot['transfers'][0]['revision'] == paused['revision']
        await close()
    try:
        run(check())
    finally:
        if side == 'host': app.host._peers.pop('audit')


def test_canonical_pause_action_returns_new_revision_after_resume_withdrawal(app_api):
    api, app = app_api
    app.connect()
    manager = app.link.manager
    manager.suspended = True
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'paused', 4, start_immediately=False)
    manager.pause_transfer(transfer.id)
    manager.resume_transfer(transfer.id)
    before = transfer.revision
    result = run(request(api, 'POST', f'/api/transfers/{transfer.id}/pause'))
    assert result.status_code == 202
    snapshot = result.json()['transfer']
    assert not snapshot['resume_requested'] and snapshot['revision'] == before + 1


def test_websocket_anyio_cancellation_cleans_children_and_subscription(app_api):
    api, _ = app_api
    async def check():
        incoming, outgoing = asyncio.Queue(), asyncio.Queue()
        scope = dict(type='websocket', asgi={'version': '3.0'}, scheme='ws',
                     path='/api/transfers/events', raw_path=b'/api/transfers/events',
                     query_string=b'side=client', headers=[(k.lower().encode(), v.encode()) for k,v in api._test_auth_headers.items()], client=('test', 1),
                     server=('test', 80), subprotocols=[], root_path='')
        await incoming.put({'type': 'websocket.connect'})
        async with anyio.create_task_group() as group:
            group.start_soon(api.app, scope, incoming.get, outgoing.put)
            assert (await outgoing.get())['type'] == 'websocket.accept'
            assert (await outgoing.get())['type'] == 'websocket.send'
            assert api._transfer_events.subscribers
            group.cancel_scope.cancel()
        assert not api._transfer_events.subscribers
    run(check())


def test_browser_owned_source_has_one_cleanup_owner(app_api, monkeypatch):
    from pathlib import Path
    api, app = app_api
    app.connect()
    path = Path(api._UPLOAD_SCRATCH_DIR) / 'single-owner.tmp'
    original = api.os.remove
    removed = []
    def remove(filename, *args, **kwargs):
        if Path(filename) == path:
            removed.append(filename)
        return original(filename, *args, **kwargs)
    monkeypatch.setattr(api.os, 'remove', remove)
    result = run(request(api, 'POST', '/api/uploads/single-owner/chunks',
                         params=dict(filename='owned.bin', size=4, chunk_index=0, is_final=True),
                         content=b'abcd'))
    assert result.status_code == 202
    transfer = app.link.manager.get(result.json()['transfer']['id'])
    wait(lambda: transfer.is_terminal, timeout=5)
    assert transfer.state == TransferState.COMPLETED
    wait(lambda: removed and not path.exists(), timeout=5)
    assert len(removed) == 1
    assert 'single-owner' not in api._upload_transfers


@pytest.mark.parametrize('action', ['start', 'stop'])
def test_host_worker_conflicts_are_explicit_http_errors(app_api, monkeypatch, action):
    api, app = app_api
    def blocked(): raise SessionError('host workers are still stopping')
    with monkeypatch.context() as patch:
        patch.setattr(app.host, action, blocked)
        result = run(request(api, 'POST', '/api/host/' + action))
    assert result.status_code == 409 and result.json()['stopping']
    assert not result.json()['ok'] and 'still stopping' in result.json()['error']
