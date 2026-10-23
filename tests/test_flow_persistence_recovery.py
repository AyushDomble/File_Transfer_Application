"""Live-app recovery wiring over socket pairs, plus durable journal contracts.

--memory-transport explicitly substitutes the byte stream, not app objects.
Native TCP/TLS/browser integration is still a separate verification tier.
"""
from helpers.app_peers import (AppPair, app_pair, wait)
import hashlib
import os
from pathlib import Path
import queue
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from filetransfer import config
from filetransfer.engine import FrameRouter, _decode_data_payload
from filetransfer.flowcontrol import GlobalWindow, TransferWindow, MAX_WINDOW_BYTES
from filetransfer.host import TransferHost
from filetransfer.hostlink import HostLink, ConnectionRejected, ConnectionLost
from filetransfer.manager import TransferManager
from filetransfer.persistence import (PersistenceStore, _SCHEMA, save_transfer,
                                      load_transfer_records)
from filetransfer.scheduler import ConcurrentScheduler
from filetransfer.transfer import TransferDirection, TransferState
from filetransfer.transport import Frame, FrameType
from helpers.protocol import RecordingSession, receiver, data_frame








def test_app_auth_flow_and_default_stores_are_wired(app_pair):
    app = app_pair
    app.connect()
    assert app.link.connected and app.peer.session.authenticated
    assert app.link.scheduler._persistence is app.link._store
    assert app.peer.scheduler._persistence is not None
    assert app.link.router._keepalive_thread.is_alive()
    assert app.peer.router._keepalive_thread.is_alive()
    assert app.link.scheduler._send_window is not None
    assert app.link.manager.namespace != app.peer.manager.namespace


def test_global_window_keeps_debt_after_partial_release():
    window = GlobalWindow(100)
    assert window.acquire(80)
    window.resize(50)
    window.release(20)
    assert window.outstanding == 60 and window.available == 0
    assert not window.acquire(1, timeout=0)
    window.release(20)
    assert window.available == 10
    assert window.acquire(10)
    assert window.outstanding == 50 and window.available == 0


def test_admission_checks_session_loss_before_listener_finishes(app_pair):
    app = app_pair; app.connect()
    app.link.session.mark_lost()
    # Deliberately leave manager.suspended false to expose the callback gap.
    assert not app.link.manager.suspended
    source = app.root / 'stay-queued.bin'; source.write_bytes(b'abcd')
    transfer = app.link.scheduler.submit_upload(str(source))
    assert transfer.state == TransferState.QUEUED
    assert app.link.manager.running_count() == 0


@pytest.mark.parametrize('kind', [GlobalWindow, TransferWindow])
@pytest.mark.parametrize('capacity', [0, -1, True, '4', MAX_WINDOW_BYTES + 1])
def test_window_capacities_are_strictly_bounded(kind, capacity):
    with pytest.raises(ValueError):
        kind(capacity)


@pytest.mark.parametrize('signal', ['cancel', 'pause'])
def test_available_budget_does_not_ignore_stop_signals(signal):
    window = TransferWindow(100)
    event = threading.Event(); event.set()
    kwargs = {'cancel_event' if signal == 'cancel' else 'pause_flag': event}
    assert not window.acquire(10, **kwargs)
    assert window.outstanding == 0


def test_peer_scopes_and_identity_settings_survive_reopen(tmp_path):
    path = str(tmp_path / 'state.sqlite3')
    store = PersistenceStore(path)
    first, second = store.scoped('peer:a'), store.scoped('peer:b')
    manager = TransferManager()
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'one', 4, transfer_id='a' * 32)
    first.save(transfer)
    transfer.filename = 'two'
    second.save(transfer)
    assert first.load(transfer.id)['filename'] == 'one'
    assert second.load(transfer.id)['filename'] == 'two'
    secret = store.get_or_create_setting('identity:test', lambda: os.urandom(32).hex())
    reopened = PersistenceStore(path)
    assert reopened.get_or_create_setting('identity:test', lambda: 'wrong') == secret
    assert reopened.list_scopes('peer:') == ['peer:a', 'peer:b']


def test_old_database_migrates_without_losing_records(tmp_path):
    path = str(tmp_path / 'old.sqlite3')
    with sqlite3.connect(path) as database:
        database.executescript(_SCHEMA)
    store = PersistenceStore(path)
    transfer = TransferManager().create_transfer(TransferDirection.UPLOAD, 'file', 4)
    transfer.initiator, transfer.source_path = True, str(tmp_path / 'source')
    transfer.source_owned, transfer.chunk_size, transfer.overwrite = True, 2048, False
    store.save(transfer)
    record = PersistenceStore(path).load(transfer.id)
    assert record['initiator'] and record['source_owned']
    assert record['chunk_size'] == 2048 and not record['overwrite']
    assert record['source_path'] == transfer.source_path


def test_progress_snapshot_cannot_overwrite_later_cancel(tmp_path, monkeypatch):
    store = PersistenceStore(str(tmp_path / 'ordered.sqlite3'))
    manager = TransferManager()
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'file', 4)
    manager.add_listener(lambda t, old, new: store.save(t))
    entered, release = threading.Event(), threading.Event()
    save = store.save_snapshot
    def blocked(*args, **kwargs):
        if threading.current_thread().name == 'checkpoint':
            entered.set()
            assert release.wait(5)
        return save(*args, **kwargs)
    monkeypatch.setattr(store, 'save_snapshot', blocked)
    checkpoint = threading.Thread(target=lambda: store.save(transfer), name='checkpoint')
    cancel = threading.Thread(target=lambda: manager.cancel_transfer(transfer.id))
    checkpoint.start()
    try:
        assert entered.wait(5)
        acquired = transfer.lock.acquire(blocking=False)
        if acquired:
            transfer.lock.release()
        assert not acquired, 'checkpoint must hold the transfer lock through commit'
        cancel.start()
    finally:
        release.set(); checkpoint.join(5)
        if cancel.ident is not None:
            cancel.join(5)
    assert not checkpoint.is_alive() and not cancel.is_alive()
    assert store.load(transfer.id)['state'] == 'cancelled'


def test_receiver_commits_durable_checkpoint_before_ack(tmp_path, monkeypatch):
    session, _, transfer, engine = receiver(tmp_path, b'abcd')
    store = PersistenceStore(str(tmp_path / 'state.sqlite3'))
    engine._persistence = store
    events = []
    fsync, save, send = os.fsync, store.save, session.send_json
    def record_fsync(fd):
        events.append('fsync'); return fsync(fd)
    def record_save(*args, **kwargs):
        result = save(*args, **kwargs); events.append('commit'); return result
    def record_send(kind, payload, **kwargs):
        if kind == FrameType.TRANSFER_ACK:
            events.append('ack')
        return send(kind, payload, **kwargs)
    monkeypatch.setattr(os, 'fsync', record_fsync)
    monkeypatch.setattr(store, 'save', record_save)
    monkeypatch.setattr(session, 'send_json', record_send)
    engine._queue.put(data_frame(transfer, b'abcd'))
    engine.run()
    assert transfer.state == TransferState.COMPLETED
    assert events.index('fsync') < events.index('commit') < events.index('ack')
    assert store.load(transfer.id)['offset'] == 4


def test_journal_failure_never_acknowledges_uncommitted_progress(tmp_path, monkeypatch):
    session, _, transfer, engine = receiver(tmp_path, b'abcd')
    engine._persistence = PersistenceStore(str(tmp_path / 'state.sqlite3'))
    def unavailable(*args, **kwargs):
        raise OSError('simulated journal failure')
    monkeypatch.setattr(engine._persistence, 'save', unavailable)
    engine._queue.put(data_frame(transfer, b'abcd'))
    engine.run()
    assert transfer.state == TransferState.FAILED
    assert not any(frame.frame_type == FrameType.TRANSFER_ACK for frame in session.frames)
    assert not (tmp_path / 'test.bin').exists()


def test_app_automatically_advertises_both_flow_windows(app_pair, monkeypatch):
    app = app_pair; app.connect()
    frames = []
    original = app.peer.session.send
    def send(frame):
        if frame.frame_type == FrameType.WINDOW_UPDATE:
            frames.append(frame)
        original(frame)
    monkeypatch.setattr(app.peer.session, 'send', send)
    source = app.root / 'flow.bin'; source.write_bytes(b'abcd' * 4096)
    transfer = app.link.scheduler.submit_upload(str(source), chunk_size=4096)
    wait(lambda: transfer.is_terminal)
    assert transfer.state == TransferState.COMPLETED, transfer.error
    assert {'global', 'transfer'} <= {frame.json()['scope'] for frame in frames}
    assert len(frames) >= 4 and all(frame.json()['automatic'] for frame in frames)
    assert app.link.scheduler._send_window.outstanding == 0


@pytest.mark.parametrize('restart', ['client', 'host', 'both'])
@pytest.mark.parametrize('direction', ['upload', 'download'])
def test_app_recovery_survives_recreation_with_saved_ids_and_offsets(app_pair, restart, direction):
    app = app_pair; app.connect()
    # Enough chunks to interrupt at a nonzero offset and verify the suffix,
    # without turning FULL/fsync durability checks into disk benchmarks.
    content = os.urandom(24 * 4096 + 91)
    source = app.root / 'up.bin' if direction == 'upload' else app.server_dir / 'down.bin'
    source.write_bytes(content)
    def slow(done, total):
        time.sleep(0.007)
    transfer = (app.link.scheduler.submit_upload(str(source), chunk_size=4096, progress_cb=slow)
                if direction == 'upload' else app.link.scheduler.submit_download(source.name, chunk_size=4096, progress_cb=slow))
    wait(lambda: transfer.offset >= 4 * 4096)
    app.drop()
    saved_id, saved_offset = transfer.id, transfer.offset
    assert 0 < saved_offset < len(content)
    if restart in ('client', 'both'):
        app.link.close(); app.new_client()
        assert app.link.manager.get(saved_id).state == TransferState.CONNECTION_LOST
    if restart in ('host', 'both'):
        app.host.stop(); app.new_host()
        assert app.host.list_peers() and not app.host.list_peers()[0]['connected']
    recovered = app.connect()
    assert saved_id in recovered
    final = app.link.manager.get(saved_id)
    wait(lambda: final.is_terminal)
    assert final.state == TransferState.COMPLETED, final.error
    destination = (app.server_dir if direction == 'upload' else app.client_dir) / source.name
    assert destination.read_bytes() == content
    wait(lambda: app.peer.manager.get(saved_id).state == TransferState.COMPLETED)
    wait(lambda: app.link._store.load(saved_id)['state'] == 'completed')


def test_dropped_app_link_reuses_manager_instead_of_fresh_transfers(app_pair):
    app = app_pair; app.connect()
    manager, remote_manager = app.link.manager, app.peer.manager
    app.drop()
    app.connect()
    assert app.link.manager is manager and app.peer.manager is remote_manager


def test_rejected_reconnect_preserves_saved_records(app_pair):
    app = app_pair; app.connect()
    manager = app.link.manager
    app.drop()
    with pytest.raises(ConnectionRejected):
        app.connect(pin='not-the-pin')
    assert app.link.manager is manager and not app.link.connected
    assert len(app.host.list_peers()) == 1


def test_paused_intent_survives_both_sides_restarting(app_pair):
    app = app_pair; app.connect()
    content = os.urandom(32 * 4096)
    source = app.root / 'paused.bin'; source.write_bytes(content)
    transfer = app.link.scheduler.submit_upload(str(source), chunk_size=4096,
                                               progress_cb=lambda done, total: time.sleep(0.01))
    wait(lambda: transfer.offset >= 8192)
    app.link.scheduler.pause(transfer.id)
    wait(lambda: transfer.state == TransferState.PAUSED)
    wait(lambda: app.peer.manager.get(transfer.id).state == TransferState.PAUSED)
    app.drop(); app.link.close(); app.host.stop()
    app.new_client(); app.new_host()
    assert app.connect() == []
    restored = app.link.manager.get(transfer.id)
    assert restored.state == TransferState.PAUSED
    app.link.scheduler.resume(restored.id)
    wait(lambda: restored.is_terminal)
    assert restored.state == TransferState.COMPLETED, restored.error
    assert (app.server_dir / source.name).read_bytes() == content


@pytest.mark.parametrize('payload', [dict(capacity=0), dict(capacity=True), dict(capacity='4'),
    dict(capacity=MAX_WINDOW_BYTES + 1), dict(capacity=100, scope='unknown')])
def test_malformed_window_updates_fail_only_target_transfer(tmp_path, payload):
    session, manager = RecordingSession(), TransferManager()
    session.supports = lambda feature: True
    router = FrameRouter(session)
    scheduler = ConcurrentScheduler(session, manager, router, dest_dir=str(tmp_path))
    bad = manager.create_transfer(TransferDirection.UPLOAD, 'bad')
    healthy = manager.create_transfer(TransferDirection.UPLOAD, 'healthy')
    router.dispatch(Frame.json_frame(FrameType.WINDOW_UPDATE, payload, transfer_id=bad.id))
    assert bad.state == TransferState.FAILED and healthy.state == TransferState.ACTIVE
    assert not session.closed


def test_window_update_reaches_paused_transfer_without_engine_queue(tmp_path):
    session, manager = RecordingSession(), TransferManager()
    session.supports = lambda feature: True
    router = FrameRouter(session)
    scheduler = ConcurrentScheduler(session, manager, router, dest_dir=str(tmp_path))
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'paused')
    budget = scheduler._send_budget(transfer)
    manager.pause_transfer(transfer.id); manager.confirm_paused(transfer.id)
    router.dispatch(Frame.json_frame(FrameType.WINDOW_UPDATE,
        dict(capacity=1024, scope='transfer'), transfer_id=transfer.id))
    assert budget.capacity == 1024 and transfer.state == TransferState.PAUSED
    assert scheduler._send_window.capacity == config.GLOBAL_WINDOW_BYTES


def test_initial_peer_window_bounds_first_data_frame(app_pair, monkeypatch):
    app = app_pair
    monkeypatch.setattr(config, 'TRANSFER_WINDOW_BYTES', 1024)
    app.connect()
    lengths = []
    original_send = app.link.session.send
    def send(frame):
        if frame.frame_type == FrameType.TRANSFER_DATA:
            lengths.append(len(_decode_data_payload(frame.payload)[1]))
        original_send(frame)
    monkeypatch.setattr(app.link.session, 'send', send)
    source = app.root / 'small-window.bin'; source.write_bytes(os.urandom(8192))
    transfer = app.link.scheduler.submit_upload(str(source), chunk_size=8192)
    wait(lambda: transfer.is_terminal)
    assert transfer.state == TransferState.COMPLETED, transfer.error
    assert lengths and max(lengths) <= 1024


def test_same_transfer_id_cannot_adopt_another_peers_partial(tmp_path):
    from helpers.protocol import create
    first, second = TransferManager(), TransferManager()
    first.namespace, second.namespace = 'peer:one', 'peer:two'
    session = RecordingSession()
    original, _, original_path = create(session, first, FrameRouter(session), tmp_path)
    Path(original_path).write_bytes(b'ab')
    first.mark_connection_lost(original.id)
    other, offset, other_path = create(session, second, FrameRouter(session), tmp_path)
    assert offset == 0 and original_path != other_path
    assert Path(original_path).read_bytes() == b'ab'
    first.cancel_transfer(original.id); second.cancel_transfer(other.id)


def test_lost_complete_is_recovered_after_host_restart_without_resend(app_pair, monkeypatch):
    app = app_pair; app.connect()
    original_send = app.peer.session.send
    dropped = threading.Event()
    def send(frame):
        if frame.frame_type == FrameType.TRANSFER_COMPLETE:
            dropped.set(); return
        original_send(frame)
    monkeypatch.setattr(app.peer.session, 'send', send)
    source = app.root / 'complete.bin'; source.write_bytes(os.urandom(16384))
    transfer = app.link.scheduler.submit_upload(str(source), chunk_size=4096)
    assert dropped.wait(5)
    app.drop(); app.host.stop(); app.new_host()
    sent = []
    # The existing job already closes over its factory; count DATA on the new
    # session instead by wrapping Session.send just for this reconnect.
    from filetransfer.session_protocol import Session
    original_session_send = Session.send
    def track(session, frame):
        if frame.frame_type == FrameType.TRANSFER_DATA:
            sent.append(frame)
        return original_session_send(session, frame)
    monkeypatch.setattr(Session, 'send', track)
    app.connect()
    wait(lambda: transfer.state == TransferState.COMPLETED)
    assert sent == [] and (app.server_dir / source.name).read_bytes() == source.read_bytes()


def test_keepalive_is_enabled_in_live_app_and_detects_silent_peer(app_pair, monkeypatch):
    app = app_pair
    monkeypatch.setattr(config, 'KEEPALIVE_INTERVAL_SECONDS', 0.05)
    monkeypatch.setattr(config, 'KEEPALIVE_TIMEOUT_SECONDS', 0.2)
    app.connect()
    app.peer.router.stop_reader()
    # Session loss is visible before its scheduler listener finishes.
    wait(lambda: not app.link.connected and app.link.manager.suspended)


def test_reconnect_will_not_bind_new_session_over_live_old_worker(app_pair):
    from filetransfer.session_protocol import Session
    app = app_pair; app.connect(); app.drop()
    scheduler, old_session = app.link.scheduler, app.link.session
    class Unstopped:
        def is_alive(self): return True
        def join(self, timeout=None): pass
    stale = Unstopped()
    scheduler._worker_threads.add(stale)
    a, b = socket.socketpair()
    new_session = Session(a, ('new', 0))
    try:
        with pytest.raises(Exception, match='old transfer worker'):
            scheduler.attach_session(new_session, FrameRouter(new_session))
        assert scheduler._session is old_session
    finally:
        scheduler._worker_threads.discard(stale)
        a.close(); b.close()


def test_offline_api_preserves_link_and_saved_transfer_queue(app_pair, monkeypatch):
    from webapp import api
    app = app_pair; app.connect()
    app.link.manager.suspended = True
    source = app.root / 'queued.bin'; source.write_bytes(b'abcd')
    transfer = app.link.scheduler.submit_upload(str(source))
    app.drop()
    monkeypatch.setattr(api, 'client_state', {'link': app.link})
    result = api._handle_lost_connection(ConnectionLost('test'))
    assert result['recoverable'] and api.client_state['link'] is app.link
    assert api.list_transfers()['transfers'][0]['id'] == transfer.id
    status = api.client_status()
    assert not status['connected'] and status['recoverable'] == 1
    assert app.link._recovery_key not in str(status)


def test_api_connect_reuses_existing_link_and_manager(app_pair, monkeypatch):
    from webapp import api
    app = app_pair; app.connect(); app.drop()
    manager = app.link.manager
    monkeypatch.setattr(api, 'client_state', {'link': app.link})
    called = []
    monkeypatch.setattr(app.link, 'connect_with_pin', lambda pin: called.append(pin) or [])
    monkeypatch.setattr(api, 'HostLink', lambda *args, **kwargs: pytest.fail('must reuse link'))
    result = api.client_connect(api.ConnectRequest(host='test-host', pin='fresh'))
    assert result['ok'] and called == ['fresh']
    assert api.client_state['link'].manager is manager


def test_api_startup_restores_last_endpoint_without_authenticating(app_pair, monkeypatch):
    from webapp import api
    app = app_pair; app.connect()
    app.link.manager.suspended = True
    source = app.root / 'queued.bin'; source.write_bytes(b'abcd')
    transfer = app.link.scheduler.submit_upload(str(source))
    app.link.close()
    monkeypatch.setattr(config, 'CLIENT_DIR', str(app.client_dir))
    monkeypatch.setattr(api, 'client_state', {'link': None})
    monkeypatch.setattr(api, 'default_store', lambda *args: app.link._root_store)
    api._restore_client_state()
    restored = api.client_state['link']
    assert not restored.connected and restored.manager.get(transfer.id).offset == 0
    assert restored._recovery_key == app.link._recovery_key


def test_offline_cancel_is_durable_and_never_deletes_user_source(app_pair, monkeypatch):
    from webapp import api
    app = app_pair; app.connect()
    app.link.manager.suspended = True
    source = app.root / 'user-source.bin'; source.write_bytes(b'abcd')
    transfer = app.link.scheduler.submit_upload(str(source))
    app.link.close(); app.new_client()
    monkeypatch.setattr(api, 'client_state', {'link': app.link})
    assert api.cancel_transfer(transfer.id)['ok']
    assert source.read_bytes() == b'abcd'
    app.new_client()
    assert app.link.manager.get(transfer.id).state == TransferState.CANCELLED


def test_staging_id_cannot_overwrite_a_retained_scratch_file(tmp_path, monkeypatch):
    from webapp import api
    from helpers.protocol import inline
    monkeypatch.setattr(api, '_UPLOAD_SCRATCH_DIR', str(tmp_path))
    monkeypatch.setattr(api, '_upload_sessions', {})
    monkeypatch.setattr(api, '_upload_transfers', {})
    monkeypatch.setattr(api, '_cancelled_uploads', {})
    scratch = tmp_path / 'retained.tmp'; scratch.write_bytes(b'keep')
    with pytest.raises(ValueError, match='already owns'):
        inline(api._stage_browser_chunk(None, 'retained', 'file', 4, 0, True, False))
    assert scratch.read_bytes() == b'keep'


def test_offline_cancel_reaches_remote_and_ack_is_durable(app_pair):
    app = app_pair; app.connect()
    source = app.root / 'cancel-later.bin'; source.write_bytes(os.urandom(65536))
    app.link.manager.suspended = True
    transfer = app.link.scheduler.submit_upload(str(source), chunk_size=4096)
    # Create a remote dormant record, as if CREATE was accepted before loss.
    remote = app.peer.manager.create_transfer(TransferDirection.DOWNLOAD,
        source.name, source.stat().st_size, transfer_id=transfer.id)
    app.drop(); app.link.close(); app.new_client()
    app.link.cancel_saved(transfer.id)
    assert app.link._store.load(transfer.id)['cancel_pending']
    app.connect()
    wait(lambda: remote.state == TransferState.CANCELLED)
    wait(lambda: not app.link._store.load(transfer.id)['cancel_pending'])
    app.link.close(); app.new_client()
    assert not app.link.manager.get(transfer.id).cancel_pending
    assert source.exists()


def test_bad_metadata_isolated_from_other_saved_transfers(tmp_path):
    store = PersistenceStore(str(tmp_path / 'state.sqlite3'))
    manager = TransferManager()
    broken = manager.create_transfer(TransferDirection.UPLOAD, 'broken', 4)
    healthy = manager.create_transfer(TransferDirection.UPLOAD, 'healthy', 4)
    store.save(broken); store.save(healthy)
    with sqlite3.connect(store.path()) as database:
        database.execute('UPDATE transfers SET metadata=? WHERE id=?', ('not-json', broken.id))
    restored = TransferManager()
    load_transfer_records(store, restored)
    assert restored.get(broken.id).state == TransferState.FAILED
    assert restored.get(healthy.id).state == TransferState.CONNECTION_LOST


def test_host_stop_rejects_pending_approval_and_late_attach(app_pair):
    from filetransfer.session_protocol import SessionError
    app = app_pair
    app.host._pending_listeners.clear()
    pending = app.host.register_pending(('waiting', 1))
    app.host.stop()
    assert pending.event.is_set() and pending.approved is False
    assert app.host.list_pending() == []
    with pytest.raises(SessionError, match='stopping'):
        app.host._install_peer(SimpleNamespace(recovery_id='late'), ('late', 1))


def test_host_refuses_rebind_over_live_control_worker(app_pair):
    from filetransfer.session_protocol import SessionError
    app = app_pair; app.connect(); app.drop()
    old = app.peer.control_thread
    class Unstopped:
        def join(self, timeout=None): pass
        def is_alive(self): return True
    app.peer.control_thread = Unstopped()
    try:
        with pytest.raises(SessionError, match='old control worker'):
            app.host._install_peer(SimpleNamespace(recovery_id=app.peer.peer_id), ('new', 1))
    finally:
        app.peer.control_thread = old


@pytest.mark.parametrize('published', [False, True])
def test_real_process_kill_preserves_checkpoint_and_publication_gap(tmp_path, published):
    """Kill a real journal writer; this is not a TCP process-kill test."""
    script = '''
import hashlib, os, sys
from pathlib import Path
from filetransfer.manager import TransferManager
from filetransfer.persistence import PersistenceStore, save_transfer
from filetransfer.transfer import TransferDirection
root = Path(sys.argv[1]); published = sys.argv[2] == 'True'
store = PersistenceStore(str(root / 'crash.sqlite3'))
manager = TransferManager()
t = manager.create_transfer(TransferDirection.DOWNLOAD, 'file.bin', 8, hashlib.sha256(b'abcdefgh').hexdigest(), transfer_id='a'*32)
t.part_path, t.dest_path = str(root / 'file.part'), str(root / 'file.bin')
with open(t.part_path, 'wb') as output:
    output.write(b'abcdefgh' if published else b'abcd'); output.flush(); os.fsync(output.fileno())
t.update_progress(8 if published else 4)
save_transfer(store, t)
if published:
    os.replace(t.part_path, t.dest_path)
else:
    with open(t.part_path, 'ab') as output:
        output.write(b'UNCONFIRMED'); output.flush(); os.fsync(output.fileno())
print('READY', flush=True)
sys.stdin.buffer.read(1)
'''
    process = subprocess.Popen([sys.executable, '-u', '-c', script, str(tmp_path), str(published)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    ready = queue.Queue()
    def read_ready():
        for line in process.stdout:
            if line.strip() == 'READY':
                ready.put(True)
                return
        ready.put(False)
    reader = threading.Thread(target=read_ready, daemon=True)
    reader.start()
    try:
        assert ready.get(timeout=10), 'journal writer exited before checkpoint'
        process.kill()
        process.communicate(timeout=5)
    finally:
        if process.poll() is None:
            process.kill(); process.communicate(timeout=5)
        reader.join(1)
    manager = TransferManager()
    load_transfer_records(PersistenceStore(str(tmp_path / 'crash.sqlite3')), manager)
    transfer = manager.get('a' * 32)
    if published:
        assert transfer.state == TransferState.COMPLETED
        assert (tmp_path / 'file.bin').read_bytes() == b'abcdefgh'
    else:
        assert transfer.state == TransferState.CONNECTION_LOST and transfer.offset == 4
        assert (tmp_path / 'file.part').read_bytes() == b'abcd'
