"""Regression contracts for bounded preparation and low-risk hot-path changes."""
import asyncio
import builtins
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import threading
import time
from types import SimpleNamespace

from fastapi import FastAPI, HTTPException
import pytest

from filetransfer.persistence import PersistenceStore
from webapp.batch_api import install


def wait_for(predicate):
    deadline = time.monotonic() + 5
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(.01)


def test_store_reuses_full_durability_connection_and_rolls_back(tmp_path):
    store = PersistenceStore(str(tmp_path / 'journal.db'))
    with store._lock:
        first = store._connect()
        assert first is store._connect()
        assert first.execute('PRAGMA synchronous').fetchone()[0] == 2
        assert first.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
        with pytest.raises(sqlite3.IntegrityError):
            with store._transaction() as conn:
                conn.execute("INSERT INTO settings VALUES ('rollback', '1')")
                conn.execute("INSERT INTO settings VALUES ('rollback', '2')")
    assert store.get_setting('rollback') is None
    store.set_setting('valid', 42)
    assert store.get_setting('valid') == 42


def test_store_serializes_workers(tmp_path):
    store = PersistenceStore(str(tmp_path / 'journal.db'))
    def write(index):
        store.set_setting(str(index), index)
        assert store.get_setting(str(index)) == index
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(100)))
    assert all(store.get_setting(str(i)) == i for i in range(100))


@pytest.fixture
def batch_app(tmp_path):
    app = FastAPI()
    calls = []
    def batch(action, payload):
        calls.append(action)
        return {'status': 'pending' if action != 'cancel' else 'cancelled'}
    link = SimpleNamespace(session=SimpleNamespace(supports=lambda name: True), batch=batch)
    env = {'client_state': {'link': link}, '_link_or_error': lambda: (link, None),
           '_UPLOAD_SCRATCH_DIR': str(tmp_path / 'scratch')}
    install(app, env)
    routes = {route.name: route.endpoint for route in app.routes if hasattr(route, 'endpoint')}
    yield routes, link, calls, env
    env['_cleanup_batches']()


def stage_empty(routes):
    bid = routes['create']({'files': [{'filename': 'empty.txt', 'size': 0}]})['batch_id']
    class Request:
        async def stream(self):
            yield b''
    asyncio.run(routes['stage'](bid, 0, Request(), 0, True))
    return bid


def test_stage_disk_write_off_event_loop(batch_app, monkeypatch):
    routes, *_ = batch_app
    main_thread = threading.get_ident()
    writers = []
    original = builtins.open
    def spy(path, mode='r', *args, **kwargs):
        if mode in ('wb', 'ab'):
            writers.append(threading.get_ident())
        return original(path, mode, *args, **kwargs)
    monkeypatch.setattr(builtins, 'open', spy)
    stage_empty(routes)
    assert writers and all(tid != main_thread for tid in writers)


def test_cancel_during_preparation_rpc_is_prompt_and_revokes(batch_app):
    routes, link, calls, env = batch_app
    bid = stage_empty(routes)
    entered, release = threading.Event(), threading.Event()
    original = link.batch
    def blocked(action, payload):
        if action == 'request':
            entered.set()
            assert release.wait(5)
        return original(action, payload)
    link.batch = blocked
    try:
        assert routes['request_approval'](bid)['status'] == 'preparing'
        assert entered.wait(3)
        assert routes['list_groups']()['batches'][0]['status'] == 'preparing'
        assert routes['cancel'](bid) == {'ok': True}
        assert routes['status'](bid)['status'] == 'cancelled'
    finally:
        release.set()
    wait_for(lambda: 'cancel' in calls)
    from pathlib import Path
    wait_for(lambda: not list(Path(env['_UPLOAD_SCRATCH_DIR']).iterdir()))


def test_preparation_admission_is_bounded(batch_app):
    routes, link, calls, _ = batch_app
    ids = [stage_empty(routes) for _ in range(3)]
    release = threading.Event()
    original = link.batch
    def blocked(action, payload):
        if action == 'request':
            assert release.wait(5)
        return original(action, payload)
    link.batch = blocked
    try:
        for bid in ids[:2]:
            assert routes['request_approval'](bid)['status'] == 'preparing'
        with pytest.raises(HTTPException) as exc:
            routes['request_approval'](ids[2])
        assert exc.value.status_code == 429
        assert routes['status'](ids[2])['status'] == 'staging'
    finally:
        release.set()
    wait_for(lambda: all(routes['status'](bid)['status'] == 'pending' for bid in ids[:2]))


def test_preparation_failure_releases_admission_and_cleans(batch_app):
    routes, link, _, env = batch_app
    def broken(*args):
        raise ValueError('simulated host error')
    link.batch = broken
    from pathlib import Path
    for _ in range(3):
        bid = stage_empty(routes)
        routes['request_approval'](bid)
        wait_for(lambda: routes['status'](bid)['status'] == 'failed')
        wait_for(lambda: not list(Path(env['_UPLOAD_SCRATCH_DIR']).iterdir()))


def test_progress_sampling_keeps_first_and_final_byte_update(monkeypatch):
    from webapp import api
    from filetransfer.manager import TransferManager
    from filetransfer.transfer import TransferDirection
    transfer = TransferManager().create_transfer(TransferDirection.UPLOAD, 'progress', 100)
    records = []
    clock = [100.0]
    monkeypatch.setattr(api, 'time', SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(api, '_transfer_events', SimpleNamespace(publish=records.append))
    for now, offset in [(100.0, 10), (100.1, 20), (100.15, 100)]:
        clock[0] = now
        transfer.update_progress(offset)
        api._on_progress('client', None, transfer)
    assert [r['offset'] for r in records] == [10, 100]
