"""Shared asgi test support."""
import asyncio
import json
import selectors
import time
import httpx
import pytest
from webapp.events import TransferEvents
from helpers.app_peers import AppPair


class PollSelector(selectors.SelectSelector):
    def select(self, timeout=None):
        time.sleep(min(0.002, timeout) if timeout is not None else 0.002)
        return []

class NoSocketLoop(asyncio.SelectorEventLoop):
    def __init__(self):
        super().__init__(selector=PollSelector())
    def _make_self_pipe(self): pass
    def _close_self_pipe(self): pass
    def _write_to_self(self): pass

def run(coro):
    loop = NoSocketLoop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(loop.shutdown_default_executor())
        loop.close()
        asyncio.set_event_loop(None)

@pytest.fixture
def app_api(tmp_path, monkeypatch):
    from webapp import api
    from filetransfer import config
    monkeypatch.setattr(config, 'STATE_DIR', None)
    app = AppPair(tmp_path, monkeypatch)
    monkeypatch.setattr(api, 'transfer_host', app.host)
    monkeypatch.setattr(api, 'client_state', {'link': app.link})
    monkeypatch.setattr(api, '_transfer_events', TransferEvents(capacity=16))
    monkeypatch.setattr(api, '_UPLOAD_SCRATCH_DIR', str(tmp_path / 'scratch'))
    monkeypatch.setattr(api, '_upload_sessions', {})
    monkeypatch.setattr(api, '_upload_transfers', {})
    monkeypatch.setattr(api, '_cancelled_uploads', {})
    api._bind_client_events(app.link)
    app.host.add_transfer_listener(api._on_host_transfer_change)
    app.host.add_progress_listener(api._on_host_progress)
    api.app.middleware_stack = None
    from webapp.local_security import BOOTSTRAP_TOKEN
    async def authorize():
        base = f'http://127.0.0.1:{config.WEBAPP_PORT}'
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api.app), base_url=base) as client:
            response = await client.get('/launch/' + BOOTSTRAP_TOKEN)
            assert response.status_code == 303
            api._test_auth_headers = {'Host': f'127.0.0.1:{config.WEBAPP_PORT}', 'Origin':base,
                                     'Cookie':'ft_session=' + response.cookies['ft_session']}
    run(authorize())
    yield api, app
    app.close()

async def request(api, method, path, **kwargs):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api.app), base_url='http://test', headers=api._test_auth_headers) as client:
        return await client.request(method, path, **kwargs)

async def open_ws(api, side=None):
    incoming, outgoing = asyncio.Queue(), asyncio.Queue()
    scope = dict(type='websocket', asgi={'version': '3.0'}, scheme='ws', path='/api/transfers/events',
                 raw_path=b'/api/transfers/events', query_string=(f'side={side}' if side else '').encode(),
                 headers=[(k.lower().encode(), v.encode()) for k,v in api._test_auth_headers.items()], client=('test', 1), server=('test', 80), subprotocols=[], root_path='')
    async def send(message): await outgoing.put(message)
    await incoming.put({'type': 'websocket.connect'})
    task = asyncio.create_task(api.app(scope, incoming.get, send))
    assert (await asyncio.wait_for(outgoing.get(), 3))['type'] == 'websocket.accept'
    async def receive():
        result = await asyncio.wait_for(outgoing.get(), 3)
        assert result['type'] == 'websocket.send', result
        return json.loads(result['text'])
    async def close():
        await incoming.put({'type': 'websocket.disconnect', 'code': 1000})
        await asyncio.wait_for(task, 3)
    return receive, close
