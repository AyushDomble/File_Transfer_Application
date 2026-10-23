"""Real local launch authentication for adapter endpoint contract tests."""
from contextlib import contextmanager
import asyncio
from fastapi.testclient import TestClient
from filetransfer import config
from webapp.local_security import BOOTSTRAP_TOKEN


@contextmanager
def authenticated_client(api):
    base = f'http://127.0.0.1:{config.WEBAPP_PORT}'
    with TestClient(api.app, base_url=base, headers={'Origin': base},
                    backend_options={'loop_factory': asyncio.SelectorEventLoop}) as client:
        response = client.get('/launch/' + BOOTSTRAP_TOKEN, follow_redirects=False)
        assert response.status_code == 303
        websocket_connect = client.websocket_connect
        def authenticated_websocket(path, **kwargs):
            return websocket_connect(base.replace('http:', 'ws:') + path,
                                     headers={'Origin': base}, **kwargs)
        client.websocket_connect = authenticated_websocket
        yield client
