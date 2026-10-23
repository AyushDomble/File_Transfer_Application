"""Bounded, no-window release checks against the real running local backend.

Run only in a disposable working directory with fresh role/state directories.
Never emits bootstrap tokens, cookies, PINs or certificate private material.
"""
import http.cookiejar
import json
from urllib.error import HTTPError
from urllib.request import build_opener, HTTPCookieProcessor, ProxyHandler, Request

from filetransfer import config
from .local_security import BOOTSTRAP_TOKEN


def check_running_app():
    base = f'http://127.0.0.1:{config.WEBAPP_PORT}'
    anonymous = build_opener(ProxyHandler({}))
    client = build_opener(ProxyHandler({}), HTTPCookieProcessor(http.cookiejar.CookieJar()))

    def request(opener, path, method='GET', origin=None):
        headers = {'Origin': origin or base}
        try:
            with opener.open(Request(base + path, method=method, headers=headers), timeout=10) as response:
                return response.status, response.read()
        except HTTPError as exc:
            return exc.code, exc.read()

    assert request(anonymous, '/api/host/info')[0] == 403
    assert request(client, '/launch/' + BOOTSTRAP_TOKEN)[0] == 200
    assert request(anonymous, '/launch/' + BOOTSTRAP_TOKEN)[0] == 403
    for path in ('/', '/host.html', '/client.html', '/app.js', '/style.css', '/chat.js', '/chat.css'):
        status, body = request(client, path)
        assert status == 200 and body, f'Missing packaged asset: {path}'
    assert request(client, '/api/host/start', 'POST', 'http://foreign.invalid')[0] == 403
    status, body = request(client, '/api/host/start', 'POST')
    info = json.loads(body)
    assert status == 200 and info['ok'] and info['running'], 'TLS host startup failed'
    fingerprint = info['certificate_fingerprint'].replace(':', '')
    assert len(fingerprint) == 64
    assert request(client, '/api/host/stop', 'POST')[0] == 200
    assert not json.loads(request(client, '/api/host/info')[1])['running']
    print(json.dumps({'smoke': 'passed', 'checks': ['HTTP startup', 'static assets',
          'session authorization', 'one-use bootstrap', 'origin rejection',
          'TLS host start/stop'], 'native_window': 'not exercised'}), flush=True)
