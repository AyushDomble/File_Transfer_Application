"""Shared rules mock test support."""
from helpers.browser import STATIC
from helpers.ui_mock import mock_app


def setup(page):
    state = mock_app(page)
    for name in ('rules.js', 'rules.css'):
        def asset(name):
            def serve(route):
                route.fulfill(body=(STATIC / name).read_text(encoding='utf-8'), content_type='text/css' if name.endswith('css') else 'application/javascript')
            return serve
        page.route('**/' + name, asset(name))
    page.route('**/api/host/settings', lambda route: route.fulfill(json={'permission':'deny','extensions':['.exe'], 'max_upload_mb':100, 'free_bytes':10737418240}))
    page.route('**/api/host/batches', lambda route: route.fulfill(json={'batches':[]}))
    page.route('**/api/client/batches', lambda route: route.fulfill(json={'batches':[]}))
    return state
