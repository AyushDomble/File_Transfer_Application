"""Optional real-browser queue checks; mock HTTP is NOT transfer/TCP coverage.

Launch happens only in a fixture, never while collecting tests. Run on an
unrestricted machine with Playwright Chromium installed.
"""
from helpers.browser import (STATIC, browser_page)
import json
from pathlib import Path

import pytest





def test_real_dom_queue_action_errors_stale_progress_and_safe_filename(browser_page):
    page = browser_page
    page.set_content('<div id="queue"></div>')
    page.add_script_tag(path=str(STATIC / 'app.js'))
    page.evaluate('''() => {
      window.queue = createTransferQueueView(document.querySelector('#queue'), {
        autoRemoveDelay: null, onPause: async () => ({ok:false,error:'rejected'})
      });
      queue.applyEvent({id:'one',filename:'<img src=x onerror=alert(1)>',state:'active',
        direction:'upload',offset:2,total_size:4,revision:1});
    }''')
    assert page.locator('#queue img').count() == 0
    page.get_by_role('button', name='Pause', exact=True).click()
    page.get_by_role('alert').filter(has_text='rejected').wait_for()
    assert page.get_by_role('button', name='Pause', exact=True).is_enabled()
    page.evaluate('''() => {
      queue.applyEvent({id:'one',state:'completed',revision:3});
      queue.applyEvent({id:'one',state:'active',revision:2,type:'TRANSFER_PROGRESS'});
    }''')
    assert page.locator('#queue .state').inner_text() == 'Done'
    assert page.locator('#queue .pct').inner_text() == '100%'


def test_real_browser_client_download_uses_resource_and_controls(browser_page):
    page = browser_page
    requests = []
    created_flag = [False]
    transfer = dict(id='browser-job', filename="quoted'name.bin", direction='download',
                    total_size=4, offset=0, revision=1, state='queued')
    def route_request(route):
        request = route.request
        path = request.url.removeprefix('http://queue.test')
        if path in ('/client.html', '/app.js', '/theme.js', '/style.css', '/chat.js', '/chat.css', '/rules.js', '/rules.css', '/refresh.js'):
            content_type = 'text/html' if path.endswith('.html') else 'text/css' if path.endswith('.css') else 'application/javascript'
            route.fulfill(body=(STATIC / path[1:]).read_text(encoding='utf-8'), content_type=content_type)
            return
        requests.append((path, request.method, request.post_data))
        if path == '/api/client/status': result = dict(ok=True, connected=True)
        elif path == '/api/client/files': result = dict(ok=True, files=[transfer['filename']])
        elif path == '/api/transfers' and request.method == 'POST':
            created_flag[0] = True
            result = dict(ok=True, transfer=transfer)
        elif path == '/api/transfers': result = dict(ok=True, transfers=[transfer] if created_flag[0] else [])
        elif path.endswith('/cancel'):
            transfer.update(state='cancelled', revision=2)
            result = dict(ok=True, transfer=transfer)
        else: result = dict(ok=True, transfers=[])
        route.fulfill(json=result)
    page.route('http://queue.test/**', route_request)
    page.add_init_script('window.WebSocket = class {close(){} };')
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto('http://queue.test/client.html')
    page.get_by_text('Browse files on host', exact=True).click()
    page.locator('#fileList').get_by_role('button', name='Download', exact=True).click()
    page.locator('#transferQueue').get_by_role('button', name='Cancel', exact=True).click()
    page.wait_for_function("document.querySelectorAll('#transferQueue .transfer-row').length === 0")
    assert transfer['state'] == 'cancelled'
    assert any(path.endswith('/cancel') and method == 'POST' for path, method, _ in requests)
    created = [json.loads(body) for path, method, body in requests if path == '/api/transfers' and method == 'POST']
    assert created and created[0]['direction'] == 'download'
    assert created[0]['filename'] == "quoted'name.bin"
    assert not any(path.startswith('/api/client/download/') for path, _, _ in requests)
    assert not errors


def test_real_browser_host_reports_shutdown_conflict_without_false_running_state(browser_page):
    page = browser_page
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    def route_request(route):
        path = route.request.url.removeprefix('http://host.test')
        if path in ('/host.html', '/app.js', '/theme.js', '/style.css', '/chat.js', '/chat.css', '/rules.js', '/rules.css', '/refresh.js'):
            content_type = 'text/html' if path.endswith('.html') else 'text/css' if path.endswith('.css') else 'application/javascript'
            route.fulfill(body=(STATIC / path[1:]).read_text(encoding='utf-8'), content_type=content_type)
        elif path == '/api/host/start':
            route.fulfill(status=409, json=dict(ok=False, error='host workers are still stopping', stopping=True))
        elif path == '/api/host/info':
            route.fulfill(json=dict(running=False, stopping=True))
        else:
            route.fulfill(json=dict(ok=True, transfers=[], pending=[], peers=[], files=[]))
    page.route('http://host.test/**', route_request)
    page.add_init_script('window.WebSocket = class {close(){} };')
    page.goto('http://host.test/host.html')
    page.get_by_role('button', name='Start Server', exact=True).click()
    page.locator('#serverError').filter(has_text='still stopping').wait_for()
    assert page.locator('#serverInfo').inner_text() == 'Finishing previous transfer workers…'
    assert page.locator('#pinDisplay').is_hidden()
    assert page.get_by_role('button', name='Start Server', exact=True).is_enabled()
    assert page.get_by_role('button', name='Stop Server', exact=True).is_enabled()
    assert not errors
