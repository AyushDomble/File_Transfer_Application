"""Shared ui mock test support."""
from helpers.browser import STATIC


def mock_app(page, *, connected=False, transfers=None):
    state = {'connected': connected, 'transfers': transfers or [], 'requests': []}
    def handle(route):
        request = route.request
        path = request.url.removeprefix('http://design.test')
        if path in ('/index.html', '/client.html', '/host.html', '/app.js', '/theme.js', '/style.css', '/chat.js', '/chat.css', '/rules.js', '/rules.css', '/refresh.js'):
            mime = 'text/html' if path.endswith('.html') else 'text/css' if path.endswith('.css') else 'application/javascript'
            route.fulfill(body=(STATIC / path[1:]).read_text(encoding='utf-8'), content_type=mime)
            return
        state['requests'].append((path, request.method))
        if path == '/api/client/status': result = {'connected': state['connected']}
        elif path == '/api/client/files': result = {'ok': True, 'files': ['Holiday photos.zip', 'Design notes.pdf']}
        elif path == '/api/transfers': result = {'transfers': state['transfers']}
        elif path == '/api/host/info': result = {'running': False, 'stopping': False}
        elif path == '/api/host/settings': result = {'max_upload_mb': 20, 'permission': 'deny', 'extensions': ['.exe'], 'free_bytes': 1073741824}
        else: result = {'ok': True, 'files': [], 'pending': [], 'peers': [], 'transfers': []}
        route.fulfill(json=result)
    page.route('http://design.test/**', handle)
    page.add_init_script('''window.WebSocket = class {
      constructor() { setTimeout(() => this.onopen?.(), 0); } close() {}
    };''')
    return state
