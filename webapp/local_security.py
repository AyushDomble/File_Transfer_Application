"""Local browser boundary: launch capability, HttpOnly cookie, Host/Origin checks.

Loopback is not authentication. The one-use launch URL is supplied only to the
desktop window, never printed, logged, advertised, or stored in a shared file.
"""
import secrets
from http.cookies import SimpleCookie, CookieError
from starlette.responses import JSONResponse, RedirectResponse
from filetransfer import config

BOOTSTRAP_TOKEN = secrets.token_urlsafe(32)


class LocalSecurity:
    def __init__(self, app):
        self.app = app
        self.cookie = secrets.token_urlsafe(32)
        self.bootstrap_available = True

    async def __call__(self, scope, receive, send):
        if scope['type'] not in ('http', 'websocket'):
            return await self.app(scope, receive, send)
        headers = {key.decode('latin1').lower(): value.decode('latin1') for key, value in scope['headers']}
        hosts = {f'127.0.0.1:{config.WEBAPP_PORT}', f'localhost:{config.WEBAPP_PORT}'}
        host = headers.get('host', '')
        origin = headers.get('origin')
        valid_origin = origin is None or origin == f'http://{host}'
        path = scope['path']
        valid_host = host in hosts
        if (scope['type'] == 'http' and valid_host and valid_origin
                and scope.get('method') == 'GET' and self.bootstrap_available
                and secrets.compare_digest(path.encode('utf-8'), ('/launch/' + BOOTSTRAP_TOKEN).encode())):
            self.bootstrap_available = False
            response = RedirectResponse('/', status_code=303)
            response.set_cookie('ft_session', self.cookie, httponly=True, samesite='strict', path='/')
            response.headers['Cache-Control'] = 'no-store'
            response.headers['Referrer-Policy'] = 'no-referrer'
            return await response(scope, receive, send)
        cookies = SimpleCookie()
        try:
            cookies.load(headers.get('cookie', ''))
            token = cookies['ft_session'].value if 'ft_session' in cookies else ''
        except CookieError:
            token = ''
        authenticated = secrets.compare_digest(token.encode('utf-8'), self.cookie.encode())
        public = not path.startswith(('/api', '/transfers')) and scope['type'] == 'http' and scope.get('method') in ('GET', 'HEAD') and (
            path == '/' or path.endswith(('.html', '.css', '.js', '.ico', '.svg', '.png')))
        sensitive = not public
        if sensitive and (scope['type'] == 'websocket' or scope.get('method') not in ('GET', 'HEAD')):
            valid_origin = origin == f'http://{host}'
        allowed = (valid_host and valid_origin and headers.get('sec-fetch-site') != 'cross-site'
                   and (public or authenticated))
        if not allowed:
            if scope['type'] == 'websocket':
                return await send({'type': 'websocket.close', 'code': 1008})
            return await JSONResponse({'ok': False, 'error': 'Open the desktop app to authorize this local session.'},
                                      status_code=403)(scope, receive, send)

        async def secure_send(message):
            if message['type'] == 'http.response.start':
                message['headers'] = list(message.get('headers', [])) + [
                    (b'cache-control', b'no-store'), (b'x-content-type-options', b'nosniff'),
                    (b'referrer-policy', b'no-referrer'), (b'x-frame-options', b'DENY'),
                    (b'content-security-policy', b"frame-ancestors 'none'; object-src 'none'; base-uri 'self'")]
            await send(message)
        await self.app(scope, receive, secure_send)
