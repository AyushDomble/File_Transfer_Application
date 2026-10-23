"""Floating chat UI: mock HTTP layout cases plus real TCP/HTTP browser exchange."""
import json
import os
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
pytest.importorskip('playwright.sync_api')
from playwright.sync_api import expect
from helpers.browser import browser_page
from helpers.ui_mock import mock_app
from helpers.native_peers import native_http, native_pair


def chat_mock(page, *, side='host', messages=None):
    mock_app(page, connected=True)
    if side == 'host':
        page.route('**/api/host/info', lambda route: route.fulfill(json={'running':True, 'ip':'192.168.1.9', 'port':5000, 'pin':'482916'}))
        page.route('**/api/host/peers', lambda route: route.fulfill(json={'peers':[{'peer_id':'p1', 'ip':'192.168.1.12', 'connected':True}]}))
    state = {'messages': messages or [], 'incoming':len(messages or []), 'revision':1,
             'sent':[], 'available':True, 'fail':False, 'second':False}
    def handle(route):
        if route.request.method == 'POST':
            body = json.loads(route.request.post_data)
            state['sent'].append(body)
            if state['fail']:
                route.fulfill(status=409, json={'ok':False, 'error':'Delivery not confirmed. Please retry.'})
                return
            state['messages'].append(dict(id=body['message_id'], direction='outgoing', text=body['text'], status='delivered', timestamp='2026-10-18T12:00:00+00:00'))
            state['revision'] += 1
            route.fulfill(json={'ok':True})
            return
        query = parse_qs(urlparse(route.request.url).query)
        peer = query.get('peer_id', ['p1'])[0]
        info = dict(peer_id=peer, channel_id='channel-' + peer, incoming=state['incoming'] if peer == 'p1' else 0,
                    revision=state['revision'], available=state['available'], supported=True, connected=state['available'])
        peers = [dict(info, peer_id='p1', channel_id='channel-p1', label='Client · Studio laptop', incoming=state['incoming'])]
        if state['second']:
            peers.append(dict(info, peer_id='p2', channel_id='channel-p2', label='Client · Office desktop', incoming=0))
        same = query.get('revision', ['-1'])[0] == str(state['revision']) and query.get('channel_id', [''])[0] == info['channel_id']
        route.fulfill(json={'ok':True, 'peers':peers, 'conversation':info, 'messages':None if same else state['messages'] if peer == 'p1' else []})
    page.route('**/api/chat**', handle)
    page.goto(f'http://design.test/{side}.html')
    return state


def incoming(text, number=1):
    return dict(id=str(number), direction='incoming', text=text, status='received', timestamp='2026-10-18T12:00:00+00:00')


def test_chat_drag_keyboard_position_persistence_and_mobile_bounds(browser_page):
    page = browser_page
    page.set_viewport_size({'width':1280, 'height':900})
    chat_mock(page)
    page.locator('.chat-launcher').click()
    grip = page.locator('.chat-grip')
    box = grip.bounding_box()
    panel = page.locator('.chat-panel')
    before = panel.bounding_box()
    page.mouse.move(box['x'] + 30, box['y'] + 15)
    page.mouse.down()
    page.mouse.move(box['x'] - 180, box['y'] - 80, steps=8)
    page.mouse.up()
    moved = panel.bounding_box()
    assert moved['x'] < before['x'] - 150
    grip.focus()
    page.keyboard.press('ArrowLeft')
    moved = panel.bounding_box()
    page.reload()
    page.locator('.chat-launcher').click()
    assert abs(panel.bounding_box()['x'] - moved['x']) < 2
    for width, height in ((390, 844), (320, 480), (850, 400)):
        page.set_viewport_size({'width':width, 'height':height})
        # Resize delivery is asynchronous. Checking only the right edge can
        # succeed before the handler has clamped the panel vertically.
        page.wait_for_function('''() => {
          const r = document.querySelector('.chat-panel').getBoundingClientRect();
          return r.left >= 0 && r.top >= 0 && r.right <= innerWidth && r.bottom <= innerHeight;
        }''')
        box = panel.bounding_box()
        assert box['x'] >= 0 and box['y'] >= 0
        assert box['x'] + box['width'] <= width
        assert box['y'] + box['height'] <= height
    page.keyboard.press('Escape')
    expect(panel).to_be_hidden()
    expect(page.locator('.chat-launcher')).to_be_focused()


def test_unread_safe_text_selection_and_drafts(browser_page):
    page = browser_page
    state = chat_mock(page, messages=[incoming('<img src=x onerror=alert(1)>')])
    state['second'] = True
    expect(page.locator('.chat-badge')).to_have_text('1')
    page.locator('.chat-launcher').click()
    expect(page.locator('.chat-message-text')).to_have_text('<img src=x onerror=alert(1)>')
    assert page.locator('.chat-messages img').count() == 0
    expect(page.locator('.chat-badge')).to_be_hidden()
    page.locator('#chatText').fill('A draft for Studio')
    expect(page.locator('#chatPeer option')).to_have_count(2)
    page.locator('#chatPeer').select_option('p2')
    expect(page.locator('.chat-empty')).to_be_visible()
    expect(page.locator('#chatText')).to_have_value('')
    page.locator('#chatText').fill('A draft for Office')
    page.locator('#chatPeer').select_option('p1')
    expect(page.locator('#chatText')).to_have_value('A draft for Studio')
    state['messages'].append(incoming('A new note', 2)); state['incoming'] += 1; state['revision'] += 1
    page.locator('.chat-close').click()
    expect(page.locator('.chat-badge')).to_have_text('1')


def test_failed_send_keeps_draft_and_reuses_id_then_clears_after_delivery(browser_page):
    page = browser_page
    state = chat_mock(page, side='client')
    page.locator('.chat-launcher').click()
    state['fail'] = True
    page.locator('#chatText').fill('Can you see this?')
    page.locator('#chatText').press('Enter')
    expect(page.locator('.chat-error')).to_contain_text('not confirmed')
    expect(page.locator('#chatText')).to_have_value('Can you see this?')
    state['fail'] = False
    page.locator('.chat-send').click()
    expect(page.locator('#chatText')).to_have_value('')
    expect(page.locator('.chat-message-meta')).to_contain_text('Delivered')
    assert len(state['sent']) == 2 and state['sent'][0]['message_id'] == state['sent'][1]['message_id']
    page.locator('#chatText').fill('First line')
    page.locator('#chatText').press('Shift+Enter')
    page.locator('#chatText').type('Second line')
    assert len(state['sent']) == 2
    expect(page.locator('#chatText')).to_have_value('First line\nSecond line')
    state['available'] = False
    expect(page.locator('#chatText')).to_be_disabled()
    expect(page.locator('.chat-status')).to_contain_text('Disconnected')


def test_scrolled_chat_does_not_mark_new_messages_read_or_jump(browser_page):
    page = browser_page
    state = chat_mock(page, messages=[incoming('Message ' + str(i) + '\nMore detail', i) for i in range(15)])
    page.locator('.chat-launcher').click()
    expect(page.locator('.chat-badge')).to_be_hidden()
    page.locator('.chat-messages').evaluate('el => el.scrollTop = 0')
    state['messages'].append(incoming('Newest message', 20)); state['incoming'] += 1; state['revision'] += 1
    expect(page.locator('.chat-badge')).to_have_text('1')
    expect(page.locator('.chat-latest')).to_be_visible()
    assert page.locator('.chat-messages').evaluate('el => el.scrollTop') == 0
    page.locator('.chat-latest').click()
    expect(page.locator('.chat-badge')).to_be_hidden()


@pytest.mark.parametrize('theme', ['light', 'dark'])
def test_chat_theme_reduced_motion_and_preview(browser_page, theme):
    page = browser_page
    page.set_viewport_size({'width':1280, 'height':900})
    chat_mock(page, messages=[incoming('The photos are on their way 👋')])
    page.get_by_label('Color theme').select_option(theme)
    page.locator('.chat-launcher').click()
    page.locator('#chatText').fill('Perfect, thanks! I’ll send the notes next.')
    page.locator('.chat-send').click()
    expect(page.locator('.chat-message')).to_have_count(2)
    page.emulate_media(reduced_motion='reduce')
    assert page.locator('.chat-panel').evaluate('el => getComputedStyle(el).animationName') == 'none'
    folder = os.environ.get('FT_CHAT_SCREENSHOTS')
    if folder:
        Path(folder).mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(Path(folder) / f'chat-{theme}.png'), full_page=True, animations='disabled')


def test_real_browser_host_client_chat_and_unread(native_http, browser_page):
    base, api, host, link = native_http
    client = browser_page
    client.context.add_cookies([api._test_browser_cookie])
    host_page = client.context.new_page()
    errors = []
    for page in (client, host_page):
        page.on('pageerror', lambda error: errors.append(str(error)))
    host_page.goto(base + '/host.html')
    client.goto(base + '/client.html')
    client.bring_to_front()
    client.locator('.chat-launcher').click()
    client.locator('#chatText').fill('Hello from the client 👋')
    client.locator('#chatText').press('Enter')
    expect(client.locator('.chat-message-meta')).to_contain_text('Delivered')
    expect(host_page.locator('.chat-badge')).to_have_text('1')
    host_page.bring_to_front()
    host_page.locator('.chat-launcher').click()
    expect(host_page.locator('.chat-message-text')).to_have_text('Hello from the client 👋')
    host_page.locator('#chatText').fill('Hello back from your host!')
    host_page.locator('.chat-send').click()
    expect(host_page.locator('.chat-message.is-mine .chat-message-meta')).to_contain_text('Delivered')
    expect(client.locator('.chat-message.is-theirs .chat-message-text')).to_have_text('Hello back from your host!')
    client.reload()
    client.locator('.chat-launcher').click()
    expect(client.locator('.chat-message')).to_have_count(2)
    assert link.chat.info()['incoming'] == 1
    assert not errors
    host_page.close()
