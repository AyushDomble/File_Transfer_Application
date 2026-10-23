from playwright.sync_api import expect
import pytest
from helpers.browser import browser_page, STATIC
from helpers.rules_mock import setup


@pytest.mark.parametrize('side', ['host', 'client'])
def test_refresh_is_non_disruptive(browser_page, side):
    page = browser_page
    state = setup(page)
    state['connected'] = True
    page.route('**/refresh.js', lambda route: route.fulfill(body=(STATIC / 'refresh.js').read_text(encoding='utf-8'), content_type='application/javascript'))
    page.goto(f'http://design.test/{side}.html')
    page.evaluate('window.refreshTestMarker = 42')
    if side == 'host':
        page.locator('.settings-card > summary').click()
        page.locator('#sizeNumber').fill('123')
    else:
        expect(page.locator('#filesCard')).to_be_visible()
        page.locator('#zipSelection').check()
    state['requests'].clear()
    page.locator('#refreshPage').click()
    expect(page.locator('#refreshStatus')).to_have_text('Refreshed just now')
    expect(page.locator('#refreshPage')).to_be_enabled()
    assert page.evaluate('window.refreshTestMarker') == 42
    assert all(method == 'GET' for _, method in state['requests'])
    assert any(path == ('/api/host/transfers' if side == 'host' else '/api/transfers') for path, _ in state['requests'])
    if side == 'host': expect(page.locator('#sizeNumber')).to_have_value('123')
    else: expect(page.locator('#zipSelection')).to_be_checked()


def test_disconnected_refresh_searches_hosts(browser_page):
    page = browser_page; setup(page)
    page.route('**/refresh.js', lambda route: route.fulfill(body=(STATIC / 'refresh.js').read_text(encoding='utf-8'), content_type='application/javascript'))
    page.route('**/api/client/discover', lambda route: route.fulfill(json={'hosts':[{'ip':'192.168.1.7','name':'Nearby laptop'}]}))
    page.goto('http://design.test/client.html'); page.locator('#refreshPage').click()
    expect(page.locator('#scanVisual .host-choice')).to_contain_text('Nearby laptop')
    expect(page.locator('#queueCard')).to_be_hidden()
    expect(page.locator('#refreshPage')).to_be_enabled()
