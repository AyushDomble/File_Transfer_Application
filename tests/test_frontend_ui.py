"""Real Chromium UI coverage with mocked HTTP; native transfer tests remain separate."""
from helpers.ui_mock import (mock_app)
import os
from pathlib import Path

import pytest
pytest.importorskip('playwright.sync_api')
from playwright.sync_api import expect
from helpers.browser import browser_page, STATIC




def save_preview(page, name):
    folder = os.environ.get('FT_UI_SCREENSHOTS')
    if folder:
        Path(folder).mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(Path(folder) / (name + '.png')), full_page=True, animations='disabled')


@pytest.mark.parametrize('width', [390, 1280])
@pytest.mark.parametrize('theme', ['light', 'dark'])
@pytest.mark.parametrize('name', ['index', 'client', 'host'])
def test_theme_layout_and_no_browser_errors(browser_page, width, theme, name):
    page = browser_page
    mock_app(page)
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.set_viewport_size({'width': width, 'height': 900})
    page.goto(f'http://design.test/{name}.html')
    page.get_by_label('Color theme').select_option(theme)
    expect(page.locator('html')).to_have_attribute('data-theme', theme)
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    assert page.locator('h1').count() == 1
    page.reload()
    expect(page.locator('html')).to_have_attribute('data-theme', theme)
    expect(page.get_by_label('Color theme')).to_have_value(theme)
    save_preview(page, f'{name}-{theme}-{width}')
    assert not errors


def test_system_theme_changes_and_storage_unavailable(browser_page):
    page = browser_page
    mock_app(page)
    page.add_init_script("Storage.prototype.getItem = Storage.prototype.setItem = () => { throw new Error('denied'); };")
    page.emulate_media(color_scheme='dark')
    page.goto('http://design.test/index.html')
    expect(page.locator('html')).to_have_attribute('data-theme', 'dark')
    page.emulate_media(color_scheme='light')
    expect(page.locator('html')).to_have_attribute('data-theme', 'light')
    page.get_by_label('Color theme').select_option('dark')
    expect(page.locator('html')).to_have_attribute('data-theme', 'dark')


def test_scan_animation_keyboard_selection_and_failure_recovery(browser_page):
    page = browser_page
    mock_app(page)
    pending = []
    page.route('**/api/client/discover', lambda route: pending.append(route))
    page.goto('http://design.test/client.html')
    page.get_by_role('button', name='Find nearby hosts').click()
    expect(page.locator('#scanVisual')).to_have_class('scan-visual is-scanning')
    expect(page.locator('#scanBtn')).to_be_disabled()
    page.wait_for_function("document.querySelector('#scanStatus').textContent.includes('Scanning')")
    # Let Playwright dispatch the request; keep it pending to inspect actual busy state.
    for _ in range(20):
        if pending: break
        page.wait_for_timeout(25)
    assert pending
    save_preview(page, 'client-scanning')
    pending.pop().fulfill(json={'hosts': [{'ip': '192.168.1.9', 'name': 'Studio laptop'}]})
    choice = page.get_by_role('button', name='Studio laptop · 192.168.1.9')
    choice.focus()
    page.keyboard.press('Enter')
    expect(page.locator('#hostInput')).to_have_value('192.168.1.9')
    expect(page.locator('#pinInput')).to_be_focused()
    expect(page.locator('#scanVisual')).to_have_class('scan-visual')
    page.locator('#scanBtn').click()
    for _ in range(20):
        if pending: break
        page.wait_for_timeout(25)
    pending.pop().abort()
    expect(page.locator('#scanBtn')).to_be_enabled()
    expect(page.locator('#scanVisual')).to_have_class('scan-visual')
    expect(page.locator('#scanStatus')).not_to_be_empty()


def test_connect_submit_failure_unlocks_and_prevents_duplicate_requests(browser_page):
    page = browser_page
    mock_app(page)
    pending = []
    page.route('**/api/client/connect', lambda route: pending.append(route))
    page.goto('http://design.test/client.html')
    page.locator('#manualConnect > summary').click()
    page.locator('#manualAddress').fill('192.168.1.9')
    page.get_by_role('button', name='Use this host').click()
    page.locator('#fingerprintInput').fill('a' * 64)
    page.get_by_label('6-digit PIN from the host').fill('123456')
    page.keyboard.press('Enter')
    expect(page.locator('#connectBtn')).to_be_disabled()
    page.evaluate('connectToHost()')
    for _ in range(20):
        if pending: break
        page.wait_for_timeout(25)
    assert len(pending) == 1
    pending.pop().abort()
    expect(page.locator('#connectBtn')).to_be_enabled()
    expect(page.locator('#connectBtn')).not_to_have_class('full-width busy')
    expect(page.locator('#connectStatus')).not_to_be_empty()


def test_connected_queue_animation_search_and_reduced_motion(browser_page):
    page = browser_page
    transfers = [
        dict(id='active', filename='Holiday photos.zip', direction='upload', state='active', offset=42, total_size=100, bytes_per_sec=8388608, revision=1),
        dict(id='paused', filename='Design notes.pdf', direction='download', state='paused', offset=18, total_size=100, revision=1),
        dict(id='finished', filename='Welcome guide.pdf', direction='download', state='completed', offset=100, total_size=100, revision=1),
        dict(id='long', filename='very-long-file-name-' * 12 + '.bin', direction='upload', state='queued', offset=0, total_size=100, revision=1),
    ]
    mock_app(page, connected=True, transfers=transfers)
    page.set_viewport_size({'width': 1280, 'height': 900})
    page.goto('http://design.test/client.html')
    expect(page.locator('#connectionLabel')).to_have_text('Connected to host')
    expect(page.locator('.queue-empty')).to_be_hidden()
    expect(page.locator('.transfer-row')).to_have_count(4)
    active = page.locator('[data-state="active"] .transfer-bar')
    paused = page.locator('[data-state="paused"] .transfer-bar')
    assert active.evaluate("el => getComputedStyle(el, '::after').animationName") == 'flow'
    assert paused.evaluate("el => getComputedStyle(el, '::after').animationName") == 'none'
    expect(page.locator('[data-state="completed"] .pct')).to_have_text('100%')
    for theme in ('light', 'dark'):
        page.get_by_label('Color theme').select_option(theme)
        save_preview(page, f'transfers-{theme}')
    page.get_by_text('Browse files on host', exact=True).click()
    page.get_by_label('Find a shared file').fill('notes')
    expect(page.locator('#fileList li:visible')).to_have_count(1)
    page.get_by_label('Find a shared file').fill('missing')
    expect(page.locator('#fileList .search-empty')).to_be_visible()
    page.get_by_label('Find a shared file').fill('')
    expect(page.locator('#fileList li:visible')).to_have_count(2)
    for width in (320, 390, 768):
        page.set_viewport_size({'width': width, 'height': 844})
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    save_preview(page, 'transfers-tablet')
    page.emulate_media(reduced_motion='reduce')
    assert active.evaluate("el => getComputedStyle(el, '::after').animationName") == 'none'
    assert active.evaluate('el => getComputedStyle(el).transitionDuration') == '0s'


def test_dropzone_keyboard_opens_multi_file_picker(browser_page):
    page = browser_page
    mock_app(page, connected=True)
    page.goto('http://design.test/client.html')
    page.locator('#dropzone').focus()
    with page.expect_file_chooser() as chosen:
        page.keyboard.press('Enter')
    assert chosen.value.is_multiple()


def test_host_settings_failures_are_visible_and_unlock_controls(browser_page):
    page = browser_page
    mock_app(page)
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto('http://design.test/host.html')
    page.locator('.settings-card > summary').click()
    page.locator('#sizeNumber').fill('-1')
    page.locator('#saveRules').click()
    assert not page.locator('#sizeNumber').evaluate('el => el.checkValidity()')
    page.locator('#sizeNumber').fill('20')
    page.route('**/api/host/settings', lambda route: route.fulfill(status=503, json={'error': 'Service unavailable'}))
    page.locator('#saveRules').click()
    expect(page.locator('#rulesStatus')).to_have_text('Service unavailable')
    expect(page.locator('#saveRules')).to_be_enabled()
    assert not errors


def test_host_startup_network_failure_is_reported(browser_page):
    page = browser_page
    mock_app(page)
    page.route('**/api/host/info', lambda route: route.abort())
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto('http://design.test/host.html')
    expect(page.locator('#serverError')).not_to_be_empty()
    expect(page.locator('#startBtn')).to_be_enabled()
    assert not errors


def test_bulk_failure_restores_controls_and_delete_requires_confirmation(browser_page):
    page = browser_page
    state = mock_app(page, connected=True, transfers=[dict(id='one', filename='file.bin', direction='upload', state='paused', offset=10, total_size=20, revision=1)])
    page.route('**/api/transfers/actions/pause_all', lambda route: route.fulfill(status=503, json={'error': 'Try again shortly'}))
    page.goto('http://design.test/client.html')
    page.get_by_role('button', name='Pause all', exact=True).click()
    expect(page.locator('#queueStatus')).to_have_text('Try again shortly')
    expect(page.get_by_role('button', name='Resume all', exact=True)).to_be_enabled()
    page.once('dialog', lambda dialog: dialog.dismiss())
    page.get_by_text('Browse files on host', exact=True).click()
    page.locator('#fileList button.danger').first.click()
    assert not any(method == 'DELETE' for _, method in state['requests'])
