from helpers.rules_mock import (setup)
import os
from pathlib import Path
import pytest
from playwright.sync_api import expect
from helpers.browser import browser_page, STATIC
from helpers.ui_mock import mock_app




@pytest.mark.parametrize('width', [390,1280])
@pytest.mark.parametrize('theme', ['dark','light'])
def test_rules_layout_and_controls(browser_page, width, theme):
    page = browser_page; setup(page); errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.set_viewport_size({'width':width,'height':1000}); page.goto('http://design.test/host.html')
    page.get_by_label('Color theme').select_option(theme)
    page.locator('.settings-card > summary').click()
    page.locator('.extension-picker summary').click(); page.locator('#extensionSearch').fill('pdf')
    page.get_by_label('.pdf', exact=True).check(); expect(page.locator('#extensionChips')).to_contain_text('.pdf')
    page.get_by_role('button', name='Remove .exe', exact=True).click()
    page.get_by_label('Is allowed', exact=True).check()
    page.locator('#sizeNumber').fill('512'); expect(page.locator('#sizeSlider')).to_have_value('512')
    page.locator('#sizeUnit').select_option('1024'); expect(page.locator('#sizeNumber')).to_have_value('0.5')
    page.locator('#sizeSlider').fill('1024'); expect(page.locator('#sizeNumber')).to_have_value('1')
    expect(page.locator('#ruleSummary')).to_contain_text('Only allow: .pdf')
    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
    expect(page.locator('#fileList')).to_be_hidden()
    page.locator('.extension-picker summary').click()
    page.evaluate('document.activeElement.blur(); window.scrollTo(0, 0)')
    if os.environ.get('FT_UI_SCREENSHOTS'):
        folder = Path(os.environ['FT_UI_SCREENSHOTS']); folder.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(folder / f'rules-{theme}-{width}.png'), full_page=True)
    assert not errors


def test_discovery_pin_connection_and_hidden_panels(browser_page):
    page = browser_page; state = setup(page)
    page.route('**/api/client/discover**', lambda route: route.fulfill(json={'hosts':[{'name':'Studio laptop','ip':'192.168.1.22'}]}))
    page.goto('http://design.test/client.html')
    expect(page.locator('#queueCard')).to_be_hidden(); expect(page.locator('#pinStep')).to_be_hidden(); expect(page.locator('.chat-widget')).to_be_hidden()
    page.locator('#scanBtn').click()
    host = page.locator('#scanVisual .host-choice'); expect(host).to_contain_text('Studio laptop')
    host.click(); expect(page.locator('#pinStep')).to_be_visible(); expect(page.locator('#hostInput')).to_have_value('192.168.1.22')
    page.locator('#pinInput').fill('123456')
    def connect(route):
        state['connected'] = True; route.fulfill(json={'ok':True})
    page.route('**/api/client/connect', connect); page.locator('#connectBtn').click()
    expect(page.locator('#queueCard')).to_be_visible(); expect(page.locator('#connectCard')).to_be_hidden()
    expect(page.locator('.success-toast')).to_contain_text('Connected')
    expect(page.locator('#zipSelection')).to_be_visible(); expect(page.locator('#fileList')).to_be_hidden()
