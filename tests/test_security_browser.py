from playwright.sync_api import expect
from filetransfer import config
from helpers.native_peers import native_pair, native_http
from helpers.browser import browser_page


def test_first_pairing_fingerprint_and_delete_permission_ui(native_http, browser_page, monkeypatch, tmp_path):
    base, api, host, link = native_http
    link.close()
    monkeypatch.setattr(api, 'client_state', {'link': None})
    monkeypatch.setattr(config, 'CLIENT_DIR', str(tmp_path / 'fresh-client'))
    page = browser_page
    page.context.add_cookies([api._test_browser_cookie])
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(base + '/host.html')
    expect(page.locator('#certificateFingerprint')).to_have_text(host.certificate_fingerprint)
    page.locator('.settings-card > summary').click()
    expect(page.locator('#allowRemoteDelete')).not_to_be_checked()
    page.locator('#allowRemoteDelete').check()
    page.locator('#sizeNumber').fill('12')  # Do not race changing free disk space at its exact maximum.
    page.locator('#saveRules').click()
    expect(page.locator('#rulesStatus')).to_contain_text('Rules saved')
    assert host.allow_remote_delete
    page.goto(base + '/client.html')
    page.locator('#manualConnect summary').click()
    page.locator('#manualAddress').fill('127.0.0.1')
    page.get_by_role('button', name='Use this host').click()
    page.locator('#pinInput').fill(host.current_pin)
    page.locator('#connectBtn').click()
    expect(page.locator('#connectStatus')).to_contain_text('First pairing')
    page.locator('#fingerprintInput').fill(host.certificate_fingerprint)
    page.locator('#connectBtn').click()
    expect(page.locator('#filesCard')).to_be_visible(timeout=15000)
    assert api.client_state['link'].connected and not errors
