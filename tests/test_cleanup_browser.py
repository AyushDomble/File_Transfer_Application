import pytest
from playwright.sync_api import expect
from helpers.browser import browser_page, STATIC
from helpers.rules_mock import setup


def prepare(page, side='client'):
    state = setup(page)
    state['connected'] = True
    page.route('**/refresh.js', lambda route: route.fulfill(body=(STATIC / 'refresh.js').read_text(encoding='utf-8'), content_type='application/javascript'))
    page.goto(f'http://design.test/{side}.html')
    return state


@pytest.mark.parametrize('side', ['client', 'host'])
def test_cancelled_history_clears_but_live_rows_remain(browser_page, side):
    page = browser_page; prepare(page, side)
    page.evaluate("""() => {
      transferQueue.applyEvent({id:'cancel-me',peer_id:'p',state:'active',revision:1,filename:'old.txt'});
      transferQueue.applyEvent({id:'keep-me',peer_id:'p',state:'paused',revision:1,filename:'keep.txt'});
      transferQueue.applyEvent({id:'cancel-me',peer_id:'p',state:'cancelled',revision:2,filename:'old.txt'});
    }""")
    expect(page.locator('#transferQueue')).not_to_contain_text('old.txt')
    expect(page.locator('#transferQueue')).to_contain_text('keep.txt')
    page.evaluate("""() => transferQueue.syncSnapshot({transfers:[
      {id:'cancel-me',peer_id:'p',state:'cancelled',revision:2,filename:'old.txt'},
      {id:'keep-me',peer_id:'p',state:'paused',revision:1,filename:'keep.txt'}]})""")
    expect(page.locator('#transferQueue .transfer-row')).to_have_count(1)


@pytest.mark.parametrize('restored', [False, True])
def test_rejection_ok_removes_notice_and_card(browser_page, restored):
    page = browser_page; state = setup(page); state['connected'] = True
    def groups(route):
        if route.request.method == 'POST': route.fulfill(json={'batch_id':'test','status':'staging'})
        else: route.fulfill(json={'batches':[{'batch_id':'test','count':1,'status':'pending'}] if restored else []})
    page.route('**/api/client/batches', groups)
    page.route('**/api/client/batches/test/**', lambda route: route.fulfill(json={'ok':True,'status':'pending'}))
    page.route('**/api/client/batches/test', lambda route: route.fulfill(json={'status':'rejected'}))
    page.goto('http://design.test/client.html')
    if not restored:
        expect(page.locator('#filesCard')).to_be_visible()
        page.locator('#fileInput').set_input_files({'name':'test.txt','mimeType':'text/plain','buffer':b'test'})
    notice = page.get_by_role('alertdialog', name='File group update')
    expect(notice).to_contain_text('Group rejected')
    expect(page.locator('#batchProgress .batch-card')).to_have_count(0)
    notice.get_by_role('button', name='OK', exact=True).click()
    expect(page.locator('.group-notice')).to_have_count(0)
    expect(page.locator('#batchProgress')).to_be_empty()


def test_cancel_all_clears_rows_and_reload_does_not_restore_them(browser_page):
    page = browser_page; state = setup(page); state['connected'] = True
    state['transfers'] = [dict(id=str(i), filename=f'file{i}.txt', state='active', revision=1) for i in range(3)]
    def cancel(route):
        for item in state['transfers']: item.update(state='cancelled', revision=2)
        route.fulfill(json={'ok':True})
    page.route('**/api/transfers/actions/cancel_all', cancel)
    page.goto('http://design.test/client.html')
    expect(page.locator('#transferQueue .transfer-row')).to_have_count(3)
    page.get_by_role('button', name='Cancel all', exact=True).click()
    expect(page.locator('#transferQueue .transfer-row')).to_have_count(0)
    page.reload()
    expect(page.locator('#filesCard')).to_be_visible()
    expect(page.locator('#transferQueue .transfer-row')).to_have_count(0)


def test_reloaded_preparing_group_remains_cancellable(browser_page):
    page = browser_page
    state = setup(page); state['connected'] = True
    page.route('**/api/client/batches', lambda route: route.fulfill(json={'batches': [
        {'batch_id': 'preparing', 'count': 2, 'status': 'preparing'}]}))
    page.route('**/api/client/batches/preparing', lambda route: route.fulfill(
        json={'status': 'preparing', 'prepared_bytes': 1048576}))
    page.goto('http://design.test/client.html')
    expect(page.locator('#batchProgress')).to_contain_text('1.0 MB processed')
    page.locator('#batchProgress').get_by_role('button', name='Cancel group').click()
    expect(page.locator('#batchProgress')).to_be_empty()
