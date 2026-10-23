from types import SimpleNamespace
import pytest
from webapp.main import JsApi, wait_for_backend


def test_ready_backend_returns():
    wait_for_backend(SimpleNamespace(started=True), None)


def test_dead_backend_fails_without_opening_window():
    with pytest.raises(RuntimeError, match='stopped'):
        wait_for_backend(SimpleNamespace(started=False), SimpleNamespace(is_alive=lambda: False))


def test_backend_deadline_is_bounded():
    with pytest.raises(RuntimeError, match='deadline'):
        wait_for_backend(SimpleNamespace(started=False), SimpleNamespace(is_alive=lambda: True), timeout=0)


@pytest.mark.parametrize('value,expected', [(None, None), ([], None), (['a'], 'a'), ('a', 'a')])
def test_native_dialog_normalization(value, expected):
    assert JsApi._first_path(value) == expected


def test_scoped_journal_in_long_install_path(tmp_path):
    from filetransfer.persistence import PersistenceStore
    folder = tmp_path / ('nested-' + 'a' * 65) / ('nested-' + 'b' * 65)
    root = PersistenceStore(str(folder / 'client.sqlite3'))
    child = root.scoped('endpoint:tls://127.0.0.1:5000')
    child.set_setting('release-check', {'persisted': True})
    assert child.get_setting('release-check') == {'persisted': True}
    assert root.list_scopes() == ['endpoint:tls://127.0.0.1:5000']
