"""Shared browser test support."""
from pathlib import Path
import pytest


STATIC = Path(__file__).resolve().parents[2] / 'webapp' / 'static'

@pytest.fixture
def browser_page():
    playwright = pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as engine:
        try:
            browser = engine.chromium.launch(timeout=10000)
        except playwright.Error as exc:
            pytest.skip('Chromium unavailable: ' + str(exc).splitlines()[0])
        try:
            context = browser.new_context()
            yield context.new_page()
        finally:
            browser.close()
