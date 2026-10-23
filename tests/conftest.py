"""Opt-in memory transport for sandboxed protocol/engine contract checks.

Normal pytest runs use real sockets. The option only substitutes socketpair;
it cannot exercise TCP listeners, HTTP, TLS, browser integration, or packaging.
"""
import socket
import pytest

# Shared fixtures are registered here; test modules no longer provide fixtures
# to other test modules. Importing these modules does not start peers or browsers.
from helpers.browser import browser_page
from helpers.app_peers import app_pair
from helpers.native_peers import native_pair, native_http
from helpers.asgi import app_api


def pytest_addoption(parser):
    parser.addoption('--memory-transport', action='store_true',
                     help='Use an in-memory byte stream for socketpair-only tests (not TCP coverage)')


@pytest.fixture(autouse=True)
def optional_memory_transport(request, monkeypatch):
    if request.config.getoption('--memory-transport'):
        from memory_transport import socketpair
        monkeypatch.setattr(socket, 'socketpair', socketpair)
