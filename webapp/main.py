"""
Desktop app entry point. Run this (or the PyInstaller-built .exe from it)
to launch the app -- starts a local-only FastAPI server on 127.0.0.1,
then opens a native window (pywebview) pointed at it. No browser chrome,
no internet exposure, looks and feels like a regular desktop app.
"""

import threading
import time
import argparse
import sys

import uvicorn

from filetransfer import config
from .local_security import BOOTSTRAP_TOKEN


class JsApi:
    """Exposed to the frontend as `pywebview.api.<method>` -- used for
    things a browser sandbox can't do, like native file/save dialogs."""

    def pick_file(self):
        import webview
        dialog_type = getattr(getattr(webview, "FileDialog", None), "OPEN", None) or webview.OPEN_DIALOG
        result = webview.windows[0].create_file_dialog(dialog_type)
        return self._first_path(result)

    def pick_save_location(self, suggested_name: str):
        import webview
        dialog_type = getattr(getattr(webview, "FileDialog", None), "SAVE", None) or webview.SAVE_DIALOG
        result = webview.windows[0].create_file_dialog(dialog_type, save_filename=suggested_name)
        return self._first_path(result)

    @staticmethod
    def _first_path(result):
        """Normalize the dialog result across pywebview versions -- some
        return a bare string for a save dialog, others a tuple/list even
        for a single-file save. None/empty means the user cancelled."""
        if not result:
            return None
        if isinstance(result, (list, tuple)):
            return result[0] if result else None
        return result


def make_backend():
    return uvicorn.Server(uvicorn.Config(
        "webapp.api:app",
        host="127.0.0.1",
        port=config.WEBAPP_PORT,
        log_level="warning",
        access_log=False,
    ))


def wait_for_backend(server, thread, timeout=20):
    deadline = time.monotonic() + timeout
    while not server.started:
        if not thread.is_alive():
            raise RuntimeError('Local backend stopped before becoming ready; check the port and log.')
        if time.monotonic() >= deadline:
            raise RuntimeError('Local backend did not become ready within the startup deadline.')
        time.sleep(.05)


def main():
    parser = argparse.ArgumentParser(description='File Transfer desktop launcher')
    parser.add_argument('--smoke-test', action='store_true',
                        help='Check backend, assets, local authorization and TLS host startup, then exit (no window).')
    args = parser.parse_args()
    server = make_backend()
    backend_thread = threading.Thread(target=server.run, daemon=True)
    backend_thread.start()
    try:
        wait_for_backend(server, backend_thread)
        if args.smoke_test:
            from .release_smoke import check_running_app
            check_running_app()
        else:
            import webview
            webview.create_window(
                "File Transfer",
                f"http://127.0.0.1:{config.WEBAPP_PORT}/launch/{BOOTSTRAP_TOKEN}",
                js_api=JsApi(), width=980, height=680, min_size=(760, 540),
            )
            webview.start()
    finally:
        server.should_exit = True
        backend_thread.join(timeout=15)
        if backend_thread.is_alive():
            raise RuntimeError('Backend did not shut down within 15 seconds.')


if __name__ == "__main__":
    sys.exit(main())
