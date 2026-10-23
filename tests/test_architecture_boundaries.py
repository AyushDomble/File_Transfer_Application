"""Enforce module boundaries and transfer concurrency defaults."""
import ast
import inspect
from pathlib import Path

from filetransfer.host import TransferHost
from filetransfer.hostlink import HostLink

ROOT = Path(__file__).resolve().parents[1]
DISALLOWED_MODULES = {'protocol', 'client', 'server', 'compression'}


def test_disallowed_modules_absent_and_no_source_imports():
    for name in DISALLOWED_MODULES:
        assert not (ROOT / 'filetransfer' / (name + '.py')).exists()
    for folder in ('filetransfer', 'webapp', 'tests', 'tools'):
        for path in (ROOT / folder).rglob('*.py'):
            tree = ast.parse(path.read_text(encoding='utf-8-sig'))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    assert not any(n.name in {'filetransfer.' + name for name in DISALLOWED_MODULES} for n in node.names), path
                elif isinstance(node, ast.ImportFrom):
                    assert node.module not in {'filetransfer.' + name for name in DISALLOWED_MODULES}, path
                    if node.module == 'filetransfer' or (folder == 'filetransfer' and node.level == 1 and node.module is None):
                        assert not DISALLOWED_MODULES.intersection(n.name for n in node.names), path
                    if folder == 'filetransfer' and node.level == 1:
                        assert node.module not in DISALLOWED_MODULES, path


def test_current_concurrency_defaults_preserved():
    assert inspect.signature(TransferHost).parameters['max_concurrent_per_peer'].default == 5
    assert inspect.signature(HostLink).parameters['max_concurrent'].default == 5
