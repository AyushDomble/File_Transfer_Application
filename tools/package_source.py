"""Create an exclusive, manifest-verified source ZIP; never include runtime state."""
import argparse
import hashlib
import json
from pathlib import Path
import zipfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    files = {}
    folders = {
        'filetransfer': {'.py'},
        'webapp': {'.py', '.html', '.css', '.js', '.svg', '.png', '.ico'},
        'tests': {'.py', '.cjs', '.md'},
        'tools': {'.py'},
        '01-Use-the-application': {'.md'},
        '02-Understand-and-integrate': {'.md'},
        '03-Validate-and-operate': {'.md'},
    }
    for folder, suffixes in folders.items():
        for path in (root / folder).rglob('*'):
            if path.is_file() and not path.is_symlink() and path.suffix in suffixes and not any(part.startswith('.') or part == '__pycache__' for part in path.relative_to(root).parts):
                files[path.relative_to(root).as_posix()] = path.read_bytes()
    for name in (
        '.gitignore',
        'README.md',
        'OPERATING_SYSTEM_CONCEPTS.md',
        'COMPUTER_NETWORK_CONCEPTS.md',
        'launch.py',
        'requirements.txt',
        'requirements-dev.txt',
        'requirements-release-lock.txt',
    ):
        path = root / name
        if path.exists():
            files[name] = path.read_bytes()
    for name in ('protocol', 'client', 'server', 'compression'):
        assert f'filetransfer/{name}.py' not in files, 'Disallowed module unexpectedly included'
    manifest = {name: hashlib.sha256(data).hexdigest() for name, data in sorted(files.items())}
    files['SOURCE_MANIFEST.json'] = json.dumps(manifest, indent=2).encode()
    archive = args.output.resolve()
    with zipfile.ZipFile(archive, 'x', compression=zipfile.ZIP_DEFLATED) as output:
        for name, data in sorted(files.items()):
            output.writestr('filetransfer/' + name, data)
    with zipfile.ZipFile(archive) as check:
        assert check.testzip() is None
        for name, digest in manifest.items():
            assert hashlib.sha256(check.read('filetransfer/' + name)).hexdigest() == digest
    print(json.dumps({'archive': str(archive), 'files': len(manifest),
                      'sha256': hashlib.sha256(archive.read_bytes()).hexdigest()}, indent=2))


if __name__ == '__main__':
    main()
