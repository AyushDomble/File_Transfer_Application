"""Build a Windows onedir executable into a NEW directory (no overwrite)."""
import argparse
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    project = Path(__file__).resolve().parents[1]
    command = [sys.executable, '-m', 'PyInstaller', '--noconfirm', '--onedir',
               '--name', 'FileTransfer', '--distpath', str(output / 'dist'),
               '--workpath', str(output / 'work'), '--specpath', str(output),
               '--paths', str(project), '--hidden-import', 'webapp.api',
               '--collect-all', 'webview', '--collect-submodules', 'uvicorn',
               '--add-data', str(project / 'webapp' / 'static') + ';webapp/static',
               str(project / 'launch.py')]
    with (output / 'build.log').open('w', encoding='utf-8') as log:
        subprocess.run(command, cwd=project, check=True, stdout=log, stderr=subprocess.STDOUT)
    print(output / 'dist' / 'FileTransfer' / 'FileTransfer.exe')


if __name__ == '__main__':
    main()
