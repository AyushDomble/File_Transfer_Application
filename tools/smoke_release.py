"""Smoke source or a frozen executable in NEW disposable runtime directories.

Uses only the standard library. Run from the extracted release package, not an
existing developer runtime. Does not open a native window or test WebView2 UI.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--exe', type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    project = Path(__file__).resolve().parents[1]
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        port = listener.getsockname()[1]
    # A port race is a failed smoke run, never permission to connect to another app.
    env = {k: v for k, v in os.environ.items() if not k.startswith('FT_')}
    env.update(PYTHONUTF8='1', PYTHON_DOTENV_DISABLED='1',
               FT_HOST='127.0.0.1', FT_PORT='0', FT_DISCOVERY_PORT='0',
               FT_WEBAPP_PORT=str(port), FT_TLS_ENABLED='true',
               FT_SERVER_DIR=str(output / 'server'), FT_CLIENT_DIR=str(output / 'client'),
               FT_STATE_DIR=str(output / 'state'), FT_LOG_FILE=str(output / 'runtime.log'))
    command = ([str(args.exe.resolve())] if args.exe else [sys.executable, str(project / 'launch.py')])
    command += ['--smoke-test']
    with (output / 'smoke.log').open('w', encoding='utf-8') as log:
        result = subprocess.run(command, cwd=output, env=env, stdout=log,
                                stderr=subprocess.STDOUT, timeout=90,
                                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    success = result.returncode == 0 and '"smoke": "passed"' in (output / 'smoke.log').read_text(encoding='utf-8')
    report = dict(passed=success, returncode=result.returncode,
                  mode='executable' if args.exe else 'source', native_window_tested=False,
                  python=sys.version, command=command)
    (output / 'result.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2))
    return 0 if success else 1


if __name__ == '__main__':
    raise SystemExit(main())
