"""Full native Python/browser/load + Node verification, failing on skipped tests.

Requires requirements-dev.txt, Node and Playwright Chromium. Reports go into a
new directory; previous results are never overwritten. No memory transport.
"""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    project = Path(__file__).resolve().parents[1]
    env = dict(os.environ, PYTHONUTF8='1', FT_LOCAL_LOAD='1', FT_BENCHMARK_UPLOAD='1',
               FT_LOG_MAX_BYTES='100000000', FT_LOG_FILE=str(output / 'runtime.log'))
    commands = [('python', [sys.executable, '-u', '-m', 'pytest', '-q', '--timeout=300',
        '-o', 'junit_family=legacy', '--basetemp=' + str(output / 'tmp'),
        '--junitxml=' + str(output / 'python.xml')]),
        ('node', ['node', '--test', 'tests/queue.test.cjs'])]
    for name, command in commands:
        print('Running', name, 'suite; log:', output / (name + '.log'), flush=True)
        with (output / (name + '.log')).open('w', encoding='utf-8') as log:
            result = subprocess.run(command, cwd=project, env=env, stdout=log,
                                    stderr=subprocess.STDOUT)
        if result.returncode:
            return result.returncode
    root = ET.parse(output / 'python.xml').getroot()
    if root.findall('.//skipped') or root.findall('.//failure') or root.findall('.//error'):
        print('Incomplete verification: inspect Python XML (skips are not success).')
        return 1
    print('Verified', len(root.findall('.//testcase')), 'Python cases and Node suite.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
