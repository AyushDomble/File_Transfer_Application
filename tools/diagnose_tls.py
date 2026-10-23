"""Run benchmark_transfer with bounded SSL entry/error diagnostics.

Accepts its normal arguments plus --diagnostics-output PATH. No payloads, PINs,
certificates or recovery secrets are recorded. Diagnostic timing is intrusive;
do not interpret its throughput as an uninstrumented benchmark.
"""
import json
from pathlib import Path
import runpy
import ssl
import sys
import threading


def main():
    index = sys.argv.index('--diagnostics-output')
    output = Path(sys.argv[index + 1])
    del sys.argv[index:index + 2]
    lock = threading.Lock()
    active = {}
    report = {'overlapping_entries': 0, 'send_calls': 0, 'recv_calls': 0, 'errors': []}
    originals = {}
    def wrap(name, opposite):
        original = getattr(ssl.SSLSocket, name)
        originals[name] = original
        def tracked(sock, *args, **kwargs):
            key = id(sock)
            with lock:
                state = active.setdefault(key, {'send': 0, 'recv_into': 0})
                overlap = state[opposite] > 0
                report['overlapping_entries'] += int(overlap)
                report['send_calls' if name == 'send' else 'recv_calls'] += 1
                state[name] += 1
            try:
                return original(sock, *args, **kwargs)
            except OSError as exc:
                with lock:
                    if len(report['errors']) < 100 and not isinstance(exc, (ssl.SSLWantReadError, ssl.SSLWantWriteError, TimeoutError)):
                        report['errors'].append({'operation': name, 'type': type(exc).__name__,
                            'errno': exc.errno, 'winerror': getattr(exc, 'winerror', None),
                            'message': str(exc), 'overlap_at_entry': overlap,
                            'other_active_at_error': state[opposite] > 0,
                            'socket_timeout': sock.gettimeout()})
                raise
            finally:
                with lock:
                    state[name] -= 1
                    if not any(state.values()): active.pop(key, None)
        setattr(ssl.SSLSocket, name, tracked)
    wrap('send', 'recv_into')
    wrap('recv_into', 'send')
    try:
        runpy.run_path(str(Path(__file__).with_name('benchmark_transfer.py')), run_name='__main__')
        report['completed'] = True
    except BaseException as exc:
        report['completed'] = False
        report['failure'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        for name, original in originals.items(): setattr(ssl.SSLSocket, name, original)
        output.write_text(json.dumps(report, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
