"""Opt-in real localhost load checks; timings are diagnostic, not LAN guarantees."""
import hashlib
import json
import os
from pathlib import Path
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psutil
import pytest
from helpers.native_peers import native_pair, native_http
from helpers.browser import browser_page
from filetransfer.hostlink import HostLink
from filetransfer.session_protocol import Session
from filetransfer.transport import FrameType
from filetransfer.transfer import TransferState

pytestmark = pytest.mark.skipif(os.environ.get('FT_LOCAL_LOAD') != '1',
                               reason='opt-in local load measurements')


def sources(root, count, mib):
    block = os.urandom(1048576)
    expected = hashlib.sha256(block * mib).hexdigest()
    paths = []
    for i in range(count):
        path = root / f'load-{i}.bin'
        with path.open('wb') as stream:
            for _ in range(mib):
                stream.write(block)
        paths.append(path)
    return paths, expected


def finish(transfers, host, expected, timeout=120):
    deadline = time.monotonic() + timeout
    while not all(t.is_terminal and t.io_finished.is_set() for t in transfers):
        assert time.monotonic() < deadline, 'Transfer load timed out'
        assert not any(t.state == TransferState.CONNECTION_LOST for t in transfers)
        time.sleep(.01)
    for t in transfers:
        assert t.state == TransferState.COMPLETED, t.error
        path = Path(host.dest_dir) / t.filename
        with path.open('rb') as stream:
            assert hashlib.file_digest(stream, 'sha256').hexdigest() == expected
        path.unlink()  # Only this test's independently verified generated receiver file.


def slow_acks(monkeypatch, seconds):
    original = Session.send
    def send(session, frame):
        if frame.frame_type == FrameType.TRANSFER_ACK:
            time.sleep(seconds)
        return original(session, frame)
    monkeypatch.setattr(Session, 'send', send)


def test_three_minute_persistent_session_soak(native_pair, tmp_path, record_property):
    host, link = native_pair
    paths, expected = sources(tmp_path, 5, 8)
    process = psutil.Process()
    samples = []
    started = time.monotonic()
    rounds = 0
    while time.monotonic() - started < 180:
        transfers = [link.scheduler.submit_upload(str(p)) for p in paths]
        finish(transfers, host, expected)
        rounds += 1
        samples.append(dict(seconds=time.monotonic()-started,
                            rss=process.memory_info().rss,
                            handles=process.num_handles(),
                            threads=process.num_threads(),
                            retained_transfers=len(link.manager.all_transfers())))
        time.sleep(.1)
    assert rounds >= 10
    record_property('soak', json.dumps(dict(rounds=rounds, files=rounds*5,
                    mib=rounds*40, samples=samples)))


def test_three_client_fairness(native_pair, tmp_path, monkeypatch, record_property):
    host, first = native_pair
    links = [first]
    slow_acks(monkeypatch, .01)
    paths, expected = sources(tmp_path, 15, 16)
    results = []
    try:
        for index in range(2):
            link = HostLink('127.0.0.1', port=host._sock.getsockname()[1],
                use_tls=True, fingerprint=host.certificate_fingerprint,
                dest_dir=str(tmp_path / f'other-{index}'), max_concurrent=5)
            links.append(link)
            link.connect_with_pin(host.current_pin)
        for _ in range(5):
            barrier = threading.Barrier(3)
            groups = [None] * 3
            def submit(index):
                barrier.wait(timeout=10)
                groups[index] = [links[index].scheduler.submit_upload(str(p))
                                  for p in paths[index*5:(index+1)*5]]
            start = time.monotonic()
            with ThreadPoolExecutor(max_workers=3) as pool:
                list(pool.map(submit, range(3)))
            completion = [None]*3
            progress = []
            while any(value is None for value in completion):
                elapsed = time.monotonic()-start
                assert elapsed < 60
                progress.append([sum(t.offset for t in group) for group in groups])
                for index, group in enumerate(groups):
                    assert not any(t.state == TransferState.CONNECTION_LOST for t in group)
                    if completion[index] is None and all(t.is_terminal for t in group):
                        completion[index] = elapsed
                time.sleep(.02)
            rates = [80 / duration for duration in completion]
            fairness = sum(rates)**2 / (3*sum(rate*rate for rate in rates))
            finish([t for group in groups for t in group], host, expected)
            results.append(dict(completion_seconds=completion,
                                jain_completion_rate_index=fairness, progress_bytes=progress))
    finally:
        for link in links[1:]:
            link.close()
    record_property('fairness', json.dumps(results))


@pytest.mark.parametrize('side', ['client', 'host'])
def test_browser_responsiveness_under_tls_load(native_http, browser_page, tmp_path,
                                              monkeypatch, record_property, side):
    base, api, host, link = native_http
    slow_acks(monkeypatch, .05)
    paths, expected = sources(tmp_path, 5, 128)
    page = browser_page
    page.context.add_cookies([api._test_browser_cookie])
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(base + f'/{side}.html')
    page.locator('#refreshPage').wait_for()
    page.evaluate("""() => {
      window.loadFrames=[]; window.loadLongTasks=[];
      const installed=performance.now(); let last=null;
      function frame(now) {
        if(last === null) window.loadFirstCallbackDelay=now-installed;
        else loadFrames.push(now-last);
        last=now; requestAnimationFrame(frame);
      }
      requestAnimationFrame(frame);
      new PerformanceObserver(list => loadLongTasks.push(...list.getEntries().map(e=>e.duration)))
        .observe({entryTypes:['longtask']});
    }""")
    transfers = [link.scheduler.submit_upload(str(p)) for p in paths]
    actions = []
    for index in range(10):
        assert any(not t.is_terminal for t in transfers), 'Load ended before interaction sample'
        start = time.perf_counter()
        page.locator('#themeSelect').select_option('light' if index % 2 else 'dark')
        page.locator('#refreshPage').click()
        page.wait_for_function("document.querySelector('#refreshStatus').textContent.includes('Refreshed just now')")
        page.wait_for_function("!document.querySelector('#refreshPage').disabled")
        actions.append(time.perf_counter()-start)
        page.wait_for_timeout(150)
    assert page.locator('#transferQueue').inner_text().count('load-') >= 5
    metrics = page.evaluate("({frames:loadFrames,first_callback_delay:loadFirstCallbackDelay,long_tasks:loadLongTasks})")
    assert not errors, errors
    assert max(actions) < 5, 'UI interaction exceeded five-second guardrail'
    finish(transfers, host, expected)
    record_property('browser_load', json.dumps(dict(side=side, actions_seconds=actions, **metrics)))
