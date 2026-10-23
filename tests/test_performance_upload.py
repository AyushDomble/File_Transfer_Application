"""Opt-in measured HTTP staging/preparation/TLS transfer workloads.

Uses the real authenticated local API, not a browser rendering benchmark.
Run with FT_BENCHMARK_UPLOAD=1 and --junitxml to retain timings.
"""
import hashlib
import os
from pathlib import Path
import time
import zipfile

import httpx
import pytest
from helpers.native_peers import native_pair, native_http
from filetransfer.transfer import TransferState


@pytest.mark.skipif(os.environ.get('FT_BENCHMARK_UPLOAD') != '1', reason='opt-in performance workload')
@pytest.mark.parametrize('zipped', [False, True])
@pytest.mark.parametrize('compressible', [False, True])
def test_measured_upload(native_http, zipped, compressible, record_property):
    base, api, host, link = native_http
    host.require_batch_approval = True
    block = b'x' * 1048576 if compressible else os.urandom(1048576)
    content = block * 8
    expected = hashlib.sha256(content).hexdigest()
    names = [f'load-{index}.bin' for index in range(5)]
    def wait_for(predicate):
        deadline = time.monotonic() + 180
        while not predicate():
            assert time.monotonic() < deadline, 'Workload timed out'
            time.sleep(.01)
    with httpx.Client(base_url=base, timeout=60, headers=api._test_http_headers) as client:
        started = time.perf_counter()
        response = client.post('/api/client/batches', json={
            'files': [dict(filename=n, size=len(content)) for n in names], 'zipped': zipped})
        response.raise_for_status()
        bid = response.json()['batch_id']
        for index in range(len(names)):
            for chunk in range(2):
                response = client.put(
                    f'/api/client/batches/{bid}/files/{index}?chunk_index={chunk}&final={str(chunk == 1).lower()}',
                    content=content[chunk*4194304:(chunk+1)*4194304])
                response.raise_for_status()
        staged = time.perf_counter()
        response = client.post(f'/api/client/batches/{bid}/request')
        assert response.status_code == 202, response.text
        wait_for(lambda: bool(host.batches.pending()))
        prepared = time.perf_counter()
        pending = host.batches.pending()[0]
        client.post(f"/api/host/batches/{pending['peer_id']}/{bid}/accept").raise_for_status()
        client.get(f'/api/client/batches/{bid}').raise_for_status()
        wait_for(lambda: bool(link.manager.all_transfers()) and all(
            t.is_terminal and t.io_finished.is_set() for t in link.manager.all_transfers()))
        completed = time.perf_counter()
        assert all(t.state == TransferState.COMPLETED for t in link.manager.all_transfers())
        if zipped:
            with zipfile.ZipFile(Path(host.dest_dir) / f'Selection-{bid[:8]}.zip') as archive:
                assert sorted(archive.namelist()) == sorted(names)
                for name in names:
                    with archive.open(name) as stream:
                        assert hashlib.file_digest(stream, 'sha256').hexdigest() == expected
        else:
            for name in names:
                with (Path(host.dest_dir) / name).open('rb') as stream:
                    assert hashlib.file_digest(stream, 'sha256').hexdigest() == expected
        for name, value in {
            'staging_seconds': staged-started, 'preparation_seconds': prepared-staged,
            'approval_and_transfer_seconds': completed-prepared,
            'total_seconds': completed-started, 'source_mib': 40,
            'source_mib_per_second': 40/(completed-started),
        }.items():
            record_property(name, value)
