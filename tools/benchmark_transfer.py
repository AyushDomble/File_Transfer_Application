"""Repeatable localhost TLS + durable-journal benchmark (not a LAN claim).

Run each implementation in a fresh process using --project-root. Source creation,
connection setup and final independent verification are outside the timed region.
--files is per client; --file-kib overrides --mib. Run workloads sequentially.
Process I/O is not physical disk I/O; system network counters may omit loopback.
"""
import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path
import statistics
import sys
import tempfile
import threading
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--mib', type=int, default=16)
    parser.add_argument('--file-kib', type=int, help='Override --mib for tiny-file workloads')
    parser.add_argument('--clients', type=int, default=1)
    parser.add_argument('--files', type=int, default=5)
    parser.add_argument('--concurrency', type=int, default=5)
    parser.add_argument('--chunk-kib', type=int, default=1024)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--ack-delay-ms', type=float, default=0,
                        help='Synthetic delay before each ACK; not a network RTT emulator')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--temp-dir', type=Path, default=Path.cwd())
    args = parser.parse_args()
    if min(args.mib, args.files, args.concurrency, args.chunk_kib, args.repeats, args.clients) < 1:
        parser.error('All sizes and counts must be positive')
    if args.file_kib is not None and args.file_kib < 1:
        parser.error('--file-kib must be positive')
    file_bytes = args.file_kib * 1024 if args.file_kib is not None else args.mib * 1048576
    required = file_bytes * args.files * args.clients * 2 + 1073741824
    if shutil.disk_usage(args.temp_dir).free < required:
        parser.error('Insufficient free space for source + destination + 1 GiB safety margin')
    os.environ.setdefault('FT_LOG_MAX_BYTES', '100000000')
    if args.chunk_kib > 8192:
        parser.error('Chunk size must not exceed 8192 KiB')
    if not 0 <= args.ack_delay_ms <= 1000:
        parser.error('ACK delay must be between 0 and 1000 ms')
    sys.path.insert(0, str(args.project_root.resolve()))
    import psutil
    from filetransfer import config
    from filetransfer.host import TransferHost
    from filetransfer.hostlink import HostLink
    from filetransfer.transfer import TransferState
    from filetransfer.persistence import PersistenceStore
    from filetransfer import engine, scheduler
    from filetransfer.session_protocol import Session
    from filetransfer.transport import FrameType
    if args.ack_delay_ms:
        original_send = Session.send
        def delayed_ack(session, frame):
            if frame.frame_type == FrameType.TRANSFER_ACK:
                time.sleep(args.ack_delay_ms / 1000)
            return original_send(session, frame)
        Session.send = delayed_ack
    timings, timing_lock = {}, threading.Lock()
    def instrument(owner, name, label):
        original = getattr(owner, name)
        def measured(*a, **kw):
            started = time.perf_counter()
            try:
                return original(*a, **kw)
            finally:
                with timing_lock:
                    record = timings.setdefault(label, {'calls': 0, 'seconds': 0.0})
                    record['calls'] += 1
                    record['seconds'] += time.perf_counter() - started
        setattr(owner, name, measured)
    instrument(engine, '_compute_sha256', 'engine_full_hash')
    instrument(scheduler, '_compute_sha256', 'submission_full_hash')
    instrument(engine.SendEngine, '_wait_for_ack', 'ack_wait')
    instrument(PersistenceStore, 'save', 'journal_save')
    stores = []
    original_init = PersistenceStore.__init__
    def track_store(store, *a, **kw):
        original_init(store, *a, **kw)
        stores.append(store)
    PersistenceStore.__init__ = track_store
    config.HOST, config.PORT, config.TLS_ENABLED = '127.0.0.1', 0, True
    config.PIN_EXPIRY_SECONDS, config.STATE_DIR = 0, None
    runs = []
    for repeat in range(args.repeats):
        # Keep database finalizers and temporary directory ownership deterministic.
        with tempfile.TemporaryDirectory(prefix='bench-', dir=args.temp_dir.resolve()) as root:
            root = Path(root)
            sources = []
            block = os.urandom(1024 * 1024)
            digest = hashlib.sha256()
            for offset in range(0, file_bytes, len(block)):
                digest.update(block[:min(len(block), file_bytes-offset)])
            expected = digest.hexdigest()
            for index in range(args.files * args.clients):
                path = root / f'input-{index}.bin'
                with path.open('wb') as stream:
                    for offset in range(0, file_bytes, len(block)):
                        stream.write(block[:min(len(block), file_bytes-offset)])
                sources.append(path)
            host = TransferHost(dest_dir=str(root / 'host'), max_concurrent_per_peer=args.concurrency)
            host._discovery.start = lambda: None
            host.add_pending_listener(lambda info: host.accept_pending(info['id']))
            link = None
            links = []
            stop = threading.Event()
            samples = []
            process = psutil.Process()
            def sample():
                while not stop.wait(0.02):
                    samples.append(process.memory_info().rss)
            sampler = threading.Thread(target=sample, daemon=True)
            try:
                host.start()
                for client_index in range(args.clients):
                    link = HostLink('127.0.0.1', port=host._sock.getsockname()[1], use_tls=True,
                        fingerprint=host.certificate_fingerprint, dest_dir=str(root / f'client-{client_index}'),
                        max_concurrent=args.concurrency)
                    links.append(link)
                    link.connect_with_pin(host.current_pin)
                cpu = process.cpu_times()
                io_before = process.io_counters()
                network_before = psutil.net_io_counters()
                rss_before = process.memory_info().rss
                sampler.start()
                with timing_lock: timings.clear()
                started = time.perf_counter()
                transfers = [links[index % args.clients].scheduler.submit_upload(
                    str(p), chunk_size=args.chunk_kib * 1024) for index, p in enumerate(sources)]
                submitted = time.perf_counter()
                deadline = started + 300
                while not all(t.is_terminal and t.io_finished.is_set() for t in transfers):
                    if any(t.state == TransferState.CONNECTION_LOST for t in transfers):
                        raise RuntimeError('Connection lost during benchmark; run is invalid')
                    if time.perf_counter() > deadline:
                        raise TimeoutError('Benchmark exceeded five minutes')
                    time.sleep(0.005)
                elapsed = time.perf_counter() - started
                cpu_after = process.cpu_times()
                io_after = process.io_counters()
                network_after = psutil.net_io_counters()
                stop.set(); sampler.join()
                for transfer, source in zip(transfers, sources):
                    if transfer.state != TransferState.COMPLETED:
                        raise RuntimeError(f'{transfer.state}: {transfer.error}')
                    with (root / 'host' / source.name).open('rb') as stream:
                        assert hashlib.file_digest(stream, 'sha256').hexdigest() == expected
                runs.append(dict(seconds=elapsed, submission_seconds=submitted-started,
                    mib_per_second=file_bytes * len(sources) / 1048576 / elapsed,
                    cpu_seconds=(cpu_after.user + cpu_after.system) - (cpu.user + cpu.system),
                    peak_rss_mib=max(samples + [rss_before]) / 1048576,
                    rss_growth_mib=(max(samples + [rss_before])-rss_before) / 1048576,
                    process_io_bytes={field: getattr(io_after, field)-getattr(io_before, field)
                                      for field in ('read_bytes', 'write_bytes')},
                    system_network_bytes={field: getattr(network_after, field)-getattr(network_before, field)
                                          for field in ('bytes_sent', 'bytes_recv')},
                    inclusive_worker_timings=json.loads(json.dumps(timings))))
            finally:
                stop.set()
                if sampler.is_alive(): sampler.join()
                for link in links: link.close()
                host.stop()
                for store in stores:
                    store._anchor_finalizer()
                stores.clear()
                # Store finalizers own SQLite handles; Windows needs them closed
                # before TemporaryDirectory removes this isolated benchmark tree.
                import gc
                del link, host
                gc.collect()
    report = dict(environment='localhost; warm source cache; combined host/client process; TLS and FULL journals enabled',
        project_root=str(args.project_root.resolve()), bytes_per_file=file_bytes,
        clients=args.clients, files_per_client=args.files,
        concurrency=args.concurrency, chunk_kib=args.chunk_kib, ack_delay_ms=args.ack_delay_ms, runs=runs,
        median_mib_per_second=statistics.median(r['mib_per_second'] for r in runs))
    args.output.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
