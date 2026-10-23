"""TLS write boundaries, full-duplex ownership and native fault recovery."""
import hashlib
from pathlib import Path
import socket
import ssl
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from filetransfer.session_protocol import Session, SessionError
from filetransfer.tls_io import TLSIO
from filetransfer.transfer import TransferState
from filetransfer.transport import Frame, FrameType
from helpers.native_peers import native_pair
from helpers.app_peers import wait


class ScriptedTLS:
    def __init__(self, actions=()):
        self.actions = iter(actions)
        self.output = bytearray()
        self.arguments = []
        self.blocking = True

    def gettimeout(self): return .5
    def setblocking(self, value): self.blocking = value

    def send(self, data):
        self.arguments.append(bytes(data))
        action = next(self.actions, len(data))
        if isinstance(action, Exception): raise action
        count = min(action, len(data))
        self.output.extend(data[:count])
        return count


def test_tls_short_writes_and_wants_never_repeat_accepted_prefix(monkeypatch):
    sock = ScriptedTLS([3, ssl.SSLWantWriteError(), ssl.SSLWantReadError(), 2])
    io = TLSIO(sock, write_timeout=10)
    waits = []
    monkeypatch.setattr(io, '_wait', lambda writing, deadline: waits.append(writing))
    io.sendall(b'abcdefghij')
    assert sock.output == b'abcdefghij'
    assert sock.arguments == [b'abcdefghij', b'defghij', b'defghij', b'defghij', b'fghij']
    assert waits == [True, False]
    assert not sock.blocking


def test_read_poll_timeout_does_not_shorten_write_deadline(monkeypatch):
    sock = ScriptedTLS([ssl.SSLWantWriteError(), 10])
    io = TLSIO(sock, write_timeout=10)
    io.read_timeout = .001
    def delayed_ready(writing, deadline):
        time.sleep(.02)
        assert deadline - time.monotonic() > 9
    monkeypatch.setattr(io, '_wait', delayed_ready)
    io.sendall(b'abc')
    assert sock.output == b'abc'


def test_tls_deadline_after_partial_write_does_not_restart_frame():
    sock = ScriptedTLS([3, ssl.SSLWantWriteError()])
    io = TLSIO(sock, write_timeout=0)
    with pytest.raises(TimeoutError): io.sendall(b'abcdef')
    assert sock.output == b'abc'
    assert sock.arguments == [b'abcdef']


def test_ssl_operations_are_serialized_but_network_wait_releases_lock(monkeypatch):
    sock = ScriptedTLS()
    reading, release = threading.Event(), threading.Event()
    attempts = [0]
    def receive(buffer, length):
        attempts[0] += 1
        if attempts[0] == 1: raise ssl.SSLWantReadError()
        buffer[0] = 42
        return 1
    sock.recv_into = receive
    io = TLSIO(sock, write_timeout=10)
    def waiting(*args):
        reading.set()
        assert release.wait(3)
    monkeypatch.setattr(io, '_wait', waiting)
    result = []
    reader = threading.Thread(target=lambda: result.append(io.recv_into(bytearray(1), 1)))
    reader.start()
    try:
        assert reading.wait(2)
        io.sendall(b'write while reader waits')
        assert sock.output == b'write while reader waits'
    finally:
        release.set(); reader.join(3)
    assert result == [1] and not reader.is_alive()


def test_ssl_object_is_never_entered_from_two_threads():
    class OverlapDetector(ScriptedTLS):
        active = False
        def operation(self):
            assert not self.active, 'overlapping calls into one SSL object'
            self.active = True
            try: time.sleep(.001)
            finally: self.active = False
        def send(self, data):
            self.operation()
            return len(data)
        def recv_into(self, buffer, length):
            self.operation()
            buffer[0] = 1
            return 1
    io = TLSIO(OverlapDetector(), write_timeout=5)
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = [pool.submit(io.sendall, b'abc') if i % 2 else
                pool.submit(io.recv_into, bytearray(1), 1) for i in range(100)]
        for job in jobs: job.result(timeout=5)


def test_failed_partial_frame_poisoning_blocks_queued_writers():
    entered, release = threading.Event(), threading.Event()
    class BrokenSocket:
        calls = 0
        stopped = False
        def sendall(self, data):
            self.calls += 1
            self.prefix = data[:7]
            entered.set()
            assert release.wait(3)
            raise BlockingIOError(10035, 'injected partial write')
        def shutdown(self, how): self.stopped = True
    sock = BrokenSocket()
    session = Session(sock, ('test', 0))
    failures = []
    def send():
        try: session.send(Frame(FrameType.PING))
        except (OSError, SessionError) as exc: failures.append(exc)
    first, second = threading.Thread(target=send), threading.Thread(target=send)
    first.start()
    assert entered.wait(2)
    second.start(); release.set()
    first.join(3); second.join(3)
    assert len(failures) == 2 and sock.calls == 1
    assert sock.stopped and session.is_lost
    assert any(isinstance(exc, SessionError) for exc in failures)


def delayed_acks(monkeypatch, delay=.02):
    original = Session.send
    def send(session, frame):
        if frame.frame_type == FrameType.TRANSFER_ACK: time.sleep(delay)
        return original(session, frame)
    monkeypatch.setattr(Session, 'send', send)


def sources(tmp_path, count=5):
    paths = []
    for i in range(count):
        path = tmp_path / f'tls-{i}.bin'
        path.write_bytes(bytes([i + 1]) * 2 * 1024 * 1024)
        paths.append(path)
    return paths


def check_files(host, paths):
    for path in paths:
        with path.open('rb') as source, (Path(host.dest_dir) / path.name).open('rb') as target:
            assert hashlib.file_digest(source, 'sha256').digest() == hashlib.file_digest(target, 'sha256').digest()


@pytest.mark.timeout(120)
@pytest.mark.parametrize('cycle', range(3))
def test_native_five_delayed_rate_limited_transfers_cancel_isolated(native_pair, tmp_path, monkeypatch, cycle):
    host, link = native_pair
    delayed_acks(monkeypatch)
    raw = link.session._sock
    original_send = raw.send
    # Actual TLS short writes plus a deterministic per-write rate constraint.
    def limited(data, flags=0):
        time.sleep(.001)
        return original_send(data[:16384], flags)
    monkeypatch.setattr(raw, 'send', limited)
    paths = sources(tmp_path)
    transfers = [link.scheduler.submit_upload(str(p), chunk_size=65536) for p in paths]
    wait(lambda: transfers[0].offset > 0)
    link.scheduler.cancel(transfers[0].id)
    wait(lambda: all(t.is_terminal and t.io_finished.is_set() for t in transfers), timeout=90)
    assert transfers[0].state == TransferState.CANCELLED
    assert all(t.state == TransferState.COMPLETED for t in transfers[1:])
    assert not (Path(host.dest_dir) / paths[0].name).exists()
    assert link.connected and not link.session.is_lost
    check_files(host, paths[1:])


@pytest.mark.timeout(120)
@pytest.mark.parametrize('cycle', range(3))
def test_native_partial_frame_drop_recovers_five_same_ids(native_pair, tmp_path, monkeypatch, cycle):
    host, link = native_pair
    delayed_acks(monkeypatch)
    old_session = link.session
    peer = host.get_peer(host.list_peers()[0]['peer_id'])
    original = old_session._io.sendall
    frames = [0]
    submitted = threading.Event()
    def break_frame(data):
        if data[5] == FrameType.TRANSFER_DATA:
            # Exercise five admitted jobs, not a timing-dependent mixture of
            # admitted and newly queued jobs after the injected disconnection.
            assert submitted.wait(5)
            frames[0] += 1
            if frames[0] == 7:
                original(data[:100])  # valid header, incomplete data payload
                raise ConnectionResetError('injected mid-frame disconnect')
        return original(data)
    monkeypatch.setattr(old_session._io, 'sendall', break_frame)
    paths = sources(tmp_path)
    try:
        transfers = [link.scheduler.submit_upload(str(p), chunk_size=65536) for p in paths]
    finally:
        submitted.set()
    ids = [t.id for t in transfers]
    wait(lambda: old_session.is_lost)
    wait(lambda: all(t.io_finished.is_set() for t in transfers))
    wait(lambda: all(t.io_finished.is_set() for t in peer.manager.all_transfers()))
    assert all(t.state == TransferState.CONNECTION_LOST for t in transfers)
    assert all(not (Path(host.dest_dir) / p.name).exists() for p in paths)
    assert any(t.offset > 0 for t in peer.manager.all_transfers())
    link.close()
    link.connect_with_pin(host.current_pin)
    wait(lambda: all(t.is_terminal and t.io_finished.is_set() for t in transfers), timeout=90)
    assert link.session is not old_session
    assert [t.id for t in transfers] == ids
    assert all(t.state == TransferState.COMPLETED for t in transfers)
    check_files(host, paths)


@pytest.mark.timeout(120)
@pytest.mark.parametrize('cycle', range(3))
def test_native_full_duplex_small_socket_buffers(native_pair, tmp_path, monkeypatch, cycle):
    host, link = native_pair
    delayed_acks(monkeypatch)
    peer = host.get_peer(host.list_peers()[0]['peer_id'])
    for session in (link.session, peer.session):
        session._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    paths = sources(tmp_path, 3)
    downloads = []
    for i in range(2):
        path = Path(host.dest_dir) / f'download-{i}.bin'
        path.write_bytes(bytes([100+i]) * 2 * 1024 * 1024)
        downloads.append(path)
    transfers = [link.scheduler.submit_upload(str(p)) for p in paths]
    transfers += [link.scheduler.submit_download(p.name) for p in downloads]
    wait(lambda: all(t.is_terminal and t.io_finished.is_set() for t in transfers), timeout=90)
    assert all(t.state == TransferState.COMPLETED for t in transfers)
    assert link.connected
    check_files(host, paths)
    for path in downloads:
        with path.open('rb') as source, (Path(link.dest_dir) / path.name).open('rb') as target:
            assert hashlib.file_digest(source, 'sha256').digest() == hashlib.file_digest(target, 'sha256').digest()


@pytest.mark.timeout(30)
def test_loss_during_receiver_construction_never_starts_or_holds_file(native_pair, tmp_path, monkeypatch):
    from filetransfer.engine import RecvEngine
    host, link = native_pair
    peer = host.get_peer(host.list_peers()[0]['peer_id'])
    entered, release = threading.Event(), threading.Event()
    original_init, original_run = RecvEngine.__init__, RecvEngine.run
    started = []
    def delayed_init(engine, *args, **kwargs):
        original_init(engine, *args, **kwargs)
        if engine._session is peer.session:
            entered.set()
            assert release.wait(5)
    def tracked_run(engine):
        if engine._session is peer.session: started.append(engine)
        return original_run(engine)
    monkeypatch.setattr(RecvEngine, '__init__', delayed_init)
    monkeypatch.setattr(RecvEngine, 'run', tracked_run)
    source = sources(tmp_path, 1)[0]
    transfer = link.scheduler.submit_upload(str(source))
    try:
        assert entered.wait(3)
        peer.router.declare_lost('injected loss during engine construction')
    finally:
        release.set()
    receiver = peer.manager.get(transfer.id)
    assert receiver.io_finished.wait(3)
    assert transfer.io_finished.wait(3)
    assert not started
    assert receiver.state == TransferState.CONNECTION_LOST
    assert not (Path(host.dest_dir) / source.name).exists()
