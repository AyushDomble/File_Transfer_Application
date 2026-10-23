"""Integrity, bounded retransmission and atomic scheduler admission contracts.

Peer tests use socketpair (native by default, explicit memory opt-in in CI).
"""
import hashlib
import io
import queue
import threading
import time
from types import SimpleNamespace

import pytest

from filetransfer import engine as engines
from filetransfer.engine import (MAX_CHUNK_RETRIES, AckTimeout, FrameRouter,
    RecvEngine, SendEngine, TransferError, _encode_data_payload)
from filetransfer.manager import TransferManager
from filetransfer.session_protocol import Feature, Session, SessionError
from filetransfer.scheduler import ConcurrentScheduler, _Job
from filetransfer.transfer import TransferDirection, TransferState, TransferStateError
from filetransfer.transport import Frame, FrameFlags, FrameType
from helpers.protocol import RecordingSession, data_frame, receiver
from helpers.persistent_peers import _make_peers, _wait_for_state, _corrupt_data_frames


def wait(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    assert predicate(), 'condition did not become true'


def sender(tmp_path, content=b'abcdefgh'):
    source = tmp_path / 'source.bin'
    source.write_bytes(content)
    session = RecordingSession()
    session.negotiation = SimpleNamespace(protocol_version=3)
    session.supports = lambda name: name == Feature.RETRY_RANGES
    manager = TransferManager()
    transfer = manager.create_transfer(TransferDirection.UPLOAD, source.name,
        len(content), hashlib.sha256(content).hexdigest())
    engine = SendEngine(session, transfer, manager, queue.Queue(), str(source), chunk_size=4)
    engine._outstanding_start = 4
    engine._outstanding_end = 8
    return session, manager, transfer, engine, source


@pytest.mark.parametrize('changes', [dict(offset=-1), dict(offset=True), dict(offset='4'),
    dict(offset=0), dict(end=9), dict(end=7), dict(sequence=True), dict(sequence=1),
    dict(sequence=None), dict(end=None)])
def test_retry_range_must_exactly_match_outstanding_chunk(tmp_path, changes):
    _, _, transfer, engine, _ = sender(tmp_path)
    request = dict(offset=4, end=8, sequence=2)
    request.update(changes)
    engine._queue.put(Frame.json_frame(FrameType.TRANSFER_RETRY, request,
        transfer_id=transfer.id, sequence=2))
    with pytest.raises(TransferError):
        engine._wait_for_ack(transfer.id, 2, 8)


def test_valid_range_retry_is_not_an_ack(tmp_path):
    _, _, transfer, engine, _ = sender(tmp_path)
    engine._queue.put(Frame.json_frame(FrameType.TRANSFER_RETRY,
        dict(offset=4, end=8, sequence=2), transfer_id=transfer.id, sequence=2))
    assert engine._wait_for_ack(transfer.id, 2, 8) == (8, False, True)
    assert transfer.offset == 0


@pytest.mark.parametrize('negotiated', [False, True])
def test_new_retry_frame_requires_capability(tmp_path, negotiated):
    session, _, transfer, engine, _ = sender(tmp_path)
    session.supports = lambda name: negotiated
    engine._queue.put(Frame.json_frame(FrameType.TRANSFER_RETRY,
        dict(offset=4, end=8, sequence=2), transfer_id=transfer.id, sequence=2))
    if negotiated:
        assert engine._wait_for_ack(transfer.id, 2, 8)[2]
    else:
        with pytest.raises(TransferError, match='not negotiated'):
            engine._wait_for_ack(transfer.id, 2, 8)


@pytest.mark.parametrize('retry_seq,offset', [(True, 4), (2, True), (2, 0), ('2', 4)])
def test_error_frame_retry_metadata_is_validated(tmp_path, retry_seq, offset):
    _, _, transfer, engine, _ = sender(tmp_path)
    engine._queue.put(Frame.json_frame(FrameType.TRANSFER_ERROR,
        dict(error='bad checksum', retry_seq=retry_seq, offset=offset), transfer_id=transfer.id))
    with pytest.raises(TransferError):
        engine._wait_for_ack(transfer.id, 2, 8)


@pytest.mark.parametrize('range_capability', [True, False])
def test_receiver_requests_only_bad_range_without_writing(tmp_path, range_capability):
    session, _, transfer, engine = receiver(tmp_path, b'abcd')
    session.negotiation = SimpleNamespace(protocol_version=3)
    session.supports = lambda name: range_capability
    broken = Frame(FrameType.TRANSFER_DATA, _encode_data_payload(0, b'abcd')[:-1] + b'x',
        transfer_id=transfer.id, flags=FrameFlags.FINAL_CHUNK)
    output, hasher = io.BytesIO(), hashlib.sha256()
    assert engine._receive_chunk(broken, 0, hasher, output) == 0
    assert output.getvalue() == b'' and transfer.offset == 0
    reply = session.frames[-1]
    assert reply.frame_type == (FrameType.TRANSFER_RETRY if range_capability else FrameType.TRANSFER_ERROR)
    assert reply.json()['offset'] == 0
    if range_capability:
        assert reply.json()['end'] == 4 and reply.json()['sequence'] == 0
    engine._receive_chunk(data_frame(transfer, b'abcd'), 0, hasher, output)
    assert output.getvalue() == b'abcd' and transfer.offset == 4


def test_receiver_bounds_uncooperative_corruption(tmp_path):
    _, _, transfer, engine = receiver(tmp_path, b'abcd')
    broken = data_frame(transfer, b'abcd')
    broken.payload = broken.payload[:-1] + b'x'
    output, hasher = io.BytesIO(), hashlib.sha256()
    for _ in range(MAX_CHUNK_RETRIES):
        engine._receive_chunk(broken, 0, hasher, output)
    with pytest.raises(TransferError, match='retry budget'):
        engine._receive_chunk(broken, 0, hasher, output)
    assert output.getvalue() == b''


def test_whole_file_mismatch_never_publishes_and_requires_clean_retry(tmp_path):
    _, _, transfer, engine = receiver(tmp_path, b'good')
    engine._queue.put(data_frame(transfer, b'evil'))  # valid chunk hash, wrong file hash
    engine.run()
    assert transfer.state == TransferState.FAILED
    assert transfer._restart_required
    assert not (tmp_path / 'test.bin').exists()


def test_disk_corruption_after_ack_is_detected_before_publication(tmp_path):
    _, _, transfer, engine = receiver(tmp_path, b'abcd')
    def mutate(done, total):
        with open(engine._part_path, 'r+b') as output:
            output.write(b'x')
    engine._progress_cb = mutate
    engine._queue.put(data_frame(transfer, b'abcd'))
    engine.run()
    assert transfer.state == TransferState.FAILED
    assert not (tmp_path / 'test.bin').exists()


def test_resume_hashes_only_confirmed_prefix_and_truncates_tail(tmp_path):
    _, _, transfer, engine = receiver(tmp_path, b'abcd')
    with open(engine._part_path, 'wb') as output:
        output.write(b'abSTALE TAIL')
    engine._resume_offset = 2
    engine._confirmed_offset = 2
    engine._next_sequence = None
    engine._queue.put(data_frame(transfer, b'cd', offset=2))
    engine.run()
    assert transfer.state == TransferState.COMPLETED, transfer.error
    assert (tmp_path / 'test.bin').read_bytes() == b'abcd'


def test_resume_rejects_missing_confirmed_prefix(tmp_path):
    _, _, transfer, engine = receiver(tmp_path, b'abcd')
    engine._resume_offset = 2
    engine.run()
    assert transfer.state == TransferState.FAILED
    assert 'shorter' in transfer.error


def test_source_size_change_fails_before_data(tmp_path):
    session, _, transfer, engine, source = sender(tmp_path)
    source.write_bytes(b'changed size')
    engine.run()
    assert transfer.state == TransferState.FAILED
    assert not any(frame.frame_type == FrameType.TRANSFER_DATA for frame in session.frames)


def test_sender_retries_identical_bytes_not_mutated_source(tmp_path):
    session, _, transfer, engine, source = sender(tmp_path)
    original_send = session.send
    sent = []
    def send(frame):
        original_send(frame)
        if frame.frame_type != FrameType.TRANSFER_DATA:
            return
        sent.append(frame.payload)
        if len(sent) == 1:
            source.write_bytes(b'XXXXXXXX')
            engine._queue.put(Frame.json_frame(FrameType.TRANSFER_RETRY,
                dict(offset=0, end=4, sequence=0), transfer_id=transfer.id))
        elif len(sent) == 2:
            engine._queue.put(Frame.json_frame(FrameType.TRANSFER_ERROR,
                dict(error='stop after observation'), transfer_id=transfer.id))
    session.send = send
    engine.run()
    assert len(sent) == 2 and sent[0] == sent[1]
    assert transfer.state == TransferState.FAILED


def test_ack_timeout_budget_is_finite(tmp_path, monkeypatch):
    session, _, transfer, engine, _ = sender(tmp_path)
    monkeypatch.setattr(engines, 'ACK_TIMEOUT_SECONDS', 0.01)
    engine.run()
    assert transfer.state == TransferState.FAILED and 'ACK timeout' in transfer.error
    assert len([f for f in session.frames if f.frame_type == FrameType.TRANSFER_DATA]) == MAX_CHUNK_RETRIES + 1


@pytest.mark.parametrize('limit', [1, 2, 3])
def test_concurrent_admissions_cannot_oversubscribe(limit, monkeypatch):
    manager = TransferManager(max_concurrent=limit)
    transfers = [manager.create_transfer(TransferDirection.UPLOAD, f'{i}.bin',
        start_immediately=False) for i in range(20)]
    peak = []
    manager.add_listener(lambda transfer, old, new: peak.append(manager.running_count())
                         if new == TransferState.ACTIVE else None)
    # Widen the old snapshot/promote race deterministically. Counting this
    # under several simultaneous passes exceeded the cap before reservations.
    for transfer in transfers:
        original = transfer.set_state
        def delayed(state, error=None, original=original):
            if state == TransferState.ACTIVE:
                time.sleep(0.002)
            return original(state, error)
        monkeypatch.setattr(transfer, 'set_state', delayed)
    gate = threading.Barrier(9)
    workers = [threading.Thread(target=lambda: (gate.wait(), manager.schedule())) for _ in range(8)]
    for worker in workers:
        worker.start()
    gate.wait()
    for worker in workers:
        worker.join(5)
        assert not worker.is_alive()
    assert manager.running_count() == limit and max(peak) == limit
    assert [t for t in transfers if t.state == TransferState.ACTIVE] == transfers[:limit]


def test_explicit_start_cannot_bypass_capacity():
    manager = TransferManager(max_concurrent=1)
    first = manager.create_transfer(TransferDirection.UPLOAD, 'first')
    second = manager.create_transfer(TransferDirection.UPLOAD, 'second')
    with pytest.raises(TransferStateError):
        manager.start_transfer(second.id)
    assert first.state == TransferState.ACTIVE and second.state == TransferState.QUEUED


@pytest.mark.parametrize('limit', [0, -1, True, 1.5, '3'])
def test_concurrency_limit_must_be_a_positive_integer(limit):
    with pytest.raises(ValueError):
        TransferManager(max_concurrent=limit)


def test_creation_listener_can_query_manager_without_deadlock():
    manager = TransferManager()
    observed = []
    manager.add_listener(lambda transfer, old, new: observed.append(
        (manager.running_count(), len(manager.all_transfers()))))
    worker = threading.Thread(target=lambda: manager.create_transfer(TransferDirection.UPLOAD, 'file'), daemon=True)
    worker.start(); worker.join(2)
    assert not worker.is_alive()
    assert observed[0] == (0, 1) and observed[-1] == (1, 1)


def test_resume_queues_until_slot_available():
    manager = TransferManager(max_concurrent=1)
    first = manager.create_transfer(TransferDirection.UPLOAD, 'first')
    manager.pause_transfer(first.id)
    manager.confirm_paused(first.id)
    second = manager.create_transfer(TransferDirection.UPLOAD, 'second')
    manager.resume_transfer(first.id)
    assert first.state == TransferState.PAUSED and first.resume_requested
    assert manager.running_count() == 1
    manager.cancel_transfer(second.id)
    wait(lambda: first.state == TransferState.ACTIVE)
    assert not first.resume_requested and manager.running_count() == 1


def test_queued_pause_never_starts_engine_or_create(tmp_path):
    session, manager = RecordingSession(), TransferManager(max_concurrent=1)
    manager.create_transfer(TransferDirection.UPLOAD, 'blocker')
    scheduler = ConcurrentScheduler(session, manager, FrameRouter(session), dest_dir=str(tmp_path))
    source = tmp_path / 'file.bin'; source.write_bytes(b'data')
    transfer = scheduler.submit_upload(str(source))
    scheduler.pause(transfer.id)
    assert transfer.state == TransferState.PAUSED and transfer.started_at is None
    assert not session.frames and scheduler._jobs[transfer.id].thread is None


def test_retry_event_has_reset_progress_error_and_timing():
    manager = TransferManager()
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'file', 10)
    transfer.update_progress(4)
    manager.fail_transfer(transfer.id, 'failed')
    observed = []
    manager.add_listener(lambda item, old, new: observed.append(item.to_dict())
                         if new == TransferState.RETRYING else None)
    manager.retry_transfer(transfer.id)
    assert observed[0]['offset'] == 0 and observed[0]['error'] is None
    assert observed[0]['ended_at'] is None and observed[0]['attempt'] == 1


def test_invalid_retry_cannot_erase_active_job(tmp_path):
    session, manager = RecordingSession(), TransferManager()
    scheduler = ConcurrentScheduler(session, manager, FrameRouter(session), dest_dir=str(tmp_path))
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'file')
    marker = object()
    scheduler._jobs[transfer.id] = _Job(build=lambda: None, engine=marker, initiator=True)
    with pytest.raises(TransferStateError):
        scheduler.retry(transfer.id)
    assert scheduler._jobs[transfer.id].engine is marker


@pytest.fixture
def peers(tmp_path):
    client, server = _make_peers(tmp_path)
    yield client, server
    client.teardown(); server.teardown()


def test_lost_ack_retransmits_only_outstanding_chunk(peers, tmp_path, monkeypatch):
    client, server = peers
    monkeypatch.setattr(engines, 'ACK_TIMEOUT_SECONDS', 0.1)
    original_send = server.session.send
    dropped = []
    def send(frame):
        if frame.frame_type == FrameType.TRANSFER_ACK and frame.sequence == 1 and not dropped:
            dropped.append(frame)
            return
        original_send(frame)
    monkeypatch.setattr(server.session, 'send', send)
    source = tmp_path / 'lost-ack.bin'; source.write_bytes(b'abcdefgh' * 1024)
    transfer = client.scheduler.submit_upload(str(source), chunk_size=1024)
    final = _wait_for_state(client.manager, transfer.id, {TransferState.COMPLETED, TransferState.FAILED})
    assert final.state == TransferState.COMPLETED, final.error
    assert len(dropped) == 1
    assert (tmp_path / 'server_files' / source.name).read_bytes() == source.read_bytes()


@pytest.mark.parametrize('direction', ['upload', 'download'])
def test_failed_transfer_can_really_retry_on_same_session(peers, tmp_path, direction):
    client, server = peers
    source = tmp_path / ('retry-upload.bin' if direction == 'upload' else 'server_files/retry-download.bin')
    source.write_bytes(b'abcdefgh' * 1024)
    sending_session = client.session if direction == 'upload' else server.session
    corruption = _corrupt_data_frames(sending_session, target_seq=2, times=999)
    transfer = (client.scheduler.submit_upload(str(source), chunk_size=1024) if direction == 'upload'
                else client.scheduler.submit_download(source.name, chunk_size=1024))
    failed = _wait_for_state(client.manager, transfer.id, {TransferState.FAILED})
    # Retry immediately on the initiating side; do not wait for the remote
    # worker to settle. The retry handshake must supply that ordering itself.
    corruption['remaining'] = 0
    client.scheduler.retry(transfer.id)
    final = _wait_for_state(client.manager, transfer.id, {TransferState.COMPLETED, TransferState.FAILED})
    assert final.state == TransferState.COMPLETED, final.error
    dest = tmp_path / ('server_files' if direction == 'upload' else 'client_files') / source.name
    assert dest.read_bytes() == source.read_bytes()
    assert final.attempt == 1 and final.ended_at is not None
    assert not client.session.closed and not server.session.closed


def test_whole_file_failure_retry_restarts_bad_partial(peers, tmp_path, monkeypatch):
    client, server = peers
    source = tmp_path / 'whole.bin'; source.write_bytes(b'abcdefgh' * 512)
    original_send = client.session.send
    corrupt = [True]
    def send(frame):
        if frame.frame_type == FrameType.TRANSFER_DATA and corrupt[0]:
            # Recompute chunk hash too, so only the whole-file check catches it.
            offset, chunk, _ = engines._decode_data_payload(frame.payload)
            frame.payload = _encode_data_payload(offset, b'x' * len(chunk))
        original_send(frame)
    monkeypatch.setattr(client.session, 'send', send)
    transfer = client.scheduler.submit_upload(str(source), chunk_size=1024)
    _wait_for_state(client.manager, transfer.id, {TransferState.FAILED})
    assert not (tmp_path / 'server_files' / source.name).exists()
    corrupt[0] = False
    client.scheduler.retry(transfer.id)
    final = _wait_for_state(client.manager, transfer.id, {TransferState.COMPLETED, TransferState.FAILED})
    assert final.state == TransferState.COMPLETED, final.error
    assert (tmp_path / 'server_files' / source.name).read_bytes() == source.read_bytes()


def test_paused_engine_resume_obeys_limit_while_another_transfer_runs(peers, tmp_path, monkeypatch):
    client, server = peers
    client.manager.max_concurrent = server.manager.max_concurrent = 1
    original_send = client.session.send
    entered = {name: threading.Event() for name in ('first', 'second')}
    release = {name: threading.Event() for name in ('first', 'second')}
    ordinal = []
    def send(frame):
        if frame.frame_type == FrameType.TRANSFER_DATA and frame.sequence == 0:
            if frame.transfer_id not in ordinal:
                ordinal.append(frame.transfer_id)
                name = 'first' if len(ordinal) == 1 else 'second'
                entered[name].set()
                if not release[name].wait(5):
                    raise RuntimeError('test data gate timed out')
        original_send(frame)
    monkeypatch.setattr(client.session, 'send', send)
    first_source = tmp_path / 'first.bin'; first_source.write_bytes(b'abcd' * 2048)
    second_source = tmp_path / 'second.bin'; second_source.write_bytes(b'efgh' * 1024)
    first = client.scheduler.submit_upload(str(first_source), chunk_size=1024)
    try:
        assert entered['first'].wait(2)
        client.scheduler.pause(first.id)
        release['first'].set()
        wait(lambda: first.state == TransferState.PAUSED)
        wait(lambda: server.manager.get(first.id).state == TransferState.PAUSED)
        second = client.scheduler.submit_upload(str(second_source), chunk_size=1024)
        assert entered['second'].wait(2)
        client.scheduler.resume(first.id)
        time.sleep(0.1)
        assert first.state == TransferState.PAUSED
        assert client.manager.running_count() == server.manager.running_count() == 1
        release['second'].set()
        for transfer in (first, second):
            final = _wait_for_state(client.manager, transfer.id, {TransferState.COMPLETED, TransferState.FAILED})
            assert final.state == TransferState.COMPLETED, final.error
        assert (tmp_path / 'server_files/first.bin').read_bytes() == first_source.read_bytes()
        assert (tmp_path / 'server_files/second.bin').read_bytes() == second_source.read_bytes()
    finally:
        release['first'].set(); release['second'].set()


def test_retry_waits_for_old_worker_before_rearming(tmp_path):
    session, manager = RecordingSession(), TransferManager(max_concurrent=1)
    scheduler = ConcurrentScheduler(session, manager, FrameRouter(session), dest_dir=str(tmp_path))
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'file')
    manager.fail_transfer(transfer.id, 'old attempt failed')
    release, finished = threading.Event(), threading.Event()
    transfer.io_finished.clear()
    def old_worker():
        release.wait(2)
        transfer.io_finished.set()
    old = threading.Thread(target=old_worker)
    old.start()
    scheduler._jobs[transfer.id] = _Job(build=lambda: None, thread=old, initiator=True)
    manager.create_transfer(TransferDirection.UPLOAD, 'blocker')
    errors = []
    def retry():
        try:
            scheduler.retry(transfer.id)
        except Exception as error:
            errors.append(error)
        finally:
            finished.set()
    worker = threading.Thread(target=retry)
    worker.start()
    try:
        assert not finished.wait(0.05)
        assert scheduler._jobs[transfer.id].thread is old
        release.set()
        assert finished.wait(2) and not errors
        assert transfer.state == TransferState.RETRYING
        assert scheduler._jobs[transfer.id].thread is None
    finally:
        release.set(); old.join(2); worker.join(2)


def test_passive_transfer_retry_is_rejected_without_altering_job(tmp_path):
    session, manager = RecordingSession(), TransferManager()
    scheduler = ConcurrentScheduler(session, manager, FrameRouter(session), dest_dir=str(tmp_path))
    transfer = manager.create_transfer(TransferDirection.DOWNLOAD, 'file')
    manager.fail_transfer(transfer.id, 'peer failed')
    job = _Job(build=lambda: None, engine=object(), initiator=False)
    scheduler._jobs[transfer.id] = job
    with pytest.raises(TransferStateError, match='initiating peer'):
        scheduler.retry(transfer.id)
    assert transfer.state == TransferState.FAILED and scheduler._jobs[transfer.id] is job


@pytest.mark.parametrize('content', [b'', b'x', b'x' * 4096, b'x' * 4097])
def test_whole_file_integrity_boundaries(tmp_path, content):
    _, _, transfer, engine = receiver(tmp_path, content)
    transfer.chunk_size = max(4096, len(content))
    engine._queue.put(data_frame(transfer, content))
    engine.run()
    assert transfer.state == TransferState.COMPLETED, transfer.error
    assert transfer.checksum == hashlib.sha256(content).hexdigest()
    assert (tmp_path / 'test.bin').read_bytes() == content


def test_receiver_cannot_complete_without_expected_whole_file_hash(tmp_path):
    _, _, transfer, engine = receiver(tmp_path, b'abcd')
    transfer.checksum = None
    engine.run()
    assert transfer.state == TransferState.FAILED
    assert not (tmp_path / 'test.bin').exists()


def test_source_same_size_mutation_cannot_report_success(tmp_path):
    session, _, transfer, engine, source = sender(tmp_path, b'abcd')
    original_send = session.send
    def send(frame):
        original_send(frame)
        if frame.frame_type == FrameType.TRANSFER_DATA:
            source.write_bytes(b'xxxx')
            engine._queue.put(Frame.json_frame(FrameType.TRANSFER_ACK,
                dict(offset=4), transfer_id=transfer.id, sequence=0))
            engine._queue.put(Frame.json_frame(FrameType.TRANSFER_COMPLETE,
                dict(sha256=transfer.checksum, size=4), transfer_id=transfer.id))
    session.send = send
    engine.run()
    assert transfer.state == TransferState.FAILED and 'Source checksum changed' in transfer.error


def test_retry_handshake_discards_old_attempt_replies(tmp_path):
    from filetransfer.engine import send_transfer_create
    session, _, transfer, engine, _ = sender(tmp_path)
    transfer.attempt = 1
    router = FrameRouter(session)
    original_send_json = session.send_json
    def send_json(kind, payload, **kwargs):
        original_send_json(kind, payload, **kwargs)
        if kind == FrameType.TRANSFER_CREATE:
            for frame in (
                Frame.json_frame(FrameType.TRANSFER_ERROR, dict(error='old attempt'), transfer_id=transfer.id),
                Frame.json_frame(FrameType.TRANSFER_ACK, dict(offset=4), transfer_id=transfer.id),
                Frame.json_frame(FrameType.TRANSFER_ACCEPT, dict(resume_offset=0, create_id='old'), transfer_id=transfer.id),
                Frame.json_frame(FrameType.TRANSFER_ACCEPT, dict(resume_offset=4, create_id=payload['create_id']), transfer_id=transfer.id),
            ):
                router.dispatch(frame)
    session.send_json = send_json
    assert send_transfer_create(session, transfer, router, chunk_size=4) == 4


@pytest.mark.parametrize('lost', [True, False])
def test_restore_dormant_transfer_does_not_consume_busy_slot(lost):
    from filetransfer.persistence import restore_transfer
    manager = TransferManager(max_concurrent=1)
    live = manager.create_transfer(TransferDirection.UPLOAD, 'live')
    restored = restore_transfer(dict(id='a' * 32, direction='upload', filename='restored',
        total_size=4, offset=2, checksum=hashlib.sha256(b'abcd').hexdigest(), state='active'),
        manager, connection_lost=lost)
    assert restored.state == (TransferState.CONNECTION_LOST if lost else TransferState.PAUSED)
    assert restored.offset == 2 and restored.ready
    assert manager.running_count() == 1 and live.state == TransferState.ACTIVE


def test_manager_failure_stops_attached_worker_without_cancelling_transfer(tmp_path):
    session, manager = RecordingSession(), TransferManager()
    scheduler = ConcurrentScheduler(session, manager, FrameRouter(session), dest_dir=str(tmp_path))
    transfer = manager.create_transfer(TransferDirection.DOWNLOAD, 'file')
    worker = SimpleNamespace(_cancel_event=threading.Event(), _resume_event=threading.Event())
    scheduler._jobs[transfer.id] = _Job(build=lambda: worker, engine=worker, initiator=True)
    manager.fail_transfer(transfer.id, 'administrative stop')
    assert worker._cancel_event.is_set() and worker._resume_event.is_set()
    assert transfer.state == TransferState.FAILED and manager.running_count() == 0
