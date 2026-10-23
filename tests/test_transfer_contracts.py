"""regression contracts; no OS networking is required.

Session/engine integration uses the actual frame codec over a deterministic
byte stream. Native TCP/TLS and browser integration remain separate tests.
"""
from helpers.protocol import (RecordingSession, create, data_frame, inline, receiver)
import hashlib
import queue
import struct
import threading
from types import SimpleNamespace

import pytest

from filetransfer.engine import (FrameRouter, RecvEngine, SendEngine, TransferError,
    _encode_data_payload, recv_transfer_create, send_transfer_create)
from filetransfer.manager import TransferManager
from filetransfer.session_protocol import ALL_FEATURES, Feature, NegotiationError, Session
from filetransfer.scheduler import ConcurrentScheduler
from filetransfer.transfer import TransferDirection, TransferState
from filetransfer.transferfiles import partial_path
from filetransfer.transport import (Frame, FrameError, FrameFlags, FrameReader,
    FrameType, HEADER_SIZE, MAGIC, VERSION, _STRUCT_HEADER, recv_frame, send_frame)
from memory_transport import socketpair


class Stream:
    def __init__(self, actions):
        self.actions = list(actions)
    def recv_into(self, target, length):
        if not self.actions:
            return 0
        item = self.actions.pop(0)
        if isinstance(item, Exception):
            raise item
        part = item[:length]
        target[:len(part)] = part
        if len(item) > length:
            self.actions.insert(0, item[length:])
        return len(part)


def packet(kind=FrameType.TRANSFER_DATA, payload=b'payload', **kw):
    fields = dict(magic=MAGIC, version=VERSION, kind=kind, flags=0,
                  reserved=0, tid=b'a' * 32, sequence=0, size=len(payload))
    fields.update(kw)
    return _STRUCT_HEADER.pack(*fields.values()) + payload


@pytest.mark.parametrize('boundary', [1, 12, 47, 48, 49, 54])
def test_timeout_preserves_partial_header_and_payload(boundary):
    raw = packet()
    stream = Stream([raw[:boundary], TimeoutError(), raw[boundary:], packet(FrameType.TRANSFER_ACK)])
    with pytest.raises(TimeoutError):
        recv_frame(stream)
    assert recv_frame(stream).payload == b'payload'
    assert recv_frame(stream).frame_type == FrameType.TRANSFER_ACK


@pytest.mark.parametrize('changes', [dict(magic=0), dict(version=99), dict(kind=255),
    dict(flags=128), dict(reserved=1), dict(tid=b'z' * 32), dict(size=64 * 1024 * 1024 + 1)])
def test_invalid_header_poisoned_decoder(changes):
    reader = FrameReader()
    stream = Stream([packet(**changes)])
    with pytest.raises(FrameError):
        reader.recv(stream)
    with pytest.raises(FrameError, match='unusable'):
        reader.recv(Stream([packet()]))


@pytest.mark.parametrize('payload', [b'[]', b'null', b'42', b'"text"'])
def test_json_control_frames_require_objects(payload):
    with pytest.raises(FrameError):
        Frame(FrameType.HELLO, payload).json()


@pytest.mark.parametrize('client_versions,server_versions,selected', [
    ((2, 3), (2, 3), 3), ((2,), (2, 3), 2), ((2, 3), (2,), 2), ((3,), (2, 3), 3)])
def test_highest_common_version_and_actual_wire_version(client_versions, server_versions, selected):
    a, b = socketpair()
    client = Session(a, ('memory', 1), supported_versions=client_versions)
    server = Session(b, ('memory', 2), supported_versions=server_versions)
    errors = []
    def negotiate():
        try:
            server.negotiate_as_server()
        except Exception as exc:
            errors.append(exc)
    th = threading.Thread(target=negotiate)
    th.start()
    try:
        client.negotiate_as_client()
        th.join(2)
        assert not errors and not th.is_alive()
        assert client.negotiation.protocol_version == server.negotiation.protocol_version == selected
        client.mark_authenticated()
        server.mark_authenticated()
        client.send(Frame(FrameType.PING))
        assert server.recv().version == selected
        assert Feature.COMPRESSION not in ALL_FEATURES
    finally:
        a.close(); b.close()


def test_no_common_version_rejected_on_both_sides():
    a, b = socketpair()
    client, server = Session(a, ('memory', 1), supported_versions=(3,)), Session(b, ('memory', 2), supported_versions=(2,))
    errors = []
    def responder():
        try:
            server.negotiate_as_server()
        except NegotiationError as exc:
            errors.append(exc)
    th = threading.Thread(target=responder)
    th.start()
    try:
        with pytest.raises(NegotiationError, match='mismatch'):
            client.negotiate_as_client()
        th.join(2)
        assert len(errors) == 1
    finally:
        a.close(); b.close()


def test_bootstrap_hello_ack_and_feature_intersection():
    a, b = socketpair()
    server = Session(b, ('memory', 2))
    send_frame(a, Frame.json_frame(FrameType.HELLO,
        {'version': 2, 'features': ['cancel', 'compression', 'unknown']}))
    try:
        assert server.negotiate_as_server().protocol_version == 2
        ack = recv_frame(a)
        assert ack.version == 2 and ack.json()['features'] == ['cancel']
    finally:
        a.close(); b.close()






@pytest.mark.parametrize('changes', [dict(size=-1), dict(size=True), dict(size='4'),
    dict(direction='sideways'), dict(chunk_size=0), dict(chunk_size=9 * 1024 * 1024),
    dict(overwrite='yes'), dict(checksum='bad'), dict(checksum=None),
    dict(filename=[]), dict(filename='bad\x00name'), dict(have_offset=-1)])
def test_malformed_create_rejected_before_registering(tmp_path, changes):
    session, manager = RecordingSession(), TransferManager()
    router = FrameRouter(session)
    with pytest.raises(TransferError):
        create(session, manager, router, tmp_path, **changes)
    assert manager.all_transfers() == []
    assert not list(tmp_path.iterdir())
    assert session.frames[-1].frame_type == FrameType.TRANSFER_REJECT


def test_busy_destination_rejected_across_peer_managers_and_parts_are_unique(tmp_path):
    session = RecordingSession()
    first_manager, second_manager = TransferManager(), TransferManager()
    first, _, path = create(session, first_manager, FrameRouter(session), tmp_path)
    with pytest.raises(TransferError, match='busy'):
        create(session, second_manager, FrameRouter(session), tmp_path, tid='b' * 32)
    first_manager.cancel_transfer(first.id)
    second, _, second_path = create(session, second_manager, FrameRouter(session), tmp_path, tid='b' * 32)
    assert path != second_path
    assert path == partial_path(str(tmp_path / 'test.bin'), first.id)
    assert second.state == TransferState.ACTIVE


def test_pull_accept_contains_size_and_checksum(tmp_path):
    (tmp_path / 'test.bin').write_bytes(b'data')
    session, manager = RecordingSession(), TransferManager()
    create(session, manager, FrameRouter(session), tmp_path, direction='download')
    accept = session.frames[-1].json()
    assert accept['size'] == 4
    assert accept['checksum'] == hashlib.sha256(b'data').hexdigest()






@pytest.mark.parametrize('declared,actual', [(1, 1024), (100, 3)])
def test_false_lengths_never_acknowledged_or_published(tmp_path, declared, actual):
    session, _, transfer, engine = receiver(tmp_path, b'x' * actual, declared=declared)
    engine._queue.put(data_frame(transfer, b'x' * actual))
    engine.run()
    assert transfer.state == TransferState.FAILED
    assert not (tmp_path / 'test.bin').exists()
    assert not any(f.frame_type == FrameType.TRANSFER_ACK for f in session.frames)


def test_duplicate_chunk_is_acked_without_rewriting(tmp_path):
    session, _, transfer, engine = receiver(tmp_path, b'ab')
    engine._queue.put(data_frame(transfer, b'a', final=False))
    engine._queue.put(data_frame(transfer, b'a', final=False))
    engine._queue.put(data_frame(transfer, b'b', offset=1, sequence=1))
    engine.run()
    assert transfer.state == TransferState.COMPLETED
    assert (tmp_path / 'test.bin').read_bytes() == b'ab'
    acks = [f for f in session.frames if f.frame_type == FrameType.TRANSFER_ACK]
    assert [(f.sequence, f.json()['offset']) for f in acks] == [(0, 1), (0, 1), (1, 2)]


@pytest.mark.parametrize('offset', [-1, True, '4', 5])
def test_sender_rejects_invalid_ack_offsets(tmp_path, offset):
    source = tmp_path / 'source.bin'; source.write_bytes(b'data')
    session, manager = RecordingSession(), TransferManager()
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'source.bin', 4, hashlib.sha256(b'data').hexdigest())
    inbox = queue.Queue()
    inbox.put(Frame.json_frame(FrameType.TRANSFER_ACK, {'offset': offset}, transfer_id=transfer.id))
    engine = SendEngine(session=session, transfer=transfer, manager=manager, frame_queue=inbox, file_path=str(source))
    with pytest.raises(TransferError):
        engine._wait_for_ack(transfer.id, 0, 4)


def test_stale_ack_does_not_advance_outstanding_chunk(tmp_path):
    source = tmp_path / 'source.bin'; source.write_bytes(b'data')
    session, manager = RecordingSession(), TransferManager()
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'source.bin', 4)
    inbox = queue.Queue()
    for offset in (0, 2, 4):
        inbox.put(Frame.json_frame(FrameType.TRANSFER_ACK, {'offset': offset}, transfer_id=transfer.id))
    engine = SendEngine(session=session, transfer=transfer, manager=manager, frame_queue=inbox, file_path=str(source))
    assert engine._wait_for_ack(transfer.id, 0, 4) == (4, False, False)


@pytest.mark.parametrize('cancel_first', [True, False])
def test_cancel_vs_complete_has_one_terminal_outcome_and_no_late_publish(tmp_path, cancel_first):
    _, _, transfer, engine = receiver(tmp_path, b'data')
    engine._queue.put(data_frame(transfer, b'data'))
    if cancel_first:
        engine.cancel()
        engine.run()
        assert transfer.state == TransferState.CANCELLED
        assert not (tmp_path / 'test.bin').exists()
    else:
        engine.run()
        engine.cancel()
        assert transfer.state == TransferState.COMPLETED
        assert (tmp_path / 'test.bin').read_bytes() == b'data'
    assert not __import__('os').path.exists(engine._part_path)


def test_queued_inbound_cancel_is_immediate_and_does_not_cancel_another_transfer(tmp_path):
    session, manager = RecordingSession(), TransferManager(max_concurrent=1)
    blocker = manager.create_transfer(TransferDirection.UPLOAD, 'blocker.bin', 4)
    router = FrameRouter(session)
    scheduler = ConcurrentScheduler(session, manager, router, dest_dir=str(tmp_path))
    frame = Frame.json_frame(FrameType.TRANSFER_CREATE,
        dict(filename='queued.bin', size=4, direction='upload',
             checksum=hashlib.sha256(b'data').hexdigest()), transfer_id='b' * 32)
    scheduler._handle_inbound_create(frame)
    target = manager.get('b' * 32)
    assert target.state == TransferState.QUEUED
    router.dispatch(Frame.json_frame(FrameType.TRANSFER_CANCEL, {}, transfer_id=target.id))
    assert target.state == TransferState.CANCELLED
    assert blocker.state == TransferState.ACTIVE and not session.closed
    assert any(f.frame_type == FrameType.TRANSFER_CANCEL and f.json().get('acknowledged') for f in session.frames)


def test_cancel_before_create_is_not_lost():
    session = RecordingSession()
    router = FrameRouter(session)
    router.dispatch(Frame.json_frame(FrameType.TRANSFER_CANCEL, {}, transfer_id='a' * 32))
    router.dispatch(Frame.json_frame(FrameType.TRANSFER_CREATE, {}, transfer_id='a' * 32))
    assert router.next_create(timeout=0) is None
    assert session.frames[-1].frame_type == FrameType.TRANSFER_REJECT


def test_cancel_adapter_preserves_connection(monkeypatch):
    from webapp import api
    calls = []
    link = SimpleNamespace(scheduler=SimpleNamespace(cancel_all=lambda: calls.append('cancel') or 2))
    monkeypatch.setattr(api, 'client_state', {'link': link})
    assert api.client_cancel_transfer() == {'ok': True, 'count': 2}
    assert api.client_state['link'] is link and calls == ['cancel']




@pytest.fixture
def staging(tmp_path, monkeypatch):
    from webapp import api
    class Executor:
        async def run_in_executor(self, executor, fn):
            return fn()
    class Request:
        def __init__(self, data):
            self.data = data
        async def body(self):
            return self.data
    monkeypatch.setattr(api.asyncio, 'get_event_loop', lambda: Executor())
    monkeypatch.setattr(api, '_UPLOAD_SCRATCH_DIR', str(tmp_path / 'scratch'))
    monkeypatch.setattr(api, '_upload_sessions', {})
    monkeypatch.setattr(api, '_upload_transfers', {})
    monkeypatch.setattr(api, '_cancelled_uploads', {})
    return api, Request


@pytest.mark.parametrize('upload_id', ['../escape', '..\\escape', 'C:\\escape', '', 'a/b'])
def test_upload_ids_cannot_escape_scratch(staging, upload_id):
    api, Request = staging
    with pytest.raises(ValueError, match='upload_id'):
        inline(api._stage_browser_chunk(Request(b'x'), upload_id, 'safe.bin', 1, 0, False, False))


def test_staging_validates_order_size_and_cancel_tombstones(staging):
    api, Request = staging
    inline(api._stage_browser_chunk(Request(b'ab'), 'test-id', 'safe.bin', 4, 0, False, False))
    with pytest.raises(ValueError, match='order'):
        inline(api._stage_browser_chunk(Request(b'cd'), 'test-id', 'safe.bin', 4, 0, True, False))
    with pytest.raises(ValueError, match='size'):
        inline(api._stage_browser_chunk(Request(b'c'), 'test-id', 'safe.bin', 4, 1, True, False))
    assert inline(api.client_cancel_upload_stream('test-id'))['ok']
    with pytest.raises(ValueError, match='cancelled'):
        inline(api._stage_browser_chunk(Request(b'cd'), 'test-id', 'safe.bin', 4, 1, True, False))


def test_stream_cancel_routes_to_only_its_submitted_transfer(staging):
    api, _ = staging
    calls = []
    transfer = SimpleNamespace(id='a' * 32)
    link = SimpleNamespace(scheduler=SimpleNamespace(cancel=lambda tid: calls.append(tid)))
    api._upload_transfers['upload-id'] = (link, transfer)
    assert inline(api.client_cancel_upload_stream('upload-id'))['ok']
    assert calls == [transfer.id]


def test_accept_is_sent_before_any_job_can_start(tmp_path):
    session, manager = RecordingSession(), TransferManager()
    events = []
    original = session.send_json
    def send(kind, payload, **kw):
        events.append(kind.name)
        original(kind, payload, **kw)
    session.send_json = send
    manager.add_listener(lambda t, old, new: events.append('ACTIVE') if new == TransferState.ACTIVE else None)
    create(session, manager, FrameRouter(session), tmp_path)
    assert events == ['TRANSFER_ACCEPT', 'ACTIVE']


def test_accept_send_failure_rolls_back_reservation_and_registration(tmp_path):
    session, manager = RecordingSession(), TransferManager()
    router = FrameRouter(session)
    original = session.send_json
    def send(kind, payload, **kw):
        if kind == FrameType.TRANSFER_ACCEPT:
            raise OSError('connection lost during ACCEPT')
        original(kind, payload, **kw)
    session.send_json = send
    with pytest.raises(OSError):
        create(session, manager, router, tmp_path)
    assert manager.all_transfers() == []
    assert router.get_queue('a' * 32) is None
    session.send_json = original
    assert create(session, manager, router, tmp_path)[0].state == TransferState.ACTIVE


def test_duplicate_create_reaches_rejection_instead_of_active_data_queue(tmp_path):
    session, manager = RecordingSession(), TransferManager()
    router = FrameRouter(session)
    transfer, _, _ = create(session, manager, router, tmp_path)
    frame = Frame.json_frame(FrameType.TRANSFER_CREATE, {'filename': 'test.bin',
        'size': 4, 'direction': 'upload', 'checksum': hashlib.sha256(b'data').hexdigest()}, transfer_id=transfer.id)
    router.dispatch(frame)
    assert router.next_create(timeout=0) is frame
    assert router.get_queue(transfer.id).empty()
    with pytest.raises(TransferError, match='cannot be recovered'):
        recv_transfer_create(session, frame, str(tmp_path), manager, router)
    assert manager.get(transfer.id) is transfer


@pytest.mark.parametrize('accepted_offset', [-1, True, '0', 5])
def test_create_initiator_rejects_invalid_resume_offsets(accepted_offset):
    session, manager = RecordingSession(), TransferManager()
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'source.bin', 4)
    router = FrameRouter(session)
    def send(kind, payload, **kw):
        router.dispatch(Frame.json_frame(FrameType.TRANSFER_ACCEPT,
            {'resume_offset': accepted_offset}, transfer_id=transfer.id))
    session.send_json = send
    with pytest.raises(TransferError):
        send_transfer_create(session, transfer, router)
    assert router.get_queue(transfer.id) is None


@pytest.mark.parametrize('features,version,versions', [(['compression'], 2, [2]),
    (['cancel'], 2, [2, 3]), (['cancel'], 99, [99])])
def test_client_rejects_unoffered_features_and_invalid_version_selection(features, version, versions):
    a, b = socketpair()
    send_frame(b, Frame.json_frame(FrameType.HELLO_ACK,
        {'features': features, 'version': version, 'versions': versions}))
    client = Session(a, ('memory', 1))
    try:
        with pytest.raises(NegotiationError):
            client.negotiate_as_client(offered_features=frozenset({'cancel'}))
        assert client.negotiation is None
    finally:
        a.close(); b.close()


def test_unnegotiated_pause_rejected_without_mutating_transfer(tmp_path):
    from filetransfer.session_protocol import NegotiatedSession
    session, manager = RecordingSession(), TransferManager()
    session.negotiation = NegotiatedSession(3, frozenset())
    session.supports = lambda name: False
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'source.bin', 4)
    scheduler = ConcurrentScheduler(session, manager, FrameRouter(session), dest_dir=str(tmp_path))
    from filetransfer.transfer import TransferStateError
    with pytest.raises(TransferStateError, match='negotiate'):
        scheduler.pause(transfer.id)
    assert transfer.state == TransferState.ACTIVE


def test_sender_does_not_overwrite_expected_digest_with_false_completion(tmp_path):
    source = tmp_path / 'source.bin'; source.write_bytes(b'data')
    session, manager = RecordingSession(), TransferManager()
    expected = hashlib.sha256(b'data').hexdigest()
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'source.bin', 4, expected)
    inbox = queue.Queue()
    inbox.put(Frame.json_frame(FrameType.TRANSFER_ACK, {'offset': 4}, transfer_id=transfer.id))
    inbox.put(Frame.json_frame(FrameType.TRANSFER_COMPLETE, {'sha256': '0' * 64}, transfer_id=transfer.id))
    SendEngine(session=session, transfer=transfer, manager=manager,
        frame_queue=inbox, file_path=str(source)).run()
    assert transfer.state == TransferState.FAILED
    assert transfer.checksum == expected


def test_v3_ack_sequence_must_correspond_to_the_outstanding_chunk(tmp_path):
    from filetransfer.session_protocol import NegotiatedSession
    source = tmp_path / 'source.bin'; source.write_bytes(b'data')
    session, manager = RecordingSession(), TransferManager()
    session.negotiation = NegotiatedSession(3, ALL_FEATURES)
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'source.bin', 4)
    inbox = queue.Queue()
    inbox.put(Frame.json_frame(FrameType.TRANSFER_ACK, {'offset': 4},
                               transfer_id=transfer.id, sequence=123))
    engine = SendEngine(session=session, transfer=transfer, manager=manager,
        frame_queue=inbox, file_path=str(source))
    with pytest.raises(TransferError, match='sequence'):
        engine._wait_for_ack(transfer.id, 0, 4)


@pytest.mark.parametrize('trial', range(15))
def test_real_cancel_complete_race_never_publishes_a_cancelled_receiver(tmp_path, trial):
    _, _, transfer, engine = receiver(tmp_path, b'data')
    engine._queue.put(data_frame(transfer, b'data'))
    start = threading.Barrier(2)
    def run():
        start.wait()
        engine.run()
    th = threading.Thread(target=run)
    th.start()
    start.wait()
    engine.cancel()
    th.join(2)
    assert not th.is_alive()
    if transfer.state == TransferState.CANCELLED:
        assert not (tmp_path / 'test.bin').exists()
    else:
        assert transfer.state == TransferState.COMPLETED
        assert (tmp_path / 'test.bin').read_bytes() == b'data'
    assert not __import__('os').path.exists(engine._part_path)


def test_partial_frame_has_a_bounded_assembly_deadline(monkeypatch):
    from filetransfer import transport
    now = [0.0]
    monkeypatch.setattr(transport.time, 'monotonic', lambda: now[0])
    reader = FrameReader(frame_timeout=2)
    stream = Stream([packet()[:1], TimeoutError()])
    with pytest.raises(TimeoutError):
        reader.recv(stream)
    now[0] = 3
    with pytest.raises(FrameError, match='timed out'):
        reader.recv(stream)


def test_control_frame_length_is_rejected_before_reading_body():
    stream = Stream([packet(FrameType.HELLO, payload=b'', size=65537)])
    with pytest.raises(FrameError, match='Control payload too large'):
        recv_frame(stream)


def test_browser_body_abort_discards_only_its_staging_and_tombstones_id(staging):
    api, Request = staging
    inline(api._stage_browser_chunk(Request(b'ab'), 'aborted', 'safe.bin', 4, 0, False, False))
    class AbortedRequest:
        async def body(self):
            raise ConnectionError('browser aborted body')
    with pytest.raises(api.BrowserUploadAborted):
        inline(api._stage_browser_chunk(AbortedRequest(), 'aborted', 'safe.bin', 4, 1, True, False))
    assert 'aborted' not in api._upload_sessions
    assert 'aborted' in api._cancelled_uploads
    assert not __import__('os').path.exists(api._scratch_path('aborted'))


def test_cancel_all_includes_staging_and_prevents_late_recreation(staging, monkeypatch):
    api, Request = staging
    inline(api._stage_browser_chunk(Request(b'ab'), 'staged', 'safe.bin', 4, 0, False, False))
    link = SimpleNamespace(scheduler=SimpleNamespace(cancel_all=lambda: 0))
    monkeypatch.setattr(api, 'client_state', {'link': link})
    assert api.client_cancel_transfer() == {'ok': True, 'count': 1}
    with pytest.raises(ValueError, match='cancelled'):
        inline(api._stage_browser_chunk(Request(b'abcd'), 'staged', 'safe.bin', 4, 0, True, False))


def test_late_manager_completion_does_not_mutate_cancelled_metadata():
    manager = TransferManager()
    transfer = manager.create_transfer(TransferDirection.UPLOAD, 'cancelled.bin', 4, 'a' * 64)
    manager.cancel_transfer(transfer.id)
    assert manager.complete_transfer(transfer.id, checksum='b' * 64) is False
    assert transfer.state == TransferState.CANCELLED and transfer.checksum == 'a' * 64


def test_partial_names_are_bounded_and_distinguish_equal_ids_for_different_files(tmp_path):
    first = partial_path(str(tmp_path / ('x' * 250)), 'a' * 32)
    second = partial_path(str(tmp_path / 'different.bin'), 'a' * 32)
    assert first != second
    assert len(__import__('os').path.basename(first)) < 255


@pytest.mark.parametrize('handler', ['client_upload_stream', 'create_upload_transfer_stream'])
def test_both_upload_routes_report_body_abort_without_disconnect(staging, monkeypatch, handler):
    api, _ = staging
    link = SimpleNamespace()
    monkeypatch.setattr(api, '_link_or_error', lambda: (link, None))
    class Request:
        async def body(self):
            raise ConnectionError('browser body aborted')
    result = inline(getattr(api, handler)(Request(), 'abort', 'safe.bin', 4, 0, False, False))
    assert result == {'ok': False, 'cancelled': True, 'error': 'upload cancelled'}
    assert not api._upload_sessions
