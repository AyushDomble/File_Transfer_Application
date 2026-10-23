"""Chat validation, isolation, delivery and coexistence with real TCP transfers."""
from concurrent.futures import ThreadPoolExecutor
import threading
import uuid

import httpx
import pytest

from filetransfer.chat import ChatChannel, ChatError, HISTORY_LIMIT, validate_text
from filetransfer.engine import FrameRouter, RecvEngine
from filetransfer.hostlink import HostLink
from filetransfer.session_protocol import ALL_FEATURES, Feature, NegotiatedSession, Session, SessionError
from filetransfer.transport import Frame, FrameType
from filetransfer.transfer import TransferState
from helpers.native_peers import native_pair, native_http
from helpers.app_peers import wait


def mid():
    return uuid.uuid4().hex


@pytest.mark.parametrize('value', ['', '  ', 'a' * 2001, None, '\x00', '\ud800'])
def test_reject_invalid_message(value):
    with pytest.raises(ChatError):
        validate_text(value)


def test_unicode_and_multiline_valid():
    assert validate_text('  Hi 👋\nनमस्ते  ') == 'Hi 👋\nनमस्ते'


def test_chat_requires_authenticated_negotiated_session():
    class Socket:
        def sendall(self, data):
            pytest.fail('unapproved chat must not reach the socket')
    session = Session(Socket(), ('test', 1))
    with pytest.raises(SessionError):
        session.send_json(FrameType.CHAT_MESSAGE, {'text': 'hi'}, transfer_id=mid())
    session.mark_authenticated()
    session._negotiation = NegotiatedSession(2, ALL_FEATURES - {Feature.CHAT})
    with pytest.raises(SessionError):
        session.send_json(FrameType.CHAT_MESSAGE, {'text': 'hi'}, transfer_id=mid())


def test_router_never_routes_chat_into_transfer_queue():
    router = FrameRouter(None)
    cid = mid()
    transfer_queue = router.register(cid)
    frames = []
    router.chat_handler = frames.append
    router.dispatch(Frame.json_frame(FrameType.CHAT_MESSAGE, {'text': 'hi'}, transfer_id=cid))
    router.dispatch(Frame.json_frame(FrameType.CHAT_ACK, {'ok': True}, transfer_id=cid))
    assert len(frames) == 2 and transfer_queue.empty()


def test_channel_memory_bounds_duplicates_and_rate_limit():
    class SessionStub:
        authenticated = True
        is_lost = False
        def __init__(self): self.replies = []
        def supports(self, name): return True
        def send_json(self, kind, data, **kwargs): self.replies.append(data)
    class RouterStub: pass
    session, channel = SessionStub(), ChatChannel()
    channel.bind(session, RouterStub())
    message = Frame.json_frame(FrameType.CHAT_MESSAGE, {'text': '<script>hello</script>'}, transfer_id=mid())
    channel._receive(session, message)
    channel._receive(session, message)
    assert len(channel.snapshot()) == 1 and channel.info()['incoming'] == 1
    for i in range(19):
        channel._receive(session, Frame.json_frame(FrameType.CHAT_MESSAGE, {'text': str(i)}, transfer_id=mid()))
    channel._receive(session, Frame.json_frame(FrameType.CHAT_MESSAGE, {'text': 'too fast'}, transfer_id=mid()))
    assert session.replies[-1]['ok'] is False
    assert len(channel.snapshot()) == 20
    for i in range(HISTORY_LIMIT + 5):
        channel._received.clear()  # each iteration models a later rate-limit interval
        channel._receive(session, Frame.json_frame(FrameType.CHAT_MESSAGE, {'text': str(i)}, transfer_id=mid()))
    assert len(channel.snapshot()) == HISTORY_LIMIT
    before = channel.info()['revision']
    assert channel.view(channel.id, before)[1] is None
    channel._receive(session, Frame(FrameType.CHAT_MESSAGE, b'not json', transfer_id=mid()))
    assert session.replies[-1]['ok'] is False
    assert channel.info()['revision'] == before


def test_two_way_chat_during_active_file_transfer(native_pair, tmp_path, monkeypatch):
    host, link = native_pair
    peer = host.get_peer(host.list_peers()[0]['peer_id'])
    assert link.chat.available and peer.chat.available
    entered, release = threading.Event(), threading.Event()
    original = RecvEngine._receive_chunk
    def hold(engine, *args):
        if engine._session is peer.session and not entered.is_set():
            entered.set()
            assert release.wait(10)
        return original(engine, *args)
    monkeypatch.setattr(RecvEngine, '_receive_chunk', hold)
    source = tmp_path / 'chat-transfer.bin'
    source.write_bytes(bytes(range(256)) * 2048)
    transfer = link.scheduler.submit_upload(str(source), chunk_size=4096)
    try:
        assert entered.wait(5)
        with ThreadPoolExecutor(2) as pool:
            sent = [pool.submit(link.chat.send, 'Sending your files 👋', mid()),
                    pool.submit(peer.chat.send, 'Thanks!\nKeep it going.', mid())]
            assert all(job.result(timeout=5)['status'] == 'delivered' for job in sent)
        assert any(m['text'] == 'Sending your files 👋' and m['direction'] == 'incoming' for m in peer.chat.snapshot())
        assert any(m['text'] == 'Thanks!\nKeep it going.' and m['direction'] == 'incoming' for m in link.chat.snapshot())
        assert not link.router.connection_lost.is_set()
    finally:
        release.set()
    wait(lambda: transfer.is_terminal)
    assert transfer.state == TransferState.COMPLETED
    assert (tmp_path / 'server/chat-transfer.bin').read_bytes() == source.read_bytes()


def test_lost_ack_retry_deduplicates_and_reconnect_keeps_live_history(native_pair, monkeypatch):
    host, link = native_pair
    peer = host.get_peer(host.list_peers()[0]['peer_id'])
    original = peer.session.send_json
    def drop_ack(kind, *args, **kwargs):
        if kind != FrameType.CHAT_ACK:
            return original(kind, *args, **kwargs)
    message_id = mid()
    monkeypatch.setattr(peer.session, 'send_json', drop_ack)
    with pytest.raises(ChatError, match='not confirmed'):
        link.chat.send('Only show this once', message_id, timeout=.2)
    assert link.chat.snapshot()[0]['status'] == 'unconfirmed'
    assert len(peer.chat.snapshot()) == 1
    monkeypatch.setattr(peer.session, 'send_json', original)
    assert link.chat.send('Only show this once', message_id)['status'] == 'delivered'
    assert len(peer.chat.snapshot()) == 1
    with pytest.raises(ChatError, match='different text'):
        link.chat.send('changed', message_id)
    link.close()
    with pytest.raises(ChatError, match='unavailable'):
        link.chat.send('offline', mid())
    link.connect_with_pin(host.current_pin)
    wait(lambda: peer.chat.available)
    link.chat.send('We are back', mid())
    assert len(peer.chat.snapshot()) == 2
    assert len(link.chat.snapshot()) == 2


def test_messages_are_private_to_selected_client(native_pair, tmp_path):
    host, first = native_pair
    first_peer = host.get_peer(host.list_peers()[0]['peer_id'])
    second = HostLink('127.0.0.1', port=first.port, use_tls=True, dest_dir=str(tmp_path / 'second-client'), fingerprint=host.certificate_fingerprint)
    try:
        second.connect_with_pin(host.current_pin)
        wait(lambda: len(host.list_peers()) == 2)
        first_peer.chat.send('For the first device only', mid())
        assert len(first.chat.snapshot()) == 1
        assert second.chat.snapshot() == []
        second.send_id = mid()
        second.chat.send('From the second device', second.send_id)
        assert len(first_peer.chat.snapshot()) == 1
        second_peer = next(host.get_peer(p['peer_id']) for p in host.list_peers() if p['peer_id'] != first_peer.peer_id)
        assert second_peer.chat.snapshot()[0]['text'] == 'From the second device'
    finally:
        second.close()


def test_chat_http_snapshot_validation_and_acknowledged_send(native_http):
    base, api, host, link = native_http
    client = httpx.Client(base_url=base, headers=api._test_http_headers)
    try:
        snapshot = client.get('/api/chat', params={'side':'client'}).json()
        conversation = snapshot['conversation']
        sent = client.post('/api/chat/messages', json={'side':'client', 'peer_id':conversation['peer_id'], 'message_id':mid(), 'text':'From HTTP'})
        assert sent.status_code == 200 and sent.json()['message']['status'] == 'delivered'
        host_snapshot = client.get('/api/chat', params={'side':'host'}).json()
        assert host_snapshot['messages'][0]['text'] == 'From HTTP'
        assert host_snapshot['conversation']['incoming'] == 1
        current = host_snapshot['conversation']
        unchanged = client.get('/api/chat', params={'side':'host', 'peer_id':current['peer_id'], 'channel_id':current['channel_id'], 'revision':current['revision']}).json()
        assert unchanged['messages'] is None
        payload = {'side':'client', 'peer_id':current['peer_id'], 'message_id':mid(), 'text':'wrong peer'}
        assert client.post('/api/chat/messages', json=payload).status_code == 404
        payload.update(peer_id=conversation['peer_id'], text='x' * 2001)
        assert client.post('/api/chat/messages', json=payload).status_code == 422
        assert client.get('/api/chat?side=invalid').status_code == 422
    finally:
        client.close()
