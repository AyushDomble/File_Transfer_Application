"""Bounded, acknowledged, in-memory chat on an authenticated transfer session.

No additional listener or unauthenticated messaging path. Each peer owns a
separate channel. IDs deduplicate retries within the retained 200-message window.
"""
from collections import OrderedDict, deque
from datetime import datetime, timezone
import threading
import time
import uuid

from .session_protocol import SessionError
from .transport import FrameError, FrameType, validate_transfer_id

MAX_CHAT_CHARS = 2000
HISTORY_LIMIT = 200
ACK_TIMEOUT = 8.0


class ChatError(Exception):
    pass


def validate_text(text):
    if not isinstance(text, str) or not text.strip() or len(text) > MAX_CHAT_CHARS:
        raise ChatError('Write a message between 1 and 2,000 characters.')
    try:
        if len(text.encode('utf-8')) > 8000:
            raise ChatError('Message is too large.')
    except UnicodeError as exc:
        raise ChatError('Message contains invalid characters.') from exc
    if any(ord(c) < 32 and c not in '\n\t' for c in text):
        raise ChatError('Message contains unsupported control characters.')
    return text.strip()


class ChatChannel:
    def __init__(self):
        self.id = uuid.uuid4().hex
        self._lock = threading.RLock()
        self._send_lock = threading.Lock()
        self._session = None
        self._messages = OrderedDict()
        self._pending = {}
        self._received = deque()
        self._sent = deque()
        self._revision = 0
        self._incoming = 0

    def bind(self, session, router):
        with self._lock:
            self._session = session
        # Install before any reader (including scheduler.attach_session) starts.
        router.chat_handler = lambda frame: self._receive(session, frame)

    @property
    def available(self):
        session = self._session
        return bool(session and session.authenticated and not session.is_lost and session.supports('chat'))

    def info(self):
        with self._lock:
            session = self._session
            return dict(channel_id=self.id, revision=self._revision, incoming=self._incoming,
                        connected=bool(session and not session.is_lost),
                        supported=bool(session and session.supports('chat')),
                        available=self.available)

    def snapshot(self):
        with self._lock:
            return [dict(message) for message in self._messages.values()]

    def view(self, channel_id=None, revision=-1):
        with self._lock:
            info = self.info()
            messages = None if channel_id == self.id and revision == self._revision else self.snapshot()
            return info, messages

    @staticmethod
    def _rate_limit(timestamps):
        now = time.monotonic()
        while timestamps and timestamps[0] < now - 10:
            timestamps.popleft()
        if len(timestamps) >= 20:
            raise ChatError('Too many messages. Please wait a few seconds.')
        timestamps.append(now)

    def _put(self, key, message):
        self._messages[key] = message
        while len(self._messages) > HISTORY_LIMIT:
            self._messages.popitem(last=False)
        self._revision += 1

    def _receive(self, session, frame):
        if session is not self._session or not session.authenticated or not session.supports('chat'):
            return
        if frame.frame_type == FrameType.CHAT_ACK:
            try:
                data = frame.json()
            except FrameError:
                return
            with self._lock:
                pending = self._pending.get(frame.transfer_id)
                if pending and pending['session'] is session:
                    pending['reply'] = data
                    pending['event'].set()
            return
        reply = {'ok': False}
        try:
            validate_transfer_id(frame.transfer_id, allow_null=False)
            if frame.flags or frame.sequence:
                raise ChatError('Invalid chat frame.')
            text = validate_text(frame.json().get('text'))
            key = ('incoming', frame.transfer_id)
            with self._lock:
                old = self._messages.get(key)
                if old and old['text'] != text:
                    raise ChatError('Message ID was already used for different text.')
                if not old:
                    self._rate_limit(self._received)
                    self._incoming += 1
                    self._put(key, dict(id=frame.transfer_id, direction='incoming', text=text,
                                        timestamp=datetime.now(timezone.utc).isoformat(), status='received'))
            reply = {'ok': True}
        except (ChatError, FrameError) as exc:
            reply['error'] = str(exc)
        try:
            session.send_json(FrameType.CHAT_ACK, reply, transfer_id=frame.transfer_id)
        except (OSError, SessionError, FrameError):
            pass  # Retry uses the same message ID; no duplicate display.

    def send(self, text, message_id, timeout=ACK_TIMEOUT):
        text = validate_text(text)
        try:
            validate_transfer_id(message_id, allow_null=False)
        except FrameError as exc:
            raise ChatError('Invalid message ID.') from exc
        if not self._send_lock.acquire(blocking=False):
            raise ChatError('A message is still sending. Please wait.')
        key = ('outgoing', message_id)
        pending = {'event': threading.Event(), 'reply': None, 'session': self._session}
        try:
            with self._lock:
                previous = self._messages.get(key)
                if previous and previous['text'] != text:
                    raise ChatError('Message ID was already used for different text.')
                if previous and previous['status'] == 'delivered':
                    return dict(previous)
                if not self.available:
                    raise ChatError('Chat is unavailable. Connect to a device with chat support first.')
                self._rate_limit(self._sent)
                session = self._session
                pending['session'] = session
                self._pending[message_id] = pending
                message = dict(id=message_id, direction='outgoing', text=text,
                               timestamp=previous['timestamp'] if previous else datetime.now(timezone.utc).isoformat(),
                               status='sending')
                self._put(key, message)
            try:
                session.send_json(FrameType.CHAT_MESSAGE, {'text': text}, transfer_id=message_id)
                deadline = time.monotonic() + timeout
                while not pending['event'].wait(min(.1, max(0, deadline - time.monotonic()))):
                    if session.is_lost or session is not self._session or time.monotonic() >= deadline:
                        raise ChatError('Delivery not confirmed. Reconnect if needed and retry the same message.')
                reply = pending['reply']
                if reply.get('ok') is not True:
                    raise ChatError(reply.get('error') or 'The other device could not receive this message.')
            except (OSError, SessionError, FrameError, ChatError) as exc:
                with self._lock:
                    message['status'] = 'unconfirmed'
                    self._revision += 1
                raise ChatError(str(exc)) from exc
            with self._lock:
                message['status'] = 'delivered'
                self._revision += 1
                return dict(message)
        finally:
            with self._lock:
                self._pending.pop(message_id, None)
            self._send_lock.release()
