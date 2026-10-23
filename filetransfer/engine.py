"""
Transfer engine.

Architecture
────────────
Two classes handle the two sides of each transfer:

  SendEngine   — runs on the initiating side (uploader)
  RecvEngine   — runs on the receiving side (downloader)

Both run in their own thread. The FrameRouter demultiplexes inbound
frames from the Session into per-transfer Queues. Each engine pulls
from its own Queue — it never calls session.recv() directly.

Public control API (thread-safe, callable from any thread):
  SendEngine.pause()   — initiate pause from sender side
  SendEngine.resume()  — initiate resume from sender side
  SendEngine.cancel()  — cancel this transfer (session stays open)
  RecvEngine.pause()   — initiate pause from receiver side
  RecvEngine.resume()  — initiate resume from receiver side
  RecvEngine.cancel()  — cancel this transfer (session stays open)

pause — Pause (deterministic safe-point)
  pause() requests a pause; PAUSED is only reached after the current
  in-flight chunk + ACK exchange completes. Never interrupts mid-chunk.

resume — Resume
  The receiver is authoritative for the confirmed offset.
  resume() sends TRANSFER_RESUME; receiver replies with
  TRANSFER_ACK {offset:N, resuming:True}; sender seeks to N.

integrity — Per-chunk integrity verification
  Each TRANSFER_DATA frame carries a SHA-256 of the raw chunk bytes in
  its JSON prefix: {"offset": N, "sha256": "hex"}

  The receiver verifies the chunk hash before writing. On mismatch it
  sends negotiated TRANSFER_RETRY (or adapter TRANSFER_ERROR metadata)
  instead of ACK. The sender
  retransmits the chunk (up to MAX_CHUNK_RETRIES times). On exhaustion
  the transfer fails.

  The whole-file SHA-256 (sent in TRANSFER_COMPLETE) is unchanged and
  provides end-to-end integrity on top of per-chunk verification.

retry — Retry
  Chunk-level retry without restarting the whole transfer.
  TRANSFER_RETRY payload: {"offset": start, "end": end, "sequence": N}.
  Only the outstanding stop-and-wait range can be retried. The sender
  retransmits the identical buffered bytes, also on an ACK timeout.
  Max retries per chunk: MAX_CHUNK_RETRIES (default 3).
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import socket
import struct
import threading
import time
import uuid
from typing import Callable, Optional

from . import config
from .logsetup import get_logger
from .flowcontrol import GlobalWindow, TransferWindow, validate_capacity
from .manager import TransferManager, TransferNotFound
from .session_protocol import Session, SessionError
from .security import safe_filename, shared_file_path
from .transfer import (
    Transfer, TransferDirection, TransferState, TransferStateError,
    TERMINAL_STATES,
)
from .transport import Frame, FrameType, FrameFlags, ConnectionClosed, FrameError
from .transport import validate_transfer_id
from .transferfiles import claim_destination, destination_for_part, partial_path, publish

log = get_logger("filetransfer.engine")


# ── Constants ─────────────────────────────────────────────────────────────────

DEFAULT_CHUNK_SIZE  = 1024 * 1024   # 1 MB
ACK_TIMEOUT_SECONDS = 30
CREATE_TIMEOUT      = 15
MAX_CHUNK_RETRIES   = 3             # retry: max retransmits per chunk
MAX_CHUNK_SIZE = 8 * 1024 * 1024


def _integer(value, name, maximum=(1 << 63) - 1):
    if type(value) is not int or not 0 <= value <= maximum:
        raise TransferError(f"{name} must be an integer between 0 and {maximum}")
    return value


def _checksum(value):
    if value is not None and (not isinstance(value, str) or len(value) != 64
                             or any(c not in '0123456789abcdef' for c in value)):
        raise TransferError('checksum must be a lowercase SHA-256 digest')
    return value


def require_feature(session, name):
    if getattr(session, 'negotiation', None) is not None and not session.supports(name):
        raise TransferStateError(f'Peer did not negotiate {name}')


# ── Data-frame codec ──────────────────────────────────────────────────────────

_PREFIX_STRUCT = struct.Struct(">I")


def _chunk_sha256(chunk: bytes) -> str:
    """SHA-256 hex digest of raw chunk bytes (integrity)."""
    return hashlib.sha256(chunk).hexdigest()


def _encode_data_payload(offset: int, chunk: bytes) -> bytes:
    """
    Pack [4B prefix_len][JSON prefix][raw chunk].

    integrity: the prefix includes the chunk's SHA-256 so the receiver
    can verify integrity before writing.

    JSON prefix: {"offset": N, "sha256": "<hex>"}
    """
    sha = _chunk_sha256(chunk)
    prefix = json.dumps(
        {"offset": offset, "sha256": sha}, separators=(",", ":")
    ).encode()
    return _PREFIX_STRUCT.pack(len(prefix)) + prefix + chunk


def _decode_data_payload(payload: bytes) -> tuple[int, bytes, Optional[str]]:
    """
    Unpack a TRANSFER_DATA payload.

    Returns (offset, raw_chunk, chunk_sha256_or_None).
    The codec reports an absent checksum as None; RecvEngine requires one.
    Raises ValueError on malformed input.
    """
    if len(payload) < 4:
        raise ValueError("DATA payload too short")
    (prefix_len,) = _PREFIX_STRUCT.unpack_from(payload, 0)
    if prefix_len > 4096:
        raise ValueError('DATA prefix is too large')
    end = 4 + prefix_len
    if len(payload) < end:
        raise ValueError(f"DATA payload truncated: need {end}, got {len(payload)}")
    try:
        meta = json.loads(payload[4:end])
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"DATA prefix JSON invalid: {e}") from e
    if not isinstance(meta, dict):
        raise ValueError('DATA prefix must be a small JSON object')
    offset = meta.get("offset")
    if not isinstance(offset, int):
        raise ValueError(f"DATA prefix missing 'offset': {meta!r}")
    # Parsing permits absent sha256 metadata; the receiver validates integrity.
    chunk_sha = meta.get("sha256")
    return offset, payload[end:], chunk_sha


class TransferError(Exception):
    """Unrecoverable transfer-level error."""


class AckTimeout(TransferError):
    """Only an ACK deadline is eligible for bounded chunk retransmission."""


def _range_retry_supported(session):
    return (getattr(session, 'negotiation', None) is not None
            and session.supports('retry_ranges'))


def _scoped_flow_supported(session):
    return (getattr(session, 'negotiation', None) is not None
            and session.supports('flow_control') and session.supports('scoped_flow_control'))


def _resume_with_slot(engine, tid):
    """True: admitted, False: stopped, None: withdrawn; repeat the handshake."""
    with engine._transfer.lock:
        if engine._pause_flag.is_set():
            engine._pause_flag.clear()
            return None
    # resume_transfer schedules other transfers after releasing its own lock.
    # Never keep this transfer locked across that cross-transfer scheduling.
    engine._manager.resume_transfer(tid)
    while not engine._cancel_event.is_set():
        with engine._transfer.lock:
            state = engine._transfer.state
            pending = engine._transfer.resume_requested
            if engine._pause_flag.is_set():
                # Pause may race the admission above. No new DATA has been
                # sent yet, so this is already a safe checkpoint.
                if state == TransferState.ACTIVE or (state == TransferState.PAUSED and pending):
                    engine._manager.pause_transfer(tid)
                    state = engine._transfer.state
                if state == TransferState.PAUSING:
                    engine._manager.confirm_paused(tid)
                engine._pause_flag.clear()
                return None
            if state == TransferState.ACTIVE:
                return True
            if state != TransferState.PAUSED:
                return False
            if not pending:
                # Do not consume a newer local resume trigger. A fresh
                # handshake, not this old confirmed offset, must admit it.
                return None
            # Inspect this bounded Queue under its own mutex: rotating
            # unrelated frames with get/put could reorder traffic or race a
            # full queue. Only consume PAUSE; a fresh RESUME stays queued for
            # the next handshake and retains its original ordering.
            frame = None
            with engine._queue.mutex:
                for queued in engine._queue.queue:
                    if queued.frame_type in (FrameType.TRANSFER_PAUSE, FrameType.TRANSFER_RESUME):
                        frame = queued
                        break
                if frame is not None and frame.frame_type == FrameType.TRANSFER_PAUSE:
                    engine._queue.queue.remove(frame)
                    engine._queue.not_full.notify()
            if frame is not None:
                engine._manager.pause_transfer(tid)
                return None
        engine._cancel_event.wait(0.05)
    return False


# ── FrameRouter ───────────────────────────────────────────────────────────────

class FrameRouter:
    """
    Demultiplexes frames arriving on one Session into per-transfer queues.
    Session-level frames (PING/PONG) are handled inline.
    """

    def __init__(self, session: Session) -> None:
        self._session = session
        self._queues: dict[str, queue.Queue] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None
        self._reader_start_lock = threading.Lock()
        self.chat_handler = None

        # scheduling: inbound TRANSFER_CREATE frames arrive with a
        # transfer_id nobody has registered a queue for yet — the
        # receiver doesn't know about the transfer until this frame
        # shows up. Route these to a dedicated inbox instead of
        # dropping them, so an "accept loop" can consume them one at
        # a time and register a real per-transfer queue before
        # replying, regardless of how many other transfers are
        # already streaming on this session.
        self._create_queue: queue.Queue = queue.Queue(maxsize=32)
        # API integration: same idea as _create_queue, for the host-browsing
        # request types (FILE_LIST_REQUEST / FILE_DELETE_REQUEST) —
        # unsolicited inbound frames with no per-transfer queue
        # registered yet.
        self._control_queue: queue.Queue = queue.Queue(maxsize=32)
        self._cancel_listeners = []
        self._cancel_ack_listeners = []
        self._window_listeners = []
        self._cancelled = {}  # bounded session-local tombstones for cancel-before-CREATE

        # recovery: connection-loss detection. `connection_lost` is set
        # (once) when the reader hits EOF / a fatal socket error that
        # we did not ask for, or when the keepalive sees the peer go
        # silent. Deliberate stop()/stop_reader() never sets it.
        self.connection_lost = threading.Event()
        self._loss_reason: Optional[str] = None
        self._loss_listeners: list[Callable[[], None]] = []
        self._loss_lock = threading.Lock()
        self._last_rx = time.monotonic()
        self._keepalive_thread: Optional[threading.Thread] = None

    def add_loss_listener(self, fn: Callable[[], None]) -> None:
        """Call fn() (once, from the reader/keepalive thread) when this
        router's connection is declared lost. If it already was, fn is
        called immediately."""
        with self._loss_lock:
            already = self.connection_lost.is_set()
            if not already:
                self._loss_listeners.append(fn)
        if already:
            try:
                fn()
            except Exception:
                log.exception("FrameRouter loss listener raised")

    def declare_lost(self, reason: str) -> None:
        """Idempotent. Marks the session lost, unblocks the reader by
        shutting the socket down, and notifies listeners."""
        with self._loss_lock:
            if self.connection_lost.is_set():
                return
            self._loss_reason = reason
            self.connection_lost.set()
            listeners = list(self._loss_listeners)
            self._loss_listeners.clear()
        log.warning("FrameRouter: connection lost (%s)", reason)
        self._session.mark_lost()
        try:
            sock = self._session._sock
            underlying = getattr(sock, '_s', sock)
            if isinstance(underlying, socket.socket):
                socket.socket.shutdown(underlying, socket.SHUT_RDWR)
            else:
                underlying.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        for fn in listeners:
            try:
                fn()
            except Exception:
                log.exception("FrameRouter loss listener raised")

    def start_keepalive(self, interval: float = 2.0, timeout: float = 8.0) -> None:
        """
        Detect half-open connections (laptop sleep, Wi-Fi drop, cable
        pull) that never deliver an EOF. Sends a PING after `interval`
        seconds of receive-silence and declares the connection lost
        after `timeout` seconds of it. Any received frame — including
        the peer's automatic PONG — counts as liveness. Off by default.
        """
        if self._keepalive_thread is not None:
            return

        def _loop() -> None:
            tick = max(0.05, min(interval, timeout) / 4)
            while not self._stop.is_set() and not self.connection_lost.is_set():
                idle = time.monotonic() - self._last_rx
                if idle > timeout:
                    self.declare_lost(f"no data for {idle:.1f}s (keepalive)")
                    return
                if idle > interval:
                    try:
                        self._session.send_ping()
                    except Exception:
                        self.declare_lost("keepalive ping failed")
                        return
                self._stop.wait(tick)

        self._keepalive_thread = threading.Thread(
            target=_loop, daemon=True, name="FrameRouter-keepalive")
        self._keepalive_thread.start()

    def register(self, transfer_id: str) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=128)
        with self._lock:
            self._queues[transfer_id] = q
        return q

    def get_queue(self, transfer_id: str) -> Optional[queue.Queue]:
        """Return the already-registered queue for this transfer, or None.
        Use this to fetch a queue a prior register() call created —
        calling register() again would silently replace it and drop
        anything already buffered."""
        with self._lock:
            return self._queues.get(transfer_id)

    def unregister(self, transfer_id: str) -> None:
        with self._lock:
            self._queues.pop(transfer_id, None)

    def next_create(self, timeout: Optional[float] = None) -> Optional[Frame]:
        """Block for the next inbound TRANSFER_CREATE frame not yet
        claimed by any registered transfer. Returns None on timeout."""
        try:
            return self._create_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def next_control_request(self, timeout: Optional[float] = None) -> Optional[Frame]:
        """API integration: block for the next inbound FILE_LIST_REQUEST /
        FILE_DELETE_REQUEST. Returns None on timeout."""
        try:
            return self._control_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def dispatch(self, frame: Frame) -> None:
        if frame.frame_type in (FrameType.CHAT_MESSAGE, FrameType.CHAT_ACK):
            if self.chat_handler is not None:
                self.chat_handler(frame)
            return  # Never allow chat correlation IDs into a transfer queue.
        if frame.frame_type == FrameType.TRANSFER_CANCEL and frame.json().get('acknowledged'):
            for listener in list(self._cancel_ack_listeners):
                listener(frame.transfer_id)
            return
        if frame.frame_type == FrameType.WINDOW_UPDATE and self._window_listeners:
            for listener in list(self._window_listeners):
                listener(frame)
            return
        if frame.frame_type == FrameType.PING:
            try:
                self._session.send_pong()
            except Exception:
                pass
            return
        if frame.frame_type == FrameType.PONG:
            return
        if frame.frame_type == FrameType.TRANSFER_CANCEL and not frame.json().get('acknowledged'):
            with self._lock:
                self._cancelled[frame.transfer_id] = True
                if len(self._cancelled) > 1024:
                    self._cancelled.pop(next(iter(self._cancelled)))
            handled = False
            for fn in list(self._cancel_listeners):
                handled = fn(frame.transfer_id) or handled
            self._session.send_json(FrameType.TRANSFER_CANCEL,
                {'acknowledged': True, 'reason': 'cancel received'}, transfer_id=frame.transfer_id)
            if handled:
                return
        if frame.frame_type == FrameType.TRANSFER_CREATE and self.is_cancelled(frame.transfer_id):
            self._session.send_json(FrameType.TRANSFER_REJECT, {'reason': 'transfer was cancelled'},
                                    transfer_id=frame.transfer_id)
            return
        if frame.frame_type == FrameType.TRANSFER_CREATE:
            try:
                self._create_queue.put_nowait(frame)
            except queue.Full:
                self._session.send_json(FrameType.TRANSFER_REJECT, {'reason': 'create queue is full'},
                                        transfer_id=frame.transfer_id)
            return
        with self._lock:
            q = self._queues.get(frame.transfer_id)
        if q is not None:
            try:
                q.put_nowait(frame)
            except queue.Full:
                log.warning(
                    "Frame queue full for %s — dropping %s",
                    frame.transfer_id[:8], frame.frame_type.name,
                )
        elif frame.frame_type in (FrameType.FILE_LIST_REQUEST, FrameType.FILE_DELETE_REQUEST,
                                  FrameType.BATCH_REQUEST, FrameType.BATCH_STATUS, FrameType.BATCH_CANCEL):
            try:
                self._control_queue.put_nowait(frame)
            except queue.Full:
                log.warning("Control inbox full — dropping %s", frame.frame_type.name)
        else:
            log.debug(
                "No queue for %s — dropping %s",
                frame.transfer_id[:8], frame.frame_type.name,
            )

    def is_cancelled(self, transfer_id):
        with self._lock:
            return transfer_id in self._cancelled

    def add_cancel_listener(self, fn):
        self._cancel_listeners.append(fn)

    def add_window_listener(self, fn):
        self._window_listeners.append(fn)

    def add_cancel_ack_listener(self, fn):
        self._cancel_ack_listeners.append(fn)

    def start_reader(self) -> None:
        # attach_session starts its fresh router before app-level setup also
        # calls this method. Two readers corrupt the incremental frame decoder.
        with self._reader_start_lock:
            if self._reader_thread is not None:
                return
            self._reader_thread = threading.Thread(
                target=self._reader_loop, daemon=True, name="FrameRouter-reader"
            )
            self._reader_thread.start()

    def stop_reader(self, timeout: float = 1.0) -> None:
        """
        Stop the reader thread without touching the underlying socket —
        unlike stop(), which also calls shutdown() on it. Useful when a
        new FrameRouter is about to take over reading from the same,
        still-live connection rather than tearing the connection down
        — e.g. this process's in-memory transfer state was lost (a
        restart) but the socket itself survived, and a fresh
        TransferManager/ConcurrentScheduler/FrameRouter is picking up
        where persistence.restore_transfer() left off. The reader loop
        already polls a short socket timeout regardless of any
        shutdown signal (see _reader_loop), so this is a clean stop —
        just a slower one (up to that timeout) than stop()'s forced
        interrupt via shutdown().
        """
        self._stop.set()
        if self._reader_thread:
            self._reader_thread.join(timeout=timeout)

    def stop(self) -> None:
        """
        Stop the router and wait for the reader thread to exit.

        Two mechanisms ensure the reader unblocks promptly:
        1. shutdown() on the underlying socket forces any blocked recv_into()
           to return immediately with an OSError on Linux.
        2. The reader uses a socket timeout (set in start_reader) so it
           polls _stop every second as a fallback even if shutdown fails.
        """
        self._stop.set()
        # Interrupt the reader thread's blocked recv_into() immediately.
        # shutdown(SHUT_RDWR) on a socketpair makes recv return 0/EBADF.
        try:
            sock = self._session._sock
            # Reach through Throttle wrapper if present
            underlying = getattr(sock, '_s', sock)
            if isinstance(underlying, socket.socket):
                socket.socket.shutdown(underlying, socket.SHUT_RDWR)
            else:
                underlying.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass   # already closed or not a real socket — harmless
        if self._reader_thread:
            self._reader_thread.join(timeout=1)   # should return almost instantly

    def _reader_loop(self) -> None:
        # Set a short socket timeout so we can poll _stop even if shutdown
        # doesn't interrupt recv immediately (e.g. when Throttle is used).
        try:
            self._session.set_read_timeout(0.5)
        except OSError:
            pass
        while not self._stop.is_set():
            try:
                frame = self._session.recv()
                self._last_rx = time.monotonic()
                self.dispatch(frame)
            except ConnectionClosed:
                log.info("FrameRouter: connection closed")
                if not self._stop.is_set():
                    self.declare_lost("peer closed the connection")
                break
            except (FrameError, SessionError) as e:
                log.warning("FrameRouter: frame error: %s", e)
                if not self._stop.is_set():
                    self.declare_lost(f"malformed framing: {e}")
                break
            except TimeoutError:
                # socket.timeout (== TimeoutError in Python 3.11+) from the
                # 0.5s read timeout we set above — just poll _stop and retry.
                continue
            except OSError as e:
                import errno as _errno
                # ETIMEDOUT / EAGAIN are retriable; anything else is fatal.
                if e.errno in (_errno.ETIMEDOUT, _errno.EAGAIN, _errno.EWOULDBLOCK):
                    continue
                if not self._stop.is_set():
                    log.info("FrameRouter: socket error: %s", e)
                    self.declare_lost(f"socket error: {e}")
                break


# ── transfer creation: Transfer creation handshake ─────────────────────────────────────

class AlreadyComplete(Exception):
    """recovery: raised by send_transfer_create() when the peer answers a
    re-CREATE of a recovering transfer with "already complete" — the
    connection died after the receiver had written and verified the
    last byte but before its TRANSFER_COMPLETE reached us. There is
    nothing left to send; the caller should just mark the transfer
    COMPLETED (see .sha256)."""

    def __init__(self, sha256: Optional[str] = None) -> None:
        super().__init__("transfer already complete on the peer")
        self.sha256 = sha256


def send_transfer_create(
    session: Session,
    transfer: Transfer,
    router: "FrameRouter",
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    overwrite: bool = False,
    have_offset: int = 0,
    receive_window: Optional[int] = None,
    receive_global_window: Optional[int] = None,
) -> int:
    """Initiator: send TRANSFER_CREATE, wait for TRANSFER_ACCEPT.
    Returns the receiver's resume_offset.

    scheduling: routes the reply through `router` instead of calling
    session.recv() directly. Session.recv() is documented as single-
    reader-only (see session_protocol.Session) — the FrameRouter's
    background reader thread already owns that role once more than
    one transfer may be in flight on this session, so a second direct
    reader here would race it and could steal (and drop) another
    transfer's frame, or have its own ACCEPT frame stolen and dropped
    by the router before this function ever saw it. Registering the
    queue *before* sending TRANSFER_CREATE closes that race: the
    router will always have somewhere to put the ACCEPT/REJECT reply.
    The queue is left registered on return — the caller reuses it for
    the DATA/ACK frames that follow.
    """
    validate_transfer_id(transfer.id, allow_null=False)
    if not 1 <= _integer(chunk_size, 'chunk_size', MAX_CHUNK_SIZE):
        raise TransferError('chunk_size must be positive')
    _integer(have_offset, 'have_offset')
    transfer.chunk_size = chunk_size
    with transfer.lock:
        tid      = transfer.id
        filename = transfer.filename
        size     = transfer.total_size
        checksum = transfer.checksum
        direction = transfer.direction.value

    router.register(tid)
    try:
        create_id = uuid.uuid4().hex
        create_payload = {
            "filename": filename, "size": size, "checksum": checksum,
            "chunk_size": chunk_size, "direction": direction, "overwrite": overwrite,
            "retry": transfer.attempt > 0,
            "create_id": create_id,
        }
        if getattr(transfer, 'batch_id', None):
            create_payload['batch_id'] = transfer.batch_id
        if _scoped_flow_supported(session) and direction == 'download':
            create_payload['receive_window'] = receive_window or config.TRANSFER_WINDOW_BYTES
            create_payload['global_window'] = receive_global_window or config.GLOBAL_WINDOW_BYTES
        # A pull can resume when the serving side knows how many bytes
        # the downloader already holds.
        if have_offset > 0:
            create_payload["have_offset"] = int(have_offset)
        session.send_json(FrameType.TRANSFER_CREATE, create_payload, transfer_id=tid)

        q = router.get_queue(tid)
        deadline = time.monotonic() + CREATE_TIMEOUT
        while True:
            frame = _dequeue(q, max(0, deadline - time.monotonic()))
            if frame is None:
                raise TransferError(f"Timed out waiting for TRANSFER_ACCEPT ({tid[:8]})")
            if transfer.attempt > 0:
                if frame.frame_type in (FrameType.TRANSFER_ERROR, FrameType.TRANSFER_ACK,
                                         FrameType.TRANSFER_COMPLETE, FrameType.TRANSFER_RETRY):
                    continue  # previous attempt; ACCEPT is the ordering barrier
                if frame.frame_type == FrameType.TRANSFER_ACCEPT and frame.json().get('create_id') != create_id:
                    continue
            break

        if frame.frame_type == FrameType.TRANSFER_REJECT:
            raise TransferError(
                f"Transfer rejected: {frame.json().get('reason', 'unknown')}"
            )
        if frame.frame_type != FrameType.TRANSFER_ACCEPT:
            raise TransferError(
                f"Expected TRANSFER_ACCEPT, got {frame.frame_type.name}"
            )
    except Exception:
        router.unregister(tid)
        raise

    try:
        accept = frame.json()
        if accept.get('create_id', create_id) != create_id:
            raise TransferError('ACCEPT belongs to another creation attempt')
        resume_offset = _integer(accept.get('resume_offset'), 'resume_offset')
        if _scoped_flow_supported(session):
            transfer.chunk_size = _integer(accept.get('chunk_size', chunk_size), 'accepted chunk size', chunk_size)
            if not transfer.chunk_size:
                raise TransferError('accepted chunk size must be positive')
            if direction == 'upload':
                transfer.peer_window = validate_capacity(accept.get('receive_window', config.TRANSFER_WINDOW_BYTES))
                transfer.peer_global_window = validate_capacity(accept.get('global_window', config.GLOBAL_WINDOW_BYTES))
        if direction == TransferDirection.DOWNLOAD.value:
            total = _integer(accept.get('size'), 'accepted size')
            expected = _checksum(accept.get('checksum'))
            if expected is None:
                raise TransferError('pull ACCEPT must include the source checksum')
            with transfer.lock:
                transfer.total_size, transfer.checksum = total, expected
        else:
            total = size
        if resume_offset > total:
            raise TransferError('resume_offset exceeds file size')
        if accept.get('already_complete'):
            if resume_offset != total or _checksum(accept.get('sha256')) != checksum:
                raise TransferError('already-complete reply does not match the source')
            router.unregister(tid)
            raise AlreadyComplete(accept.get('sha256'))
    except Exception:
        router.unregister(tid)
        raise
    log.debug("TRANSFER_ACCEPT for %s: resume_offset=%d", tid[:8], resume_offset)
    return resume_offset


def _dequeue(q: queue.Queue, timeout: float) -> Optional[Frame]:
    """Small helper: get from a queue with a timeout, None instead of
    raising on empty. Used by the handshake functions above/below."""
    try:
        return q.get(timeout=timeout)
    except queue.Empty:
        return None


def recv_transfer_create(
    session: Session,
    frame: Frame,
    dest_dir: str,
    manager: TransferManager,
    router: "FrameRouter",
    max_size: int = 0,
    blocked_extensions: Optional[set] = None,
    start_immediately: bool = True,
    on_created: Optional[Callable[[Transfer, int, str], None]] = None,
    reattach_ok: Optional[Callable[[Transfer], bool]] = None,
    receive_window: Optional[int] = None,
    receive_global_window: Optional[int] = None,
) -> tuple[Transfer, int, str]:
    """Receiver: handle TRANSFER_CREATE, register transfer, reply ACCEPT/REJECT.

    recovery: if a transfer with this ID is ALREADY known to `manager`,
    the frame is a recovery attempt (the peer reconnected and is
    re-CREATEing the same transfer), not a new transfer. It is
    re-attached — same Transfer object, offset re-derived from disk (or
    from the requester's have_offset for a pull) — provided the
    metadata matches and `reattach_ok(transfer)` agrees (the scheduler
    refuses a transfer that still has a live engine, so a duplicate ID
    can't hijack a running transfer). A recovery that isn't allowed is
    REJECTed. The returned Transfer is left in CONNECTION_LOST/PAUSED
    for the caller to re-activate AFTER the ACCEPT has gone out.

    Returns (transfer, resume_offset, result_path) where result_path is a
    .part path to write to when transfer.direction is DOWNLOAD (peer
    pushed to us), or the source file path to read from when it's
    UPLOAD (peer asked to pull from us) — branch on transfer.direction.

    scheduling: registers the router queue for this transfer_id before
    the transfer is even created (not just before sending
    TRANSFER_ACCEPT). A scheduler's on_created callback can trigger an
    engine thread the instant create_transfer() promotes this transfer
    to ACTIVE — which can happen before this function's own next line
    runs — and that engine calls router.get_queue() immediately. If
    registration happened after create_transfer(), that thread could
    win the race and find no queue yet (same failure mode as replying
    before registering: the difference is only which downstream reader
    loses the race, direct-recv or FrameRouter).

    start_immediately is forwarded to manager.create_transfer() — pass
    False when a scheduler needs to record the pending engine job
    before the transfer can be promoted to ACTIVE (see scheduler.py).
    Prefer on_created for that instead: it's called atomically with
    the transfer's insertion into the registry, under the manager's
    own lock, closing a race that start_immediately alone can't (an
    unrelated transfer finishing elsewhere can trigger its own
    _schedule() pass at any time — start_immediately only suppresses
    *this* call's own pass, not that one). Registering the final ID
    atomically prevents promotion while keyed by a temporary ID.

    on_created, if given, is called as on_created(transfer,
    resume_offset, result_path) — result_path is the .part path to
    write to for a push (peer uploads to us) or the source file path
    to read from for a pull (peer asked to download from us), same as
    this function's own return value.
    """
    tid          = frame.transfer_id

    def _reject(reason: str) -> None:
        log.warning("Rejecting %s: %s", tid[:8], reason)
        session.send_json(FrameType.TRANSFER_REJECT, {"reason": reason}, transfer_id=tid)

    try:
        if frame.frame_type != FrameType.TRANSFER_CREATE:
            raise TransferError('expected TRANSFER_CREATE')
        validate_transfer_id(tid, allow_null=False)
        p = frame.json()
        raw_filename = p.get('filename', '')
        if not isinstance(raw_filename, str):
            raise TransferError('filename must be a string')
        filename = safe_filename(raw_filename)
        if any(ord(char) < 32 for char in filename) or len(filename.encode('utf-8')) > 255:
            raise TransferError('invalid filename characters or length')
        if os.name == 'nt':
            stem = filename.split('.', 1)[0].upper()
            reserved = {'CON', 'PRN', 'AUX', 'NUL'} | {f'{p}{n}' for p in ('COM', 'LPT') for n in range(1, 10)}
            if (stem in reserved or filename.endswith((' ', '.'))
                    or any(char in '<>:"|?*' for char in filename)):
                raise TransferError('filename is invalid on Windows')
        size = _integer(p.get('size'), 'size')
        checksum = _checksum(p.get('checksum'))
        direction_str = p.get('direction')
        if direction_str not in ('upload', 'download'):
            raise TransferError('direction must be upload or download')
        if direction_str == 'download':
            # Apply to new requests AND recovery before accepting any metadata.
            shared_file_path(dest_dir, filename)
        if direction_str == 'upload' and checksum is None:
            raise TransferError('upload CREATE must include the source checksum')
        chunk_size = _integer(p.get('chunk_size', DEFAULT_CHUNK_SIZE), 'chunk_size', MAX_CHUNK_SIZE)
        if not chunk_size:
            raise TransferError('chunk_size must be positive')
        if _scoped_flow_supported(session):
            own_window = validate_capacity(receive_window or config.TRANSFER_WINDOW_BYTES)
            own_global = validate_capacity(receive_global_window or config.GLOBAL_WINDOW_BYTES)
            if direction_str == 'upload':
                chunk_size = min(chunk_size, own_window, own_global)
            else:
                chunk_size = min(chunk_size, validate_capacity(p.get('receive_window', config.TRANSFER_WINDOW_BYTES)),
                                 validate_capacity(p.get('global_window', config.GLOBAL_WINDOW_BYTES)))
            p['chunk_size'] = chunk_size
            p['_receive_window'], p['_global_window'] = own_window, own_global
        if (size + chunk_size - 1) // chunk_size > 0x100000000:
            raise TransferError('file requires more than uint32 sequence space')
        _integer(p.get('have_offset', 0), 'have_offset')
        overwrite = p.get('overwrite', False)
        if type(overwrite) is not bool:
            raise TransferError('overwrite must be a boolean')
    except (ValueError, FrameError, TransferError) as e:
        _reject(str(e)); raise TransferError(str(e))

    ext = os.path.splitext(filename)[1].lower()
    if blocked_extensions and ext in blocked_extensions:
        r = f"file type {ext!r} not allowed"
        _reject(r); raise TransferError(r)

    direction = (
        TransferDirection.DOWNLOAD if direction_str == "upload"
        else TransferDirection.UPLOAD
    )
    if direction == TransferDirection.DOWNLOAD and max_size and size > max_size:
        _reject(f'file too large: {size} bytes')
        raise TransferError(f'file too large: {size} bytes')

    # ── recovery: recovery of a transfer we already know about ────────
    try:
        existing = manager.get(tid)
    except TransferNotFound:
        existing = None
    if existing is not None:
        # ERROR precedes retry CREATE on the stream, but its engine consumes
        # the error asynchronously. Wait for that old worker to close first.
        if p.get('retry') is True:
            existing.io_finished.wait(5)
        return _reattach_transfer(
            session, existing, p, filename, direction, dest_dir, router,
            on_created, reattach_ok, _reject,
        )

    if direction == TransferDirection.DOWNLOAD:
        # Peer is pushing a file to us — we're about to write it.
        if max_size and size and size > max_size:
            r = f"file too large: {size} bytes"
            _reject(r); raise TransferError(r)

        os.makedirs(dest_dir, exist_ok=True)
        dest_path = os.path.join(dest_dir, filename)
        part_path = partial_path(dest_path, tid, manager.namespace)

        if os.path.isfile(dest_path) and not overwrite:
            r = "file already exists on the host"
            session.send_json(FrameType.TRANSFER_REJECT,
                              {"reason": r, "conflict": True}, transfer_id=tid)
            raise TransferError(r)

        resume_offset = os.path.getsize(part_path) if os.path.isfile(part_path) else 0
        if resume_offset > size:
            _reject('partial file exceeds declared size')
            raise TransferError('partial file exceeds declared size')
        result_path = part_path
        transfer_size, transfer_checksum = size, checksum
    else:
        # Peer wants to pull a file FROM us — we'll read and send it.
        # Resolve the source for an inbound download request.
        src_path = os.path.join(dest_dir, filename)
        if not os.path.isfile(src_path):
            r = "file not found"
            _reject(r); raise TransferError(r)

        transfer_size = os.path.getsize(src_path)
        transfer_checksum = _compute_sha256(src_path)
        if checksum is not None and checksum != transfer_checksum:
            _reject('source changed since the previous download')
            raise TransferError('source changed since the previous download')
        resume_offset = min(p.get('have_offset', 0), transfer_size)
        result_path = src_path

    # Register the router queue before the transfer becomes visible to
    # the scheduler (create_transfer(), below) — not just before the
    # ACCEPT reply. on_created can trigger an immediate engine launch
    # on another thread the instant create_transfer() promotes this
    # transfer to ACTIVE, and that engine calls router.get_queue()
    # right away; if registration happened after create_transfer()
    # instead, that thread could beat this one to it and find no
    # queue yet.
    router.register(tid)

    def _created(t):
        if direction == TransferDirection.DOWNLOAD:
            claim_destination(t, dest_path, part_path, overwrite=overwrite)
        t.chunk_size = chunk_size
        t.max_receive_size = max_size
        if direction == TransferDirection.UPLOAD and _scoped_flow_supported(session):
            t.peer_window = p.get('receive_window', config.TRANSFER_WINDOW_BYTES)
            t.peer_global_window = p.get('global_window', config.GLOBAL_WINDOW_BYTES)
        if on_created is not None:
            on_created(t, resume_offset, result_path)
    try:
        transfer = manager.create_transfer(
            direction=direction, filename=filename,
            total_size=transfer_size, checksum=transfer_checksum,
            start_immediately=False, transfer_id=tid, on_created=_created, ready=False)
    except (ValueError, TransferError) as exc:
        router.unregister(tid)
        _reject(str(exc))
        raise TransferError(str(exc)) from exc

    try:
        session.send_json(FrameType.TRANSFER_ACCEPT,
                          {"resume_offset": resume_offset, "size": transfer_size,
                           "checksum": transfer_checksum, "chunk_size": chunk_size,
                           "receive_window": p.get('_receive_window'), "global_window": p.get('_global_window'),
                           "create_id": p.get('create_id')}, transfer_id=tid)
    except Exception:
        router.unregister(tid)
        if transfer.state == TransferState.QUEUED:
            manager.discard_unaccepted(tid)
        raise
    transfer.ready = True
    if start_immediately:
        manager.schedule()
    log.info("TRANSFER_ACCEPT sent for %s %r (resume_offset=%d)",
             tid[:8], filename, resume_offset)
    return transfer, resume_offset, result_path


# ── SendEngine ────────────────────────────────────────────────────────────────

def _reattach_transfer(
    session: Session,
    existing: Transfer,
    p: dict,
    filename: str,
    direction: TransferDirection,
    dest_dir: str,
    router: "FrameRouter",
    on_created: Optional[Callable[[Transfer, int, str], None]],
    reattach_ok: Optional[Callable[[Transfer], bool]],
    reject: Callable[[str], None],
) -> tuple[Transfer, int, str]:
    """See recv_transfer_create's recovery note. Validates that the
    re-CREATE really describes `existing`, computes the offset the
    peer should continue from, installs the engine job via on_created,
    registers the queue and sends ACCEPT."""
    tid  = existing.id
    size = int(p.get("size", 0))
    checksum = p.get("checksum")

    def _refuse(reason: str):
        reject(reason)
        raise TransferError(reason)

    with existing.lock:
        state = existing.state
        e_dir, e_name = existing.direction, existing.filename
        e_size, e_sum = existing.total_size, existing.checksum

    if e_dir != direction or e_name != filename:
        _refuse("transfer id conflict: metadata does not match")

    # A push we already finished, whose COMPLETE the sender never saw.
    if state == TransferState.COMPLETED and direction == TransferDirection.DOWNLOAD:
        dest_path = os.path.join(dest_dir, filename)
        if (os.path.isfile(dest_path) and checksum == e_sum
                and os.path.getsize(dest_path) == e_size
                and _compute_sha256(dest_path) == e_sum):
            session.send_json(
                FrameType.TRANSFER_ACCEPT,
                {"resume_offset": os.path.getsize(dest_path),
                 "already_complete": True, "sha256": e_sum,
                 "create_id": p.get('create_id')},
                transfer_id=tid,
            )
            log.info("Re-CREATE for %s: already complete", tid[:8])
            return existing, os.path.getsize(dest_path), dest_path
        _refuse("transfer id conflict: completed transfer does not match")

    if state not in (TransferState.CONNECTION_LOST, TransferState.PAUSED, TransferState.FAILED):
        _refuse(f"transfer cannot be recovered (state={state.value})")
    if state == TransferState.FAILED and p.get('retry') is not True:
        _refuse('failed transfer requires an explicit retry')
    if reattach_ok is not None and not reattach_ok(existing):
        _refuse("transfer is still active on the previous connection")

    if direction == TransferDirection.DOWNLOAD:
        # Peer pushes; the .part file on disk is the truth.
        if e_size != size:
            _refuse("transfer id conflict: size changed")
        if checksum and e_sum and checksum != e_sum:
            _refuse("transfer id conflict: checksum changed")
        dest_path = existing.dest_path or os.path.join(dest_dir, filename)
        part_path = existing.part_path or partial_path(dest_path, tid, getattr(existing, 'namespace', ''))
        resume_offset = os.path.getsize(part_path) if os.path.isfile(part_path) else 0
        if state == TransferState.FAILED and (getattr(existing, '_restart_required', False)
                                               or resume_offset == size):
            if os.path.isfile(part_path):
                with open(part_path, 'wb'):
                    pass
            resume_offset = 0
        if resume_offset > size:
            _refuse('partial file exceeds declared size')
        try:
            claim_destination(existing, dest_path, part_path, overwrite=existing.overwrite)
        except ValueError as exc:
            _refuse(str(exc))
        result_path = part_path
    else:
        # Peer pulls from us; it tells us what it already has.
        src_path = os.path.join(dest_dir, filename)
        if not os.path.isfile(src_path):
            _refuse("file not found")
        cur_size = os.path.getsize(src_path)
        if e_size and e_size != cur_size:
            _refuse("transfer id conflict: file changed since the transfer began")
        if e_sum and _compute_sha256(src_path) != e_sum:
            _refuse("transfer id conflict: file changed since the transfer began")
        resume_offset = max(0, min(int(p.get("have_offset", 0)), cur_size))
        result_path = src_path

    existing.update_progress(resume_offset)
    existing.chunk_size = p.get('chunk_size', DEFAULT_CHUNK_SIZE)
    if direction == TransferDirection.UPLOAD and _scoped_flow_supported(session):
        existing.peer_window = p.get('receive_window', config.TRANSFER_WINDOW_BYTES)
        existing.peer_global_window = p.get('global_window', config.GLOBAL_WINDOW_BYTES)
    router.register(tid)                       # on the NEW router
    if on_created is not None:
        on_created(existing, resume_offset, result_path)
    session.send_json(FrameType.TRANSFER_ACCEPT,
                      {"resume_offset": resume_offset, "size": existing.total_size,
                       "checksum": existing.checksum, "chunk_size": existing.chunk_size,
                       "receive_window": p.get('_receive_window'), "global_window": p.get('_global_window'),
                       "create_id": p.get('create_id')}, transfer_id=tid)
    log.info("Re-CREATE accepted for %s %r (resume_offset=%d)",
             tid[:8], filename, resume_offset)
    return existing, resume_offset, result_path


class SendEngine:
    """
    Sends one file transfer. Runs in a dedicated thread via run().

    Internal design
    ───────────────
    _run_inner() is the main loop: read chunk → send → wait for ACK → repeat.
    It is the SOLE consumer of the frame queue while actively transferring.

    A lightweight background thread (_cancel_watcher) only watches for
    inbound TRANSFER_CANCEL frames so cancel() from the peer works even
    while _run_inner is blocked in _wait_for_ack.

    pause_flag     — set by pause() or by an inbound PAUSE frame
    resume_event   — set by resume() or by an inbound RESUME frame
    cancel_event   — set by cancel() or by an inbound CANCEL frame

    Pause is deferred: _run_inner checks pause_flag between chunks,
    never mid-ACK-wait. _wait_for_ack detects an inbound PAUSE frame
    and records it, but does NOT abort the ACK wait.
    """

    def __init__(
        self,
        session: Session,
        transfer: Transfer,
        manager: TransferManager,
        frame_queue: queue.Queue,
        file_path: str,
        resume_offset: int = 0,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        progress_cb: Optional[Callable[[int, int], None]] = None,
        needs_resume_handshake: bool = False,
        persistence: Optional["PersistenceStore"] = None,
        flow_window: Optional[GlobalWindow] = None,
        transfer_window: Optional[TransferWindow] = None,
    ) -> None:
        self._session      = session
        self._transfer     = transfer
        self._manager      = manager
        self._queue        = frame_queue
        self._file_path    = file_path
        self._resume_offset = resume_offset
        self._chunk_size   = chunk_size
        self._progress_cb  = progress_cb
        self._persistence  = persistence
        # flow control: shared budget across every SendEngine on this
        # session (see flowcontrol.py) — bounds total outstanding
        # bytes across ALL concurrently active transfers, which
        # scheduling's max_concurrent alone does not (it caps transfer
        # *count*, not bytes). None means no shared budget is supplied.
        self._flow_window  = flow_window
        self._transfer_window = transfer_window

        # Set when this engine is standing in for a transfer that was
        # reconstructed from PersistenceStore after a restart (see
        # scheduler.resume_restored_upload) rather than one that just
        # went through a normal in-process pause. There is no already-
        # running peer-side wait loop counting on *us* to already know
        # the right offset — the peer paused a while ago and has been
        # sitting in its own _wait_for_resume ever since, so the first
        # thing this engine does is exactly what resume() + _wait_for_
        # resume() already do for an ordinary in-process resume: send
        # RESUME, then wait for the peer's confirmed offset. This is
        # deliberately not a new code path — it reuses resume_id
        # correlation, preemption-by-pause, and everything else about
        # that machinery that's already tested, just invoked once at
        # startup instead of in response to an external resume() call.
        self._needs_resume_handshake = needs_resume_handshake

        # recovery: set by abort_connection_lost() when the scheduler
        # learns the connection died. Unlike cancel(), this sends
        # nothing and leaves the transfer recoverable.
        self._conn_lost = False

        # Control signals (all thread-safe Events)
        self._pause_flag    = threading.Event()   # pause requested
        self._resume_event  = threading.Event()   # resume requested
        self._cancel_event  = threading.Event()   # cancel requested

        # The resume_id of the resume attempt we're currently waiting to
        # get confirmed (set right before _resume_event, by resume()).
        # _wait_for_resume's resume-confirmation stage only accepts a "resuming" ACK whose
        # echoed resume_id matches this — a delayed ACK from an earlier,
        # abandoned resume attempt (superseded by a new pause) must never
        # be mistaken for confirmation of the current one.
        self._resume_id: Optional[str] = None

    # ── Public API ────────────────────────────────────────────────────────

    def pause(self) -> None:
        """
        Request a pause. Sends TRANSFER_PAUSE to the peer.
        The transfer reaches PAUSED only at the next safe boundary
        (after the current in-flight chunk + ACK completes).
        """
        require_feature(self._session, 'pause')
        tid = self._transfer.id
        log.info("SendEngine %s: pause() called", tid[:8])
        try:
            with self._transfer.lock:
                self._pause_flag.set()
                self._manager.pause_transfer(tid)
        except (TransferNotFound, TransferStateError):
            pass
        try:
            self._session.send_json(FrameType.TRANSFER_PAUSE, {}, transfer_id=tid)
        except Exception as e:
            log.warning("SendEngine %s: could not send PAUSE: %s", tid[:8], e)

    def resume(self) -> None:
        """
        Resume after a pause. Sends TRANSFER_RESUME (carrying a fresh
        resume_id) to the peer. The peer replies with an ACK carrying
        the confirmed offset and echoing that same resume_id — only
        that reply will be accepted as confirmation (see
        _wait_for_resume). PAUSED -> ACTIVE happens on that
        confirmation, not here — resume() only kicks off the attempt.

        Sets _resume_event BEFORE sending the frame, not after. A
        reply to this RESUME (or a peer already mid-round-trip on a
        DIFFERENT resume) can arrive fast enough for resume-trigger stage's "early
        ACK" handling to match and fully complete this exact attempt
        (clearing _resume_event) before this function's own `.set()`
        line would otherwise run — that out-of-order `.set()` then
        left the event incorrectly armed for the NEXT resume cycle,
        with this attempt's now-stale resume_id, so a later real
        resume's genuine confirmation would be discarded as a
        "mismatch" while the client sat waiting on an ID nothing will
        ever reply to again. Setting the event first makes that
        ordering impossible: a reply cannot exist before its request
        has even been sent, so _resume_event.set() always happens-
        before anything that could observe or clear it.
        """
        require_feature(self._session, 'resume')
        tid = self._transfer.id
        resume_id = uuid.uuid4().hex[:12]
        log.info("SendEngine %s: resume() called (resume_id=%s)", tid[:8], resume_id)
        with self._transfer.lock:
            if self._transfer.state == TransferState.PAUSED:
                self._pause_flag.clear()
            self._resume_id = resume_id
            self._resume_event.set()
        try:
            self._session.send_json(
                FrameType.TRANSFER_RESUME, {"resume_id": resume_id}, transfer_id=tid
            )
        except Exception as e:
            log.warning("SendEngine %s: could not send RESUME: %s", tid[:8], e)

    def cancel(self, notify_peer: bool = True) -> None:
        """Cancel this transfer. Session stays open."""
        if notify_peer:
            require_feature(self._session, 'cancel')
        tid = self._transfer.id
        log.info("SendEngine %s: cancel() called", tid[:8])
        with self._transfer.lock:
            if self._transfer.state in (TransferState.COMPLETED, TransferState.FAILED):
                return
            self._cancel_event.set()
            self._manager.cancel_transfer(tid)
        if notify_peer:
            try:
                self._session.send_transfer_cancel(tid, 'cancelled by sender')
            except Exception:
                pass

    def abort_connection_lost(self) -> None:
        """recovery: the connection is gone. Unwind every blocking wait
        (they all poll _cancel_event) WITHOUT sending anything or
        touching transfer state — run() then moves the transfer to
        CONNECTION_LOST instead of CANCELLED/FAILED."""
        self._conn_lost = True
        self._cancel_event.set()

    def _connection_dead(self) -> bool:
        return self._conn_lost or self._session.is_lost

    def _mark_conn_lost(self) -> None:
        tid = self._transfer.id
        try:
            self._manager.mark_connection_lost(tid)
        except TransferNotFound:
            pass

    # ── Engine entry point ────────────────────────────────────────────────

    def run(self) -> None:
        tid = self._transfer.id
        # Lightweight cancel watcher — watches only for inbound CANCEL frames
        # while _run_inner blocks in _wait_for_ack.
        watcher = threading.Thread(
            target=self._cancel_watcher, daemon=True,
            name=f"SendWatcher-{tid[:8]}"
        )
        watcher.start()
        try:
            self._run_inner()
            if self._conn_lost:
                self._mark_conn_lost()
        except TransferError as e:
            if self._connection_dead():
                log.warning("SendEngine %s: connection lost (%s)", tid[:8], e)
                self._mark_conn_lost()
            else:
                log.error("SendEngine %s failed: %s", tid[:8], e)
                try:
                    self._session.send_transfer_error(tid, str(e))
                except Exception:
                    pass
                try:
                    self._manager.fail_transfer(tid, str(e))
                except Exception:
                    pass
        except Exception as e:
            if self._connection_dead():
                log.warning("SendEngine %s: connection lost (%s)", tid[:8], e)
                self._mark_conn_lost()
            else:
                log.exception("SendEngine %s unexpected error", tid[:8])
                try:
                    self._session.send_transfer_error(tid, f'unexpected: {e}')
                except Exception:
                    pass
                try:
                    self._manager.fail_transfer(tid, f"unexpected: {e}")
                except Exception:
                    pass
        finally:
            self._cancel_event.set()   # stop watcher
            watcher.join(timeout=2)

    def _cancel_watcher(self) -> None:
        """
        Background thread: watches only for inbound TRANSFER_CANCEL frames.

        Uses a separate peek-and-requeue approach only when the main
        thread is not consuming — but actually the simplest correct
        approach is: this thread does NOT consume from the main queue at
        all. Instead, it relies on the cancel_event being set by the
        public cancel() method or by _run_inner detecting CANCEL inline.

        This thread only handles one case: the peer sends CANCEL while
        _run_inner is blocked waiting for an ACK or COMPLETE. In that
        case _run_inner itself detects the CANCEL frame in _wait_for_ack
        and _wait_for_complete, so this watcher is actually not needed
        for inbound cancels — _run_inner handles them.

        The watcher's only remaining job: signal the cancel_event if
        cancel() is called externally (already done by cancel() itself),
        so this thread just waits and exits.
        """
        while not self._cancel_event.is_set():
            time.sleep(0.1)

    def _run_inner(self) -> None:
        tid       = self._transfer.id
        if self._cancel_event.is_set() or self._transfer.state == TransferState.CANCELLED:
            return
        offset    = self._resume_offset
        seq       = 0

        if self._needs_resume_handshake:
            log.info(
                "SendEngine %s: restored transfer — performing initial "
                "resume handshake before sending (last-known offset=%d)",
                tid[:8], offset,
            )
            self.resume()
            confirmed = self._wait_for_resume(tid, offset)
            if confirmed is None:
                return  # cancelled during the handshake
            offset = confirmed
            log.info("SendEngine %s: handshake confirmed offset=%d",
                     tid[:8], offset)

        if not os.path.isfile(self._file_path):
            raise TransferError(f"Source file not found: {self._file_path!r}")

        file_size = os.path.getsize(self._file_path)
        if file_size != self._transfer.total_size:
            raise TransferError('Source size changed since transfer creation')
        if not 0 <= offset <= file_size:
            raise TransferError('Resume offset is outside the source')
        if self._transfer.checksum is None:
            self._transfer.checksum = _compute_sha256(self._file_path)
        self._transfer.update_progress(offset, total_size=file_size)
        _checksum(self._transfer.checksum)
        self._outstanding_start = offset
        self._outstanding_end = offset
        log.info("SendEngine %s: sending %r from offset %d (%d bytes)",
                 tid[:8], self._file_path, offset, file_size)

        with open(self._file_path, "rb") as fh:
            fh.seek(offset)

            if offset >= file_size:
                # Nothing to send — either a genuinely empty file, or
                # a resume that's already at EOF. The receiver still
                # needs exactly one FINAL_CHUNK DATA frame (even an
                # empty one) to know the transfer is done. Without
                # it, both sides deadlock until their own unrelated
                # timeouts fire: we'd block in _wait_for_complete()
                # forever since the receiver never got a signal to
                # finalize and reply, and the receiver blocks in
                # _dequeue() forever since no DATA frame — the only
                # thing it's listening for — ever arrives.
                payload = _encode_data_payload(offset, b"")
                frame = Frame(
                    frame_type=FrameType.TRANSFER_DATA,
                    payload=payload, transfer_id=tid, sequence=seq,
                    flags=FrameFlags.FINAL_CHUNK,
                )
                for attempt in range(1 + MAX_CHUNK_RETRIES):
                    frame.payload = _encode_data_payload(offset, b'')
                    self._session.send(frame)
                    try:
                        ack_result = self._wait_for_ack(tid, seq, offset)
                    except AckTimeout:
                        if attempt == MAX_CHUNK_RETRIES:
                            raise
                        continue
                    if ack_result is None or not ack_result[2]:
                        break
                    if attempt == MAX_CHUNK_RETRIES:
                        raise TransferError('Empty final chunk retry budget exhausted')
                if ack_result is None:
                    return  # cancelled
                acked_offset, _peer_paused, retry_requested = ack_result
                if retry_requested:
                    raise TransferError(
                        f"Unexpected retry request for empty final chunk "
                        f"(transfer={tid[:8]})"
                    )
                offset = acked_offset
                self._transfer.update_progress(offset)
                if self._progress_cb:
                    try:
                        self._progress_cb(offset, file_size)
                    except Exception:
                        pass
            else:
                while True:
                    # ── cancel check ─────────────────────────────────────
                    if self._cancel_event.is_set():
                        log.info("SendEngine %s: cancelled at offset %d", tid[:8], offset)
                        return

                    # ── pause check (safe-point: between chunks only) ─────
                    if self._pause_flag.is_set():
                        self._pause_flag.clear()

                        # Guard: if the transfer already finished before we
                        # processed the pause flag, skip it entirely.
                        with self._transfer.lock:
                            cur = self._transfer.state
                        if cur in TERMINAL_STATES:
                            log.debug("SendEngine %s: late pause ignored (state=%s)",
                                      tid[:8], cur.value)
                        else:
                            log.info("SendEngine %s: paused at offset %d", tid[:8], offset)
                            try:
                                self._manager.confirm_paused(tid)
                            except (TransferNotFound, TransferStateError):
                                pass

                            # Block until resume() or cancel()
                            resumed_offset = self._wait_for_resume(tid, offset)
                            if resumed_offset is None:
                                return  # cancelled during pause

                            if resumed_offset != offset:
                                log.info("SendEngine %s: resume offset %d → %d",
                                         tid[:8], offset, resumed_offset)
                                offset = resumed_offset
                            # Always re-sync the file position with
                            # `offset` after a resume, not only when
                            # the offset changed: belt and braces
                            # against any path that reaches here with
                            # fh ahead of offset.
                            fh.seek(offset)
                            log.info("SendEngine %s: resumed at offset %d", tid[:8], offset)

                    # ── read chunk ───────────────────────────────────────
                    read_size = self._chunk_size
                    if self._transfer_window is not None and self._flow_window is not None and _scoped_flow_supported(self._session):
                        read_size = min(read_size, self._transfer_window.capacity, self._flow_window.capacity)
                    chunk = fh.read(read_size)
                    if not chunk:
                        break   # EOF

                    next_offset = offset + len(chunk)
                    is_final    = next_offset >= file_size
                    chunk_len   = len(chunk)

                    # Reserve both budgets. Updated scoped-flow peers can
                    # use smaller DATA chunks after a shrink, so a window
                    # below the original chunk size cannot deadlock them.
                    if self._flow_window is not None:
                        acquired = False
                        transfer_acquired = False
                        while True:
                            if self._cancel_event.is_set():
                                return
                            if self._pause_flag.is_set():
                                break
                            self._drain_window_updates()
                            if self._transfer_window is not None and _scoped_flow_supported(self._session):
                                bound = min(self._transfer_window.capacity, self._flow_window.capacity)
                                if len(chunk) > bound:
                                    chunk = chunk[:bound]
                                    chunk_len = len(chunk)
                                    next_offset = offset + chunk_len
                                    is_final = next_offset == file_size
                                    fh.seek(next_offset)
                            if self._transfer_window is not None:
                                transfer_acquired = self._transfer_window.acquire(chunk_len,
                                    cancel_event=self._cancel_event, pause_flag=self._pause_flag, timeout=0.1)
                                if not transfer_acquired:
                                    continue
                            if self._flow_window.acquire(chunk_len, cancel_event=self._cancel_event,
                                                         pause_flag=self._pause_flag, timeout=0.1):
                                acquired = True
                                break
                            if transfer_acquired:
                                self._transfer_window.release(chunk_len)
                                transfer_acquired = False
                        if not acquired:
                            fh.seek(offset)
                            continue

                    try:
                        # retry: retry loop — resend this chunk up to
                        # MAX_CHUNK_RETRIES times if the receiver reports a
                        # checksum error for it.
                        chunk_offset_for_retry = offset   # where to seek on retry
                        # Retries reuse the exact immutable bytes and checksum.
                        payload = _encode_data_payload(offset, chunk)
                        for attempt in range(1 + MAX_CHUNK_RETRIES):
                            if attempt > 0:
                                log.warning(
                                    "SendEngine %s: retransmitting chunk seq=%d "
                                    "offset=%d (attempt %d/%d)",
                                    tid[:8], seq, chunk_offset_for_retry,
                                    attempt, MAX_CHUNK_RETRIES,
                                )
                                # Retransmit the exact bytes already hashed,
                                # never a newly-read, possibly changed range.

                            frame = Frame(
                                frame_type=FrameType.TRANSFER_DATA,
                                payload=payload,
                                transfer_id=tid,
                                sequence=seq,
                                flags=(FrameFlags.FINAL_CHUNK if is_final else FrameFlags.NONE),
                            )

                            try:
                                self._session.send(frame)
                            except OSError as e:
                                raise TransferError(
                                    f"Send failed at offset {offset}: {e}"
                                ) from e

                            # ── wait for ACK or retry request ─────────────────
                            self._outstanding_start = offset
                            self._outstanding_end = next_offset
                            try:
                                ack_result = self._wait_for_ack(tid, seq, next_offset)
                            except AckTimeout:
                                if attempt == MAX_CHUNK_RETRIES:
                                    raise
                                continue
                            if ack_result is None:
                                return  # cancelled

                            acked_offset, peer_paused, retry_requested = ack_result

                            if retry_requested:
                                # Receiver detected a chunk checksum error — retry
                                if attempt == MAX_CHUNK_RETRIES:
                                    raise TransferError(
                                        f"Chunk seq={seq} failed checksum after "
                                        f"{MAX_CHUNK_RETRIES} retransmissions"
                                    )
                                continue  # retry loop

                            # ACK accepted — advance
                            offset = acked_offset
                            seq   += 1

                            self._transfer.update_progress(offset)
                            if self._progress_cb:
                                try:
                                    self._progress_cb(offset, file_size)
                                except Exception:
                                    pass
                            if self._persistence:
                                try:
                                    self._persistence.save(self._transfer,
                                        part_path=self._transfer.part_path, dest_path=self._transfer.dest_path,
                                        retry_count=self._transfer.attempt)
                                except Exception:
                                    pass

                            if peer_paused:
                                log.info(
                                    "SendEngine %s: peer signalled paused in ACK", tid[:8]
                                )
                                self._pause_flag.set()
                            break  # chunk done — exit retry loop
                    finally:
                        if self._flow_window is not None:
                            self._flow_window.release(chunk_len)
                            if transfer_acquired:
                                self._transfer_window.release(chunk_len)

        if os.path.getsize(self._file_path) != file_size or _compute_sha256(self._file_path) != self._transfer.checksum:
            raise TransferError('Source checksum changed during transfer')
        # ── all chunks sent — wait for TRANSFER_COMPLETE ──────────────────
        log.debug("SendEngine %s: all chunks sent, waiting for COMPLETE", tid[:8])
        result = self._wait_for_complete(tid)
        if result is None:
            return

        with self._transfer.lock:
            if self._transfer.state == TransferState.CANCELLED:
                return
            if _checksum(result.get('sha256')) != self._transfer.checksum:
                raise TransferError('Completion checksum does not match the source')
            if 'size' in result and _integer(result['size'], 'completion size') != file_size:
                raise TransferError('Completion size does not match the source')
            self._manager.complete_transfer(tid, checksum=self._transfer.checksum)
        log.info("SendEngine %s: completed sha256=%s",
                 tid[:8], result.get("sha256", "")[:12])

    def _drain_window_updates(self) -> None:
        """
        Non-blockingly pull any WINDOW_UPDATE frames currently sitting
        in the queue and apply them to self._flow_window. Everything
        else found is put back untouched — this exists only to unstick
        a blocked flow_window.acquire() call (see its call site above),
        not to do any of the normal frame processing _wait_for_ack/
        _wait_for_resume are responsible for.
        """
        if self._flow_window is None:
            return
        pending = []
        try:
            while True:
                frame = self._queue.get_nowait()
                if frame.frame_type == FrameType.WINDOW_UPDATE:
                    self._apply_window_update(frame)
                else:
                    pending.append(frame)
        except queue.Empty:
            pass
        for f in pending:
            try:
                self._queue.put_nowait(f)
            except queue.Full:
                pass

    def _apply_window_update(self, frame):
        payload = frame.json()
        capacity = validate_capacity(payload.get('capacity'))
        scope = payload.get('scope', 'global')
        if scope == 'transfer':
            if not _scoped_flow_supported(self._session):
                raise TransferError('scoped flow control was not negotiated')
            budget = self._transfer_window
        elif scope == 'global':
            budget = self._flow_window
        else:
            raise TransferError('invalid window scope')
        if budget is not None:
            budget.resize(min(budget.capacity, capacity) if payload.get('automatic') else capacity)

    def _wait_for_ack(
        self, tid: str, seq: int, expected_offset: int
    ) -> Optional[tuple[int, bool, bool]]:
        """
        Wait for TRANSFER_ACK for the given sequence.

        Returns (acked_offset, peer_paused, retry_requested) or None if
        cancelled.

        retry_requested=True (retry): receiver detected a chunk checksum
        error and sent TRANSFER_ERROR with retry_seq == seq.  Caller should
        retransmit the chunk and call _wait_for_ack again.

        peer_paused=True: ACK carried paused=True; pause is acted on at
        the next loop iteration (safe-point).

        TRANSFER_COMPLETE arriving here is stashed and treated as a final
        implicit ACK (receiver finished all in-flight chunks during pause).

        TRANSFER_PAUSE is recorded in pause_flag but does NOT abort the
        wait — safe-point design; never drop an in-flight chunk.
        """
        deadline = time.monotonic() + ACK_TIMEOUT_SECONDS
        while True:
            if self._cancel_event.is_set():
                return None

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AckTimeout(
                    f"ACK timeout seq={seq} transfer={tid[:8]}"
                )
            try:
                frame = self._queue.get(timeout=min(remaining, 0.2))
            except queue.Empty:
                continue

            ft = frame.frame_type

            if ft == FrameType.TRANSFER_ACK:
                p = frame.json()
                if p.get('resuming'):
                    continue
                offset = _integer(p.get('offset'), 'ACK offset')
                if offset > expected_offset:
                    raise TransferError('ACK offset is beyond the outstanding chunk')
                if offset < expected_offset:
                    if p.get('paused'):
                        self._pause_flag.set()
                    continue
                if (getattr(self._session, 'negotiation', None) is not None
                        and self._session.negotiation.protocol_version >= 3
                        and not p.get('paused') and frame.sequence != seq):
                    raise TransferError('ACK sequence does not match the outstanding chunk')
                return (
                    offset,
                    bool(p.get("paused", False)),
                    False,
                )

            if ft == FrameType.TRANSFER_COMPLETE:
                if expected_offset != self._transfer.total_size:
                    raise TransferError('Premature completion before the final chunk')
                log.debug("SendEngine %s: TRANSFER_COMPLETE arrived during "
                          "ACK wait — stashing for _wait_for_complete", tid[:8])
                try:
                    self._queue.put_nowait(frame)
                except queue.Full:
                    pass
                return expected_offset, False, False

            if ft == FrameType.TRANSFER_RETRY:
                if not _range_retry_supported(self._session):
                    raise TransferError('Range retry was not negotiated')
                p = frame.json()
                start = _integer(p.get('offset'), 'retry offset')
                end = _integer(p.get('end'), 'retry end')
                requested_seq = _integer(p.get('sequence'), 'retry sequence', (1 << 32) - 1)
                if (start != self._outstanding_start or end != expected_offset
                        or requested_seq != seq or frame.sequence != seq):
                    raise TransferError('Retry range does not match the outstanding chunk')
                return expected_offset, False, True

            if ft == FrameType.TRANSFER_ERROR:
                p = frame.json()
                retry_seq = p.get("retry_seq")
                if (type(retry_seq) is int and retry_seq == seq
                        and type(p.get('offset')) is int
                        and p['offset'] == getattr(self, '_outstanding_start', expected_offset)):
                    # retry: chunk checksum error — signal retry
                    log.warning(
                        "SendEngine %s: chunk seq=%d checksum error — "
                        "will retransmit (%s)",
                        tid[:8], seq, p.get("error", ""),
                    )
                    return expected_offset, False, True
                # Fatal error (different seq or unrecoverable)
                raise TransferError(
                    f"Peer error: {p.get('error', 'unknown')}"
                )

            if ft == FrameType.TRANSFER_CANCEL:
                log.info("SendEngine %s: CANCEL while waiting for ACK", tid[:8])
                self._cancel_event.set()
                try:
                    self._session.send_transfer_cancel(tid, "cancel acknowledged")
                except Exception:
                    pass
                try:
                    self._manager.cancel_transfer(tid)
                except (TransferNotFound, TransferStateError):
                    pass
                return None

            if ft == FrameType.WINDOW_UPDATE:
                # flow control: receiver-driven flow control. Applies
                # immediately and keeps waiting — this is advisory
                # bookkeeping, not something that should interrupt an
                # in-flight chunk's own ACK wait.
                self._apply_window_update(frame)
                continue

            if ft == FrameType.TRANSFER_PAUSE:
                log.info("SendEngine %s: PAUSE received while waiting for ACK seq=%d"
                         " — will pause after this chunk", tid[:8], seq)
                with self._transfer.lock:
                    cur = self._transfer.state
                if cur == TransferState.ACTIVE:
                    try:
                        self._manager.pause_transfer(tid)
                    except (TransferNotFound, TransferStateError):
                        pass
                self._pause_flag.set()
                continue  # safe-point — keep waiting for ACK

            if ft == FrameType.TRANSFER_RESUME:
                log.debug("SendEngine %s: stray RESUME while waiting for ACK", tid[:8])
                try:
                    self._session.send_json(
                        FrameType.TRANSFER_ACK,
                        {
                            "offset": expected_offset,
                            "resuming": True,
                            "resume_id": frame.json().get("resume_id"),
                        },
                        transfer_id=tid,
                    )
                except Exception:
                    pass
                continue

            log.debug("SendEngine %s: unexpected %s while waiting for ACK",
                      tid[:8], ft.name)

    def _wait_for_resume(self, tid: str, current_offset: int) -> Optional[int]:
        """
        Block in PAUSED state until a resume is triggered AND confirmed.

        State contract (this is the part that changed): PAUSED -> ACTIVE
        happens ONLY on receiving a "resuming" ACK whose resume_id
        matches the resume attempt we're currently waiting on — never
        just on sending/seeing a RESUME. Until that confirmation
        arrives, the manager-visible state stays PAUSED. This makes
        ACTIVE actually mean "the resume handshake is fully settled and
        we have the receiver-confirmed offset", not just "someone asked
        to resume".

        That, in turn, is what makes a new pause() preemptive: since we
        haven't touched the manager yet, a pause_flag set while we're
        still waiting for confirmation needs no state-machine work to
        honor — we just abandon this attempt and go back to waiting for
        the next resume trigger. No ACK_TIMEOUT_SECONDS wait, because
        there's nothing to preserve: no data is in flight during a
        resume handshake (the safe-point invariant), so the abandoned
        attempt leaves nothing dangling.

        Structure: an outer loop that can restart at "wait for a resume
        trigger" (resume-trigger stage) any number of times — once per abandoned
        attempt — each iteration doing resume-trigger stage (find a trigger, learn
        its resume_id) then resume-confirmation stage (wait for that trigger's matching
        confirmation, watching for a new pause the whole time).

        Returns the confirmed offset from the receiver, or None if
        cancelled.
        """
        log.debug("SendEngine %s: waiting for resume at offset %d",
                  tid[:8], current_offset)

        while True:
            # ── resume-trigger stage: wait for a resume trigger (local or peer) ────────
            expected_resume_id: Optional[str] = None
            while not self._cancel_event.is_set():
                if self._resume_event.is_set():
                    self._resume_event.clear()
                    expected_resume_id = self._resume_id
                    log.debug("SendEngine %s: local resume() fired (resume_id=%s)",
                              tid[:8], expected_resume_id)
                    break

                try:
                    frame = self._queue.get(timeout=0.2)
                except queue.Empty:
                    continue

                ft = frame.frame_type

                if ft == FrameType.TRANSFER_ERROR:
                    raise TransferError(f"Peer error while paused: {frame.json().get('error', 'unknown')}")
                if ft == FrameType.TRANSFER_RESUME:
                    expected_resume_id = frame.json().get("resume_id")
                    log.info("SendEngine %s: inbound RESUME from peer "
                             "(resume_id=%s)", tid[:8], expected_resume_id)
                    break

                if ft == FrameType.TRANSFER_CANCEL:
                    log.info("SendEngine %s: CANCEL while paused", tid[:8])
                    self._cancel_event.set()
                    try:
                        self._manager.cancel_transfer(tid)
                    except (TransferNotFound, TransferStateError):
                        pass
                    return None

                if ft == FrameType.TRANSFER_COMPLETE:
                    # Receiver finished all chunks (written in-flight
                    # during pause) before any resume trigger showed up.
                    # Stash it; _wait_for_complete picks it up right
                    # after we return. We're still PAUSED here — that's
                    # fine, PAUSED -> COMPLETED is a legal transition
                    # for exactly this race (see transfer.py).
                    log.debug("SendEngine %s: TRANSFER_COMPLETE arrived "
                              "while paused — stashing", tid[:8])
                    try:
                        self._queue.put_nowait(frame)
                    except queue.Full:
                        pass
                    return current_offset

                if ft == FrameType.TRANSFER_ACK:
                    # queue.get() isn't synchronized with resume_event —
                    # if this thread was already blocked here when
                    # resume() ran, the receiver's reply to THAT call
                    # can be dequeued before the next iteration even
                    # checks resume_event.is_set(). Since the receiver
                    # only sends its resuming ACK once, discarding it
                    # unconditionally here would strand resume-confirmation stage waiting
                    # up to ACK_TIMEOUT_SECONDS for a reply that already
                    # came and went. Accept it as both trigger and
                    # confirmation in one step — but ONLY when it's
                    # unambiguously the reply to OUR most recently
                    # initiated attempt (self._resume_id); anything else
                    # (no resume_id, or one that doesn't match — e.g. a
                    # stale reply to an attempt we've since abandoned)
                    # is exactly the "must never reactivate" case the
                    # resume_id correlation exists to prevent, so it
                    # stays discarded.
                    p = frame.json()
                    if (p.get("resuming") and self._resume_id is not None
                            and p.get("resume_id") == self._resume_id):
                        self._resume_event.clear()
                        confirmed = p.get("offset", current_offset)
                        log.debug(
                            "SendEngine %s: resuming ACK for our own attempt "
                            "(resume_id=%s) arrived before we even checked "
                            "resume_event — accepting, offset=%d",
                            tid[:8], self._resume_id, confirmed,
                        )
                        _integer(confirmed, 'resume offset', self._transfer.total_size)
                        admitted = _resume_with_slot(self, tid)
                        if admitted is None:
                            if self._resume_id == p.get('resume_id'):
                                self._resume_id = None
                            continue
                        if not admitted:
                            return None
                        return confirmed
                    log.debug(
                        "SendEngine %s: discarding stale/non-matching ACK "
                        "while waiting for a trigger (resume_id=%s, ours=%s)",
                        tid[:8], p.get("resume_id"), self._resume_id,
                    )
                    continue

                # Any other frame — including TRANSFER_PAUSE (redundant,
                # we're already paused) — is simply not a resume
                # trigger. Discard and keep waiting.
                log.debug("SendEngine %s: discarding %s while paused", tid[:8], ft.name)

            if self._cancel_event.is_set():
                return None

            # If the peer initiated, it's already sent RESUME and will
            # follow with its own resuming ACK once it can — no reply
            # needed from us to unblock it (RecvEngine's own resume path
            # doesn't wait for anything back from the sender).

            # ── resume-confirmation stage: wait for the confirming ACK for THIS attempt ─────
            # Deliberately does NOT call manager.resume_transfer() before
            # this point (see docstring above) — PAUSED is still the
            # correct manager-visible state for the whole of resume-confirmation stage.
            deadline = time.monotonic() + ACK_TIMEOUT_SECONDS
            preempted = False
            while not self._cancel_event.is_set():
                # Check before every wait...
                if self._pause_flag.is_set():
                    preempted = True
                    break

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TransferError('Timed out waiting for confirmed resume offset')

                try:
                    frame = self._queue.get(timeout=min(remaining, 0.2))
                except queue.Empty:
                    continue

                # ...and after every frame, before acting on it — a pause
                # can arrive interleaved with unrelated traffic.
                if self._pause_flag.is_set():
                    preempted = True
                    # Put the frame back — resume-trigger stage (next loop iteration)
                    # or the main loop after we eventually resume may
                    # still need it (e.g. a fresh DATA frame).
                    try:
                        self._queue.put_nowait(frame)
                    except queue.Full:
                        pass
                    break

                ft = frame.frame_type

                if ft == FrameType.TRANSFER_ACK:
                    p = frame.json()
                    if p.get("resuming") and p.get("resume_id") == expected_resume_id:
                        confirmed = p.get("offset", current_offset)
                        log.debug("SendEngine %s: resuming ACK confirmed "
                                  "(resume_id=%s) offset=%d",
                                  tid[:8], expected_resume_id, confirmed)
                        _integer(confirmed, 'resume offset', self._transfer.total_size)
                        admitted = _resume_with_slot(self, tid)
                        if admitted is None:
                            preempted = True
                            break
                        if not admitted:
                            return None
                        return confirmed
                    # Either not a "resuming" ACK, or resume_id doesn't
                    # match the attempt we're currently waiting on — a
                    # stale confirmation from an attempt we've since
                    # abandoned must never be mistaken for this one.
                    log.debug(
                        "SendEngine %s: discarding stale/non-matching ACK "
                        "(resume_id=%s, expected=%s)",
                        tid[:8], p.get("resume_id"), expected_resume_id,
                    )
                    continue

                if ft == FrameType.TRANSFER_ERROR:
                    raise TransferError(f"Peer error during resume: {frame.json().get('error', 'unknown')}")
                if ft == FrameType.TRANSFER_COMPLETE:
                    log.debug("SendEngine %s: TRANSFER_COMPLETE arrived "
                              "during resume confirmation wait — stashing",
                              tid[:8])
                    try:
                        self._queue.put_nowait(frame)
                    except queue.Full:
                        pass
                    return current_offset

                if ft == FrameType.TRANSFER_CANCEL:
                    self._cancel_event.set()
                    try:
                        self._manager.cancel_transfer(tid)
                    except (TransferNotFound, TransferStateError):
                        pass
                    return None

                if ft == FrameType.TRANSFER_RESUME:
                    # A newer RESUME (e.g. the peer independently
                    # re-paused/re-resumed). Adopt it as the attempt
                    # we're now waiting on rather than discarding it.
                    new_id = frame.json().get("resume_id")
                    log.debug("SendEngine %s: newer RESUME superseding "
                              "resume_id=%s -> %s", tid[:8],
                              expected_resume_id, new_id)
                    expected_resume_id = new_id
                    deadline = time.monotonic() + ACK_TIMEOUT_SECONDS
                    continue

                if ft == FrameType.TRANSFER_PAUSE:
                    # Peer-initiated pause, wire-delivered rather than
                    # via our own pause_flag — same abandonment as the
                    # local case.
                    log.info("SendEngine %s: peer PAUSE preempted resume "
                             "confirmation (resume_id=%s)",
                             tid[:8], expected_resume_id)
                    preempted = True
                    break

                log.debug("SendEngine %s: discarding %s while waiting for "
                          "resuming ACK", tid[:8], ft.name)

            if self._cancel_event.is_set():
                return None

            if preempted:
                self._pause_flag.clear()
                if self._resume_id == expected_resume_id:
                    # Invalidate it — otherwise resume-trigger stage's early-ACK
                    # fast path (which trusts anything matching
                    # self._resume_id, precisely to catch a reply that
                    # arrives before resume_event is even checked; see
                    # above) would just as happily accept THIS stale,
                    # abandoned attempt's late reply if it wanders in
                    # afterward, silently reactivating a transfer we've
                    # already moved on from. There's no "current
                    # attempt" once abandoned, so nothing should match
                    # until a fresh resume() sets a new id.
                    self._resume_id = None
                log.info(
                    "SendEngine %s: resume attempt (resume_id=%s) abandoned "
                    "— still paused at offset %d, waiting for next trigger",
                    tid[:8], expected_resume_id, current_offset,
                )
                continue  # back to resume-trigger stage — still legitimately PAUSED

            # Unreachable in practice (every resume-confirmation stage exit path above
            # returns or continues) but keeps the loop well-formed.
            return current_offset

    def _wait_for_complete(self, tid: str) -> Optional[dict]:
        """Wait for TRANSFER_COMPLETE from receiver."""
        deadline = time.monotonic() + ACK_TIMEOUT_SECONDS * 2
        while True:
            if self._cancel_event.is_set():
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TransferError(
                    f"Timed out waiting for TRANSFER_COMPLETE ({tid[:8]})"
                )
            try:
                frame = self._queue.get(timeout=min(remaining, 0.2))
            except queue.Empty:
                continue

            ft = frame.frame_type

            if ft == FrameType.TRANSFER_COMPLETE:
                return frame.json()

            if ft == FrameType.TRANSFER_CANCEL:
                self._cancel_event.set()
                try:
                    self._manager.cancel_transfer(tid)
                except (TransferNotFound, TransferStateError):
                    pass
                return None

            if ft == FrameType.TRANSFER_ERROR:
                raise TransferError(
                    f"Peer error after final chunk: "
                    f"{frame.json().get('error', 'unknown')}"
                )

            log.debug("SendEngine %s: unexpected %s while waiting for COMPLETE",
                      tid[:8], ft.name)


# ── RecvEngine ────────────────────────────────────────────────────────────────

class RecvEngine:
    """
    Receives one file transfer. Runs in a dedicated thread via run().

    _run_inner() is the main loop: pull DATA frames from queue, write
    to .part file, send ACK, repeat. It is also the SOLE consumer of
    the frame queue.

    Pause handling
    ──────────────
    Sender-initiated: TRANSFER_PAUSE arrives in the queue during the
      main loop. RecvEngine finishes writing the current DATA frame
      (if mid-write — actually queue-ordering means we see PAUSE only
      between DATA frames), sends ACK{paused:True}, then enters
      _wait_for_resume().

    Receiver-initiated: pause() sends TRANSFER_PAUSE to the sender,
      sets pause_flag. The main loop checks pause_flag between DATA
      frames and enters _wait_for_resume().

    In _wait_for_resume():
      - Any DATA frames that arrive (in-flight from before the sender
        processed PAUSE) are written and hashed immediately. This keeps
        the .part file and hasher in sync.
      - When TRANSFER_RESUME arrives (or resume() is called), sends
        ACK{offset:N, resuming:True} and returns.
    """

    def __init__(
        self,
        session: Session,
        transfer: Transfer,
        manager: TransferManager,
        frame_queue: queue.Queue,
        part_path: str,
        resume_offset: int = 0,
        delete_part_on_cancel: bool = True,
        progress_cb: Optional[Callable[[int, int], None]] = None,
        persistence: Optional["PersistenceStore"] = None,
        needs_resume_handshake: bool = False,
        receive_limits=None,
    ) -> None:
        self._session      = session
        self._transfer     = transfer
        self._manager      = manager
        self._queue        = frame_queue
        self._part_path    = part_path
        self._resume_offset = resume_offset
        self._delete_part  = delete_part_on_cancel
        self._progress_cb  = progress_cb
        self._persistence  = persistence
        self._receive_limits = receive_limits

        # persistence: set when this engine stands in for a download that
        # was reconstructed from PersistenceStore after a receiver-side
        # restart (see scheduler.resume_restored_download). The sender
        # has been sitting in its own paused wait ever since, so the
        # first thing we do is the ordinary receiver-initiated resume:
        # send RESUME, then answer with our confirmed offset in a
        # "resuming" ACK. Deliberately reuses that already-tested path
        # (resume_id correlation, preemption) rather than adding a new
        # wire message.
        self._needs_resume_handshake = needs_resume_handshake

        # recovery: see SendEngine._conn_lost.
        self._conn_lost = False

        self._pause_flag    = threading.Event()
        self._resume_event  = threading.Event()
        self._cancel_event  = threading.Event()

        # The resume_id of a resume attempt WE initiated (receiver-
        # initiated resume) — echoed back in our own "resuming" ACK so
        # the sender's resume-confirmation stage can correlate it. See SendEngine's
        # _resume_id for the full rationale.
        self._resume_id: Optional[str] = None

        # Confirmed offset — updated after each successful chunk write.
        # Protected by _offset_lock for thread-safe reads from pause()/resume().
        self._confirmed_offset: int = resume_offset
        self._offset_lock = threading.Lock()
        self._next_sequence = None if resume_offset else 0
        self._last_chunk = None
        self._final_received = False
        self._bad_chunk_attempts = 0
        self._dest_path = transfer.dest_path or destination_for_part(part_path, transfer.id)

    # ── Public API ────────────────────────────────────────────────────────

    def pause(self) -> None:
        """Request a pause from the receiver side. Sends TRANSFER_PAUSE."""
        require_feature(self._session, 'pause')
        tid = self._transfer.id
        log.info("RecvEngine %s: pause() called", tid[:8])
        try:
            with self._transfer.lock:
                self._pause_flag.set()
                self._manager.pause_transfer(tid)
        except (TransferNotFound, TransferStateError):
            pass
        try:
            self._session.send_json(FrameType.TRANSFER_PAUSE, {}, transfer_id=tid)
        except Exception as e:
            log.warning("RecvEngine %s: could not send PAUSE: %s", tid[:8], e)

    def resume(self) -> None:
        """Resume after a pause. Sends TRANSFER_RESUME carrying a fresh
        resume_id, echoed back in our own resuming ACK.

        Sets _resume_event before sending, same rationale as
        SendEngine.resume() — makes it impossible for anything racing
        this call to observe a reply before the request was even sent.
        """
        require_feature(self._session, 'resume')
        tid = self._transfer.id
        resume_id = uuid.uuid4().hex[:12]
        log.info("RecvEngine %s: resume() called (resume_id=%s)", tid[:8], resume_id)
        with self._transfer.lock:
            if self._transfer.state == TransferState.PAUSED:
                self._pause_flag.clear()
            self._resume_id = resume_id
            self._resume_event.set()
        try:
            self._session.send_json(
                FrameType.TRANSFER_RESUME, {"resume_id": resume_id}, transfer_id=tid
            )
        except Exception as e:
            log.warning("RecvEngine %s: could not send RESUME: %s", tid[:8], e)

    def send_window_update(self, new_capacity: int, scope='global', automatic=False) -> None:
        """
        Advertise a global or negotiated per-transfer outstanding-byte
        capacity. The central router applies updates even while an engine
        is paused. Automatic refreshes cannot undo an explicit smaller cap.
        """
        tid = self._transfer.id
        validate_capacity(new_capacity)
        if scope not in ('global', 'transfer'):
            raise ValueError('invalid window scope')
        if scope == 'transfer' and not _scoped_flow_supported(self._session):
            raise TransferStateError('scoped flow control was not negotiated')
        (log.debug if automatic else log.info)("RecvEngine %s: sending WINDOW_UPDATE capacity=%d",
                 tid[:8], new_capacity)
        try:
            self._session.send_json(
                FrameType.WINDOW_UPDATE, {"capacity": new_capacity, 'scope': scope,
                                         'automatic': automatic}, transfer_id=tid
            )
        except Exception as e:
            log.warning("RecvEngine %s: could not send WINDOW_UPDATE: %s", tid[:8], e)

    def _advertise_windows(self):
        if self._receive_limits is not None and _scoped_flow_supported(self._session):
            global_capacity, transfer_capacity = self._receive_limits()
            self.send_window_update(global_capacity, 'global', automatic=True)
            self.send_window_update(transfer_capacity, 'transfer', automatic=True)

    def cancel(self, notify_peer: bool = True) -> None:
        """Cancel this transfer. Session stays open."""
        if notify_peer:
            require_feature(self._session, 'cancel')
        tid = self._transfer.id
        log.info("RecvEngine %s: cancel() called", tid[:8])
        with self._transfer.lock:
            if self._transfer.state in (TransferState.COMPLETED, TransferState.FAILED):
                return
            self._cancel_event.set()
            self._manager.cancel_transfer(tid)
        if notify_peer:
            try:
                self._session.send_transfer_cancel(tid, 'cancelled by receiver')
            except Exception:
                pass

    def abort_connection_lost(self) -> None:
        """recovery: see SendEngine.abort_connection_lost. The .part
        file is deliberately kept — it is what recovery resumes from."""
        self._conn_lost = True
        self._cancel_event.set()

    def _connection_dead(self) -> bool:
        return self._conn_lost or self._session.is_lost

    def _mark_conn_lost(self) -> None:
        try:
            self._manager.mark_connection_lost(self._transfer.id)
        except TransferNotFound:
            pass

    # ── Engine entry point ────────────────────────────────────────────────

    def run(self) -> None:
        tid = self._transfer.id
        try:
            self._run_inner()
            if self._conn_lost:
                self._mark_conn_lost()
        except TransferError as e:
            if self._connection_dead():
                log.warning("RecvEngine %s: connection lost (%s)", tid[:8], e)
                self._mark_conn_lost()
                return
            log.error("RecvEngine %s failed: %s", tid[:8], e)
            try:
                self._session.send_transfer_error(tid, str(e))
            except Exception:
                pass
            try:
                self._manager.fail_transfer(tid, str(e))
            except Exception:
                pass
        except Exception as e:
            if self._connection_dead():
                log.warning("RecvEngine %s: connection lost (%s)", tid[:8], e)
                self._mark_conn_lost()
                return
            log.exception("RecvEngine %s unexpected error", tid[:8])
            try:
                self._session.send_transfer_error(tid, f"unexpected: {e}")
            except Exception:
                pass
            try:
                self._manager.fail_transfer(tid, f"unexpected: {e}")
            except Exception:
                pass

    def _run_inner(self) -> None:
        tid        = self._transfer.id
        total_size = self._transfer.total_size
        offset     = self._resume_offset
        _integer(offset, 'resume offset', total_size)
        expected_checksum = _checksum(self._transfer.checksum)
        if expected_checksum is None:
            raise TransferError('Receiver requires the whole-file checksum')
        self._advertise_windows()

        log.info("RecvEngine %s: receiving %r → %r (resume_offset=%d)",
                 tid[:8], self._transfer.filename, self._part_path, offset)

        # Single hasher accumulates all bytes including in-flight chunks
        # written during pause. It is passed into _wait_for_resume so
        # those bytes are always included in the final SHA-256.
        hasher = hashlib.sha256()

        # Seed hasher with already-received bytes when resuming
        if offset > 0:
            if not os.path.isfile(self._part_path) or os.path.getsize(self._part_path) < offset:
                raise TransferError('Partial file is shorter than the confirmed offset')
            with open(self._part_path, "rb") as fh:
                remaining = offset
                while remaining:
                    buf = fh.read(min(65536, remaining))
                    if not buf:
                        raise TransferError('Partial file changed while hashing its prefix')
                    hasher.update(buf)
                    remaining -= len(buf)

        os.makedirs(os.path.dirname(os.path.abspath(self._part_path)), exist_ok=True)

        # `cancelled` records that cleanup is needed without acting on
        # it yet. _handle_cancel() calls os.remove() on this same
        # .part path — while `fh` (below) still holds it open, that's
        # fine on POSIX (an open file can be unlinked there) but fails
        # on Windows with WinError 32 ("file in use"), since Windows
        # won't delete a file while any handle to it is open. The
        # cleanup must therefore wait for handle closure. Every cancellation path sets
        # this flag and breaks out of the loop instead of cleaning up
        # immediately — cleanup happens once, after the `with` block
        # has exited and `fh` is guaranteed closed.
        cancelled = False

        with open(self._part_path, "r+b" if offset > 0 else "wb") as fh:
            fh.truncate(offset)
            fh.seek(offset)
            if self._needs_resume_handshake:
                log.info(
                    "RecvEngine %s: restored transfer — performing initial "
                    "resume handshake (offset=%d)", tid[:8], offset,
                )
                self.resume()
                if self._wait_for_resume(tid, offset, hasher, fh):
                    with self._offset_lock:
                        offset = self._confirmed_offset
                else:
                    cancelled = True

            while not cancelled:
                if self._final_received:
                    break
                if self._cancel_event.is_set():
                    cancelled = True
                    break

                # ── receiver-initiated pause check ────────────────────────
                if self._pause_flag.is_set():
                    self._pause_flag.clear()

                    with self._transfer.lock:
                        cur = self._transfer.state
                    if cur in TERMINAL_STATES:
                        log.debug("RecvEngine %s: late pause ignored (state=%s)",
                                  tid[:8], cur.value)
                    else:
                        log.info("RecvEngine %s: paused (receiver-initiated) "
                                 "at offset %d", tid[:8], offset)
                        # Send paused ACK so sender knows safe offset
                        self._session.send_json(
                            FrameType.TRANSFER_ACK,
                            {"offset": offset, "paused": True},
                            transfer_id=tid,
                        )
                        try:
                            self._manager.confirm_paused(tid)
                        except (TransferNotFound, TransferStateError):
                            pass
                        resumed = self._wait_for_resume(tid, offset, hasher, fh)
                        if not resumed:
                            cancelled = True
                            break
                        # Re-read offset (may have advanced due to in-flight chunks)
                        with self._offset_lock:
                            offset = self._confirmed_offset
                        log.info("RecvEngine %s: resumed at offset %d", tid[:8], offset)
                    continue

                # ── pull next frame ───────────────────────────────────────
                frame = self._dequeue(tid)
                if frame is None:
                    cancelled = True
                    break

                ft = frame.frame_type
                if ft == FrameType.TRANSFER_ERROR:
                    raise TransferError(f"Peer error: {frame.json().get('error', 'unknown')}")

                # ── sender-initiated PAUSE ────────────────────────────────
                if ft == FrameType.TRANSFER_PAUSE:
                    log.info("RecvEngine %s: sender-initiated PAUSE at offset %d",
                             tid[:8], offset)
                    # Send paused ACK so sender knows the safe offset
                    self._session.send_json(
                        FrameType.TRANSFER_ACK,
                        {"offset": offset, "paused": True},
                        transfer_id=tid,
                    )
                    # Transition our own side's transfer ACTIVE -> PAUSING.
                    # Each side tracks state in its own independent
                    # TransferManager — the sender calling
                    # pause_transfer() on ITS manager (SendEngine.pause())
                    # has no effect on ours. Without this, `cur` below is
                    # still ACTIVE (never PAUSING), confirm_paused() is
                    # never called, and this side's transfer sits at
                    # ACTIVE forever even though the RecvEngine has, in
                    # every practical sense, already paused (it's about
                    # to block in _wait_for_resume). Mirrors the
                    # already-correct symmetric case in SendEngine's own
                    # ACK-wait loop, which pause_transfer()s itself the
                    # same way on receiving a receiver-initiated PAUSE.
                    with self._transfer.lock:
                        cur = self._transfer.state
                    if cur == TransferState.ACTIVE:
                        try:
                            self._manager.pause_transfer(tid)
                        except (TransferNotFound, TransferStateError):
                            pass
                    # Confirm PAUSED
                    with self._transfer.lock:
                        cur = self._transfer.state
                    if cur == TransferState.PAUSING:
                        try:
                            self._manager.confirm_paused(tid)
                        except (TransferNotFound, TransferStateError):
                            pass
                    log.info("RecvEngine %s: paused at offset %d", tid[:8], offset)

                    resumed = self._wait_for_resume(tid, offset, hasher, fh)
                    if not resumed:
                        cancelled = True
                        break
                    with self._offset_lock:
                        offset = self._confirmed_offset
                    log.info("RecvEngine %s: resumed at offset %d", tid[:8], offset)
                    continue

                # ── sender-initiated RESUME (while we're not paused) ──────
                if ft == FrameType.TRANSFER_RESUME:
                    # Sender sent RESUME but we weren't paused (e.g. pause
                    # never landed because transfer was already done).
                    # Always reply with current offset so sender doesn't hang.
                    resume_id = frame.json().get("resume_id")
                    log.debug("RecvEngine %s: stray RESUME (state=%s, resume_id=%s), "
                              "confirming offset %d", tid[:8],
                              self._transfer.state.value, resume_id, offset)
                    self._session.send_json(
                        FrameType.TRANSFER_ACK,
                        {"offset": offset, "resuming": True, "resume_id": resume_id},
                        transfer_id=tid,
                    )
                    try:
                        self._manager.resume_transfer(tid)
                    except (TransferNotFound, TransferStateError):
                        pass
                    continue

                if ft != FrameType.TRANSFER_DATA:
                    log.debug("RecvEngine %s: unexpected %s, ignoring", tid[:8], ft.name)
                    continue

                offset = self._receive_chunk(frame, offset, hasher, fh)
                if self._final_received:
                    break

            if not cancelled:
                fh.flush()
                os.fsync(fh.fileno())

        # `fh` is guaranteed closed from here on — the `with` block above
        # has exited either via the `break`s just above or by falling
        # through normally. Only past this point is it safe to delete or
        # rename self._part_path on every platform, not just POSIX.
        if cancelled:
            if self._transfer.state == TransferState.FAILED:
                return  # externally stopped: preserve the verified partial for retry
            if self._conn_lost:
                # Connection loss, not a cancel: keep the .part file —
                # recovery resumes from it — and let run() mark the
                # transfer CONNECTION_LOST.
                log.info("RecvEngine %s: connection lost at offset %d; "
                         "keeping .part for recovery", tid[:8], offset)
                return
            self._handle_cancel(tid)
            return

        # ── finalize ──────────────────────────────────────────────────────
        sha256    = hasher.hexdigest()
        dest_path = self._dest_path

        expected = self._transfer.checksum
        disk_sha256 = _compute_sha256(self._part_path)
        if sha256 != expected or disk_sha256 != expected or os.path.getsize(self._part_path) != total_size:
            self._transfer._restart_required = True
            err = f"Checksum mismatch: got {sha256}, expected {expected}"
            raise TransferError(err)

        validator = getattr(self._transfer, '_receive_validator', None)
        if validator:
            try:
                validator(self._part_path)
            except Exception as exc:
                raise TransferError('Approved archive validation failed: ' + str(exc)) from exc

        # The same lock gates cancel() and publication. Whichever wins owns
        # the terminal outcome; a cancelled transfer never publishes later.
        with self._transfer.lock:
            if self._transfer.state == TransferState.CANCELLED or self._cancel_event.is_set():
                self._handle_cancel(tid)
                return
            publish(self._transfer, self._part_path, dest_path, overwrite=self._transfer.overwrite)
            self._manager.complete_transfer(tid, checksum=sha256)
        self._session.send_json(
            FrameType.TRANSFER_COMPLETE, {"sha256": sha256, "size": offset}, transfer_id=tid)
        log.info("RecvEngine %s: completed %r sha256=%s",
                 tid[:8], self._transfer.filename, sha256[:12])

    def _receive_chunk(self, frame, offset, hasher, fh):
        """The same validation/ACK path is used while ACTIVE and PAUSED."""
        tid = self._transfer.id
        if frame.transfer_id != tid or frame.is_compressed:
            raise TransferError('DATA has the wrong transfer ID or unsupported compression')
        try:
            start, chunk, checksum = _decode_data_payload(frame.payload)
            _integer(start, 'chunk offset')
            _checksum(checksum)
        except (ValueError, TransferError) as exc:
            raise TransferError(f'Bad DATA payload: {exc}') from exc
        digest = _chunk_sha256(chunk)
        if checksum is None:
            raise TransferError('DATA must include a chunk checksum')
        identity = (frame.sequence, start, digest, len(chunk), frame.is_final)
        if checksum == digest and identity == self._last_chunk:
            self._session.send_json(FrameType.TRANSFER_ACK, {'offset': offset},
                                    transfer_id=tid, sequence=frame.sequence)
            return offset  # duplicate retransmission: never write twice
        if ((self._next_sequence is not None and frame.sequence != self._next_sequence)
                or start != offset):
            raise TransferError('DATA sequence or offset mismatch')
        end = offset + len(chunk)
        total = self._transfer.total_size
        limit = getattr(self._transfer, 'max_receive_size', 0)
        if (len(chunk) > getattr(self._transfer, 'chunk_size', MAX_CHUNK_SIZE)
                or end > total or (limit and end > limit)):
            raise TransferError('DATA exceeds negotiated chunk size or file size limit')
        if frame.is_final and end != total:
            raise TransferError('Final DATA length does not match declared file size')
        if not chunk and not frame.is_final:
            raise TransferError('Empty DATA must be final')
        if checksum != digest:
            self._bad_chunk_attempts += 1
            if self._bad_chunk_attempts > MAX_CHUNK_RETRIES:
                raise TransferError('Chunk checksum retry budget exhausted')
            if _range_retry_supported(self._session):
                self._session.send_json(FrameType.TRANSFER_RETRY,
                    {'offset': offset, 'end': end, 'sequence': frame.sequence,
                     'reason': 'chunk checksum mismatch'}, transfer_id=tid, sequence=frame.sequence)
            else:
                self._session.send_json(FrameType.TRANSFER_ERROR,
                    {'error': 'chunk checksum mismatch', 'retry_seq': frame.sequence,
                     'offset': offset}, transfer_id=tid)
            return offset
        # Cancel may arrive while decoding/hashing. Serialize its state change
        # with this write so no further bytes are appended after cancellation.
        with self._transfer.lock:
            if self._transfer.state == TransferState.CANCELLED or self._cancel_event.is_set():
                return offset
            fh.write(chunk)
            fh.flush()
            if self._persistence is not None:
                os.fsync(fh.fileno())
            hasher.update(chunk)
            self._last_chunk = identity
            self._bad_chunk_attempts = 0
            self._next_sequence = frame.sequence + 1
            self._final_received = frame.is_final
            with self._offset_lock:
                self._confirmed_offset = end
            self._transfer.update_progress(end)
        if self._progress_cb:
            try:
                self._progress_cb(end, total)
            except Exception:
                log.exception('Transfer progress callback failed')
        if self._persistence:
            self._persistence.save(self._transfer, part_path=self._part_path,
                                   dest_path=self._dest_path, chunk_sha256=digest,
                                   retry_count=self._transfer.attempt)
        self._advertise_windows()
        self._session.send_json(FrameType.TRANSFER_ACK, {'offset': end},
                                transfer_id=tid, sequence=frame.sequence)
        return end

    def _dequeue(self, tid: str, timeout: float = 60.0) -> Optional[Frame]:
        """Pull next DATA/control frame. Returns None on CANCEL or cancel_event."""
        deadline = time.monotonic() + timeout
        while True:
            if self._cancel_event.is_set():
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TransferError(f"Timeout waiting for DATA ({tid[:8]})")
            try:
                frame = self._queue.get(timeout=min(remaining, 0.2))
            except queue.Empty:
                continue
            if frame.frame_type == FrameType.TRANSFER_CANCEL:
                return None
            return frame

    def _wait_for_resume(
        self,
        tid: str,
        offset: int,
        hasher,
        fh,
        timeout: float = 3600.0,
    ) -> bool:
        deadline = time.monotonic() + timeout
        while not self._cancel_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            resumed = self._wait_for_resume_attempt(tid, offset, hasher, fh, remaining)
            if resumed is not None:
                return resumed
            with self._offset_lock:
                offset = self._confirmed_offset
        return False

    def _wait_for_resume_attempt(self, tid, offset, hasher, fh, timeout):
        """
        Block in PAUSED state until TRANSFER_RESUME arrives or resume() is called.

        Critical: any DATA frames that arrive while paused (in-flight from
        the sender before it processed our PAUSE) are written immediately
        and included in the hasher. This keeps .part file, hasher, and
        confirmed_offset all in sync.

        When resume is triggered, sends TRANSFER_ACK{offset:N, resuming:True}
        so the sender knows the exact offset to continue from.

        Returns True if resumed, False if cancelled.
        """
        log.debug("RecvEngine %s: _wait_for_resume at offset %d", tid[:8], offset)
        deadline = time.monotonic() + timeout
        current_offset = offset   # local copy — updated as in-flight chunks arrive
        resume_id: Optional[str] = None

        while not self._cancel_event.is_set():
            # Local resume() called?
            if self._resume_event.is_set():
                self._resume_event.clear()
                resume_id = self._resume_id
                log.debug("RecvEngine %s: local resume() fired (resume_id=%s)",
                          tid[:8], resume_id)
                break

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                log.warning("RecvEngine %s: timed out waiting for resume", tid[:8])
                return False

            try:
                frame = self._queue.get(timeout=min(remaining, 0.2))
            except queue.Empty:
                continue

            ft = frame.frame_type

            if ft == FrameType.TRANSFER_ERROR:
                raise TransferError(f"Peer error while paused: {frame.json().get('error', 'unknown')}")
            if ft == FrameType.TRANSFER_RESUME:
                resume_id = frame.json().get("resume_id")
                log.info("RecvEngine %s: RESUME from sender (resume_id=%s)",
                         tid[:8], resume_id)
                break

            if ft == FrameType.TRANSFER_CANCEL:
                return False

            if ft == FrameType.TRANSFER_PAUSE:
                # Redundant — we're already paused. No-op, just discard.
                log.debug("RecvEngine %s: duplicate PAUSE while already "
                          "paused, ignoring", tid[:8])
                continue

            if ft == FrameType.TRANSFER_DATA:
                current_offset = self._receive_chunk(frame, current_offset, hasher, fh)
                if self._final_received:
                    return True  # completion wins a late pause, still validated
                continue

            log.debug("RecvEngine %s: discarding %s while paused", tid[:8], ft.name)

        if self._cancel_event.is_set():
            return False

        admitted = _resume_with_slot(self, tid)
        if admitted is None:
            return None
        if not admitted:
            return False
        # Send resuming ACK with confirmed offset, echoing back the
        # resume_id that triggered this — the sender's resume-confirmation stage (if it's
        # the one waiting) only accepts an ACK matching the attempt it's
        # currently on.
        with self._offset_lock:
            confirmed = self._confirmed_offset
        self._session.send_json(
            FrameType.TRANSFER_ACK,
            {"offset": confirmed, "resuming": True, "resume_id": resume_id},
            transfer_id=tid,
        )
        # A restored sender starts a fresh sequence epoch at the confirmed
        # byte offset. The first DATA fixes its sequence; subsequent DATA
        # must be contiguous. Byte offsets remain authoritative throughout.
        self._next_sequence = None
        self._last_chunk = None

        return True

    def _handle_cancel(self, tid: str) -> None:
        """cancellation: cancel — clean up .part, notify manager. Session stays open."""
        log.info("RecvEngine %s: cancelled", tid[:8])
        if self._delete_part and os.path.isfile(self._part_path):
            try:
                os.remove(self._part_path)
            except OSError as e:
                log.warning("RecvEngine %s: could not delete .part: %s", tid[:8], e)
        try:
            self._manager.cancel_transfer(tid)
        except (TransferNotFound, TransferStateError):
            pass


# ── High-level helpers ────────────────────────────────────────────────────────

def run_upload(
    session: Session,
    manager: TransferManager,
    router: FrameRouter,
    file_path: str,
    filename: Optional[str] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    overwrite: bool = False,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> Transfer:
    """High-level: register, create, and run one upload. Blocks until done."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(file_path)

    fname  = filename or os.path.basename(file_path)
    size   = os.path.getsize(file_path)
    sha256 = _compute_sha256(file_path)

    transfer = manager.create_transfer(
        direction=TransferDirection.UPLOAD,
        filename=fname, total_size=size, checksum=sha256,
    )
    resume_offset = send_transfer_create(
        session, transfer, router, chunk_size=chunk_size, overwrite=overwrite
    )
    transfer.update_progress(resume_offset)
    q = router.get_queue(transfer.id)  # registered by send_transfer_create()

    SendEngine(
        session=session, transfer=transfer, manager=manager,
        frame_queue=q, file_path=file_path, resume_offset=resume_offset,
        chunk_size=chunk_size, progress_cb=progress_cb,
    ).run()

    router.unregister(transfer.id)
    return transfer


def run_download(
    session: Session,
    manager: TransferManager,
    router: FrameRouter,
    remote_filename: str,
    dest_dir: str = config.CLIENT_DIR,
    dest_path: Optional[str] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> Transfer:
    """High-level: request and receive a download. Blocks until done."""
    from .security import safe_filename as _sf
    safe_name = _sf(remote_filename)

    if dest_path:
        os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
    else:
        os.makedirs(dest_dir, exist_ok=True)
        dest_path = os.path.join(dest_dir, safe_name)

    transfer = manager.create_transfer(
        direction=TransferDirection.DOWNLOAD,
        filename=safe_name, total_size=0,
    )
    part_path = partial_path(dest_path, transfer.id)
    claim_destination(transfer, dest_path, part_path, overwrite=True)
    resume_offset = send_transfer_create(session, transfer, router, chunk_size=chunk_size)
    transfer.update_progress(resume_offset)
    q = router.get_queue(transfer.id)  # registered by send_transfer_create()

    RecvEngine(
        session=session, transfer=transfer, manager=manager,
        frame_queue=q, part_path=part_path, resume_offset=resume_offset,
        progress_cb=progress_cb,
    ).run()

    router.unregister(transfer.id)
    return transfer


def _compute_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            buf = f.read(65536)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def teardown_session(session: Session, router: FrameRouter, timeout: float = 1.0) -> bool:
    """
    Task 8: reusable test/session teardown helper.

    Order:
      1. Set router stop flag
      2. shutdown() the socket — unblocks reader thread immediately
      3. close_session() — releases the fd
      4. join reader thread with timeout
      5. Return True if reader exited cleanly, False if still alive

    Use this instead of ad-hoc cleanup in tests.
    """
    router.stop()          # sets _stop, calls shutdown(), joins with timeout=1
    session.close_session()
    alive = router._reader_thread.is_alive() if router._reader_thread else False
    return not alive
