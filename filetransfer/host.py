"""
Host side of the app: PIN pairing, approval, receive rules and discovery.
Every file transfer uses the framed session_protocol session and the
TransferManager / ConcurrentScheduler lifecycle.

One ConcurrentScheduler (and one TransferManager) per connected peer;
each peer's transfers are independent of every other peer's.

ZIP selection packaging and device chat use the same session infrastructure.
"""

from __future__ import annotations

import os
import hashlib
import socket
import ssl
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional

from . import config, discovery
from .chat import ChatChannel
from .engine import FrameRouter
from .logsetup import get_logger
from .manager import TransferManager
from .persistence import PersistenceStore, default_store, load_transfer_records
from .session_protocol import NegotiationError, Session, SessionError
from .scheduler import ConcurrentScheduler
from .security import generate_pin, safe_filename, verify_pin
from .transport import Frame, FrameType, FrameError

log = get_logger("filetransfer.host")


class PendingRequest:
    def __init__(self, request_id: str, addr):
        self.id = request_id
        self.addr = addr
        self.event = threading.Event()
        self.approved: Optional[bool] = None


@dataclass
class PeerConnection:
    """A connected peer, or its dormant persisted transfer history."""
    peer_id: str
    addr: tuple
    session: Optional[Session]
    router: Optional[FrameRouter]
    manager: TransferManager
    scheduler: Optional[ConcurrentScheduler]
    control_thread: Optional[threading.Thread] = None
    _stop: threading.Event = field(default_factory=threading.Event)
    chat: ChatChannel = field(default_factory=ChatChannel)


class TransferHost:
    """
    Start once per app run. Owns the listening socket, PIN lifecycle,
    pending-approval queue, per-connection settings, and every
    connected peer's TransferManager/ConcurrentScheduler.
    """

    def __init__(self, dest_dir: Optional[str] = None,
                 max_concurrent_per_peer: int = 5,
                 persistence: Optional[PersistenceStore] = None, require_batch_approval=False):
        self.dest_dir = dest_dir or config.SERVER_DIR
        self._max_concurrent_per_peer = max_concurrent_per_peer
        self._persistence = persistence if persistence is not None else default_store('host', self.dest_dir)

        self._sock: Optional[socket.socket] = None
        self._accept_thread: Optional[threading.Thread] = None
        self._pin_thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()

        self._lock = threading.Lock()
        self._pin_lock = threading.Lock()
        self._pending: dict[str, PendingRequest] = {}
        self._peers: dict[str, PeerConnection] = {}
        self._attach_locks = {}
        # Serialize stop/start and peer installation. Retired peers remain
        # owned until every old worker has exited, including inbound CREATE.
        self._lifecycle_lock = threading.RLock()
        self._shutdown_peers: list[PeerConnection] = []
        self._generation = object()
        self._connections = {}
        self._connection_lock = threading.Lock()
        self._tls_context = None
        self.certificate_fingerprint = None
        from .rate_limit import AttemptLimiter
        self._pin_attempts = AttemptLimiter()
        self.allow_remote_delete = self._persistence.get_setting('allow_remote_delete') is True
        self.shutdown_join_timeout = 5.0

        self._pending_listeners: list[Callable[[dict], None]] = []
        self._pin_listeners: list[Callable[[str], None]] = []
        # transfer events: transfer-event listeners, fired for every
        # peer's TransferManager. fn(peer_id, transfer, old, new).
        self._transfer_listeners: list[Callable] = []
        self._progress_listeners = []

        self.current_pin: Optional[str] = None
        self.pin_generated_at: Optional[float] = None
        self.running = False

        self.max_upload_size = config.MAX_UPLOAD_SIZE_BYTES
        self.blocked_extensions = self._parse_extensions(config.BLOCKED_EXTENSIONS)
        from .batches import HostBatches
        self.require_batch_approval = require_batch_approval
        self.batches = HostBatches(self)
        if require_batch_approval:
            self.max_upload_size = int(self.batches.rules['max_upload_mb'] * 1048576)
            self.blocked_extensions = set()

        self._discovery = discovery.DiscoveryResponder(
            get_port=lambda: config.PORT,
            get_pin=lambda: self.current_pin,
        )
        for scope in self._persistence.list_scopes('peer:'):
            manager = TransferManager(max_concurrent=max_concurrent_per_peer)
            manager.namespace = scope
            manager.suspended = True
            load_transfer_records(self._persistence.scoped(scope), manager)
            peer_id = scope.removeprefix('peer:')
            self._peers[peer_id] = PeerConnection(peer_id, ('offline', 0), None, None, manager, None)

    @staticmethod
    def _parse_extensions(raw: str) -> set:
        return {("." + e.strip().lstrip(".").lower()) for e in raw.split(",") if e.strip()}

    def update_settings(self, max_upload_mb: Optional[float] = None,
                        blocked_extensions: Optional[str] = None) -> None:
        if max_upload_mb is not None:
            self.max_upload_size = int(max_upload_mb * 1024 * 1024) if max_upload_mb > 0 else 0
        if blocked_extensions is not None:
            self.blocked_extensions = self._parse_extensions(blocked_extensions)
        # Applies to transfers still to come on already-connected peers
        # too, not just future connections.
        with self._lock:
            peers = list(self._peers.values())
        for peer in peers:
            if peer.scheduler is not None:
                peer.scheduler.update_settings(
                    max_upload_mb=max_upload_mb, blocked_extensions=blocked_extensions)
        log.info("Settings updated: max_upload=%s bytes, blocked_extensions=%s",
                 self.max_upload_size, sorted(self.blocked_extensions))

    def add_pending_listener(self, fn: Callable[[dict], None]) -> None:
        self._pending_listeners.append(fn)

    def add_pin_listener(self, fn: Callable[[str], None]) -> None:
        self._pin_listeners.append(fn)

    def add_transfer_listener(self, fn: Callable) -> None:
        """fn(peer_id: str, transfer: Transfer, old: TransferState, new: TransferState)."""
        if fn not in self._transfer_listeners:
            self._transfer_listeners.append(fn)

    def add_progress_listener(self, fn):
        if fn not in self._progress_listeners:
            self._progress_listeners.append(fn)

    # ── lifecycle ──────────────────────────────────────────────────────

    def start(self) -> None:
        with self._lifecycle_lock:
            self._start_locked()

    def _start_locked(self) -> None:
        if self.running:
            return
        if self._live_shutdown_workers():
            raise SessionError('host workers are still stopping; try starting again later')
        self._shutdown_peers.clear()
        os.makedirs(self.dest_dir, exist_ok=True)
        if config.TLS_ENABLED:
            from .tls_identity import server_context
            self._tls_context, self.certificate_fingerprint = server_context(self.dest_dir)
        else:
            self._tls_context, self.certificate_fingerprint = None, None
        self._regenerate_pin(reason="start")

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((config.HOST, config.PORT))
        sock.listen(config.MAX_CLIENTS)
        sock.settimeout(0.5)

        self._sock = sock
        self._stop_flag.clear()
        self.running = True

        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True,
                                                name="TransferHost-accept")
        self._accept_thread.start()

        if config.PIN_EXPIRY_SECONDS > 0:
            self._pin_thread = threading.Thread(target=self._pin_expiry_loop, daemon=True,
                                                name="TransferHost-pin-expiry")
            self._pin_thread.start()

        self._discovery.start()
        log.info("TransferHost listening on %s:%s", config.HOST, config.PORT)

    def stop(self) -> None:
        with self._lifecycle_lock:
            self._stop_locked()

    def _stop_locked(self) -> None:
        self._stop_flag.set()
        self._generation = object()  # pre-stop authenticated handlers cannot attach after restart
        if self._sock:
            self._sock.close()
        with self._connection_lock:
            connections = list(self._connections.values())
        for connection in connections:
            try:
                # Do not clear SSLSocket._sslobj from another thread while
                # OpenSSL is in do_handshake (unsafe on Windows). Wake the
                # raw transport; the owning worker performs close().
                socket.socket.shutdown(connection, socket.SHUT_RDWR)
            except OSError:
                pass
        self.running = False
        self.current_pin = None
        self._discovery.stop()
        with self._lock:
            peers = list(self._peers.values())
            self._shutdown_peers.extend(peer for peer in peers if
                                        all(peer is not old for old in self._shutdown_peers))
            self._peers.clear()
            pending = list(self._pending.values())
            self._pending.clear()
        for request in pending:
            request.approved = False
            request.event.set()
        for peer in self._shutdown_peers:
            self._teardown_peer(peer)
        if self._accept_thread:
            if self._accept_thread is not threading.current_thread():
                self._accept_thread.join(timeout=2)
        if self._pin_thread:
            if self._pin_thread is not threading.current_thread():
                self._pin_thread.join(timeout=2)
        if self._live_shutdown_workers():
            raise SessionError('host workers are still stopping; retry stop or start after they exit')
        self._shutdown_peers.clear()
        log.info("TransferHost stopped.")

    @staticmethod
    def _peer_workers(peer):
        workers = [peer.control_thread]
        if peer.scheduler is not None:
            workers.extend(peer.scheduler._job_threads())
            workers.append(peer.scheduler._accept_thread)
        if peer.router is not None:
            workers.extend((peer.router._reader_thread, peer.router._keepalive_thread))
        return [worker for worker in workers if worker is not None]

    def _live_shutdown_workers(self):
        workers = [self._accept_thread, self._pin_thread] if self._stop_flag.is_set() else []
        for peer in self._shutdown_peers:
            workers.extend(self._peer_workers(peer))
        return [worker for worker in workers if worker is not None and worker.is_alive()]

    @property
    def stopping(self):
        with self._lifecycle_lock:
            return self._stop_flag.is_set() and bool(self._live_shutdown_workers())

    # ── PIN / pending-approval (same semantics as FileTransferServer) ──

    def _regenerate_pin(self, reason: str = "") -> None:
        with self._pin_lock:
            self.current_pin = generate_pin(config.PIN_LENGTH)
            self.pin_generated_at = time.time()
            new_pin = self.current_pin
        log.info("PIN regenerated (%s)", reason)
        for fn in list(self._pin_listeners):
            try:
                fn(new_pin)
            except Exception:
                log.exception("PIN listener raised")

    def _pin_expiry_loop(self) -> None:
        while not self._stop_flag.wait(timeout=1):
            with self._pin_lock:
                generated_at = self.pin_generated_at
            if generated_at and (time.time() - generated_at) >= config.PIN_EXPIRY_SECONDS:
                self._regenerate_pin(reason="expired")

    def register_pending(self, addr) -> PendingRequest:
        req = PendingRequest(str(uuid.uuid4()), addr)
        with self._lock:
            self._pending[req.id] = req
        info = {"id": req.id, "ip": addr[0], "port": addr[1]}
        for fn in list(self._pending_listeners):
            try:
                fn(info)
            except Exception:
                log.exception("Pending-request listener raised")
        return req

    def clear_pending(self, request_id: str) -> None:
        with self._lock:
            self._pending.pop(request_id, None)

    def list_pending(self) -> list[dict]:
        with self._lock:
            return [{"id": r.id, "ip": r.addr[0], "port": r.addr[1]}
                    for r in self._pending.values() if r.approved is None]

    def accept_pending(self, request_id: str) -> bool:
        with self._lock:
            req = self._pending.get(request_id)
        if not req:
            return False
        req.approved = True
        req.event.set()
        return True

    def reject_pending(self, request_id: str) -> bool:
        with self._lock:
            req = self._pending.get(request_id)
        if not req:
            return False
        req.approved = False
        req.event.set()
        return True

    # ── accept loop ────────────────────────────────────────────────────

    def _accept_loop(self) -> None:
        generation = self._generation
        while not self._stop_flag.is_set():
            try:
                conn, addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._stop_flag.is_set():
                    break
                log.warning('Accept failed; listener remains available')
                self._stop_flag.wait(0.1)
                continue
            with self._connection_lock:
                if len(self._connections) >= config.MAX_CLIENTS:
                    conn.close()
                    continue
                ticket = object()
                self._connections[ticket] = conn
            threading.Thread(target=self._connection_worker, args=(ticket, conn, addr, generation),
                             daemon=True, name=f"TransferHost-peer-{addr[0]}").start()

    def _connection_worker(self, ticket, conn, addr, generation):
        session = None
        try:
            conn.settimeout(config.HANDSHAKE_TIMEOUT_SECONDS)
            if self._tls_context is not None:
                conn = self._tls_context.wrap_socket(conn, server_side=True, do_handshake_on_connect=False)
                with self._connection_lock:
                    self._connections[ticket] = conn
                conn.do_handshake()
            session = self._handle_connection(conn, addr, generation)
            if session is not None:
                while not session.is_lost and not self._stop_flag.wait(0.2):
                    pass
        except (OSError, SessionError, FrameError, ValueError):
            log.warning('Rejected incomplete or malformed connection from %s', addr)
        finally:
            if session is not None:
                session.close_session('connection worker finished')
            else:
                conn.close()
            with self._connection_lock:
                self._connections.pop(ticket, None)

    def _handle_connection(self, conn: socket.socket, addr, generation=None) -> None:
        generation = self._generation if generation is None else generation
        conn.settimeout(config.HANDSHAKE_TIMEOUT_SECONDS)
        session = Session(conn, addr, name="host")
        log.info("Peer connecting: %s", addr)
        try:
            session.negotiate_as_server(server_name="host")
        except (NegotiationError, SessionError, FrameError, OSError) as e:
            log.warning("Negotiation failed for %s: %s", addr, e)
            try:
                conn.close()
            except OSError:
                pass
            return

        if generation is not self._generation or self._stop_flag.is_set():
            session.close_session('host incarnation stopped')
            return
        if not self._authenticate(session, addr):
            try:
                session.close_session("auth failed or rejected")
            except OSError:
                pass
            return

        session.set_read_timeout(config.SOCKET_IDLE_TIMEOUT_SECONDS)
        try:
            self._install_peer(session, addr, generation=generation)
            return session
        except Exception:
            log.exception('Could not attach authenticated peer')
            session.close_session('recovery setup failed')

    def _install_peer(self, session, addr, *, generation=None):
        with self._lifecycle_lock:
            if generation is not None and generation is not self._generation:
                raise SessionError('host incarnation stopped')
            return self._install_current_peer(session, addr)

    def _install_current_peer(self, session, addr):
        if self._live_shutdown_workers():
            raise SessionError('old host workers have not stopped')
        peer_id = getattr(session, 'recovery_id', None) or str(uuid.uuid4())
        session.recovery_id = peer_id
        with self._lock:
            attach_lock = self._attach_locks.setdefault(peer_id, threading.Lock())
        with attach_lock:
            return self._install_peer_locked(session, addr, peer_id)

    def _install_peer_locked(self, session, addr, peer_id):
        if self._stop_flag.is_set():
            raise SessionError('host is stopping')
        router = FrameRouter(session)
        with self._lock:
            peer = self._peers.get(peer_id)
        if peer is not None and peer.scheduler is not None:
            peer._stop.set()
            if peer.control_thread is not None:
                peer.control_thread.join(timeout=2)
                if peer.control_thread.is_alive():
                    raise SessionError('old control worker has not stopped')
            peer.chat.bind(session, router)
            peer.scheduler.attach_session(session, router, recover=False,
                keepalive=(config.KEEPALIVE_INTERVAL_SECONDS, config.KEEPALIVE_TIMEOUT_SECONDS))
            manager, scheduler = peer.manager, peer.scheduler
            peer.addr, peer.session, peer.router = addr, session, router
            peer._stop = threading.Event()
        else:
            manager = peer.manager if peer is not None else TransferManager(max_concurrent=self._max_concurrent_per_peer)
            manager.namespace = 'peer:' + peer_id
            store = self._persistence.scoped('peer:' + peer_id)
            if peer is None:
                load_transfer_records(store, manager)
            manager.suspended = True
            scheduler = ConcurrentScheduler(session, manager, router, dest_dir=self.dest_dir,
                persistence=store, keep_history=True, max_upload_size=self.max_upload_size,
                blocked_extensions=self.blocked_extensions)
            scheduler.restore_jobs()
            peer = PeerConnection(peer_id=peer_id, addr=addr, session=session,
                                  router=router, manager=manager, scheduler=scheduler)
            peer.chat.bind(session, router)

        def _on_transfer_change(transfer, old, new, _peer_id=peer_id):
            self.batches.finished(_peer_id, transfer)
            for fn in list(self._transfer_listeners):
                try:
                    fn(_peer_id, transfer, old, new)
                except Exception:
                    log.exception("Transfer listener raised")
        if not getattr(manager, '_host_events_bound', False):
            manager.add_listener(_on_transfer_change)
            def on_progress(transfer, _peer_id=peer_id):
                for fn in list(self._progress_listeners):
                    try:
                        fn(_peer_id, transfer)
                    except Exception:
                        log.exception('Host progress listener raised')
            manager.add_progress_listener(on_progress)
            manager._host_events_bound = True

        with self._lock:
            if self._stop_flag.is_set():
                raise SessionError('host is stopping')
            self._peers[peer_id] = peer

        scheduler.receive_guard = lambda frame: self.batches.authorize(peer_id, frame)
        router.add_loss_listener(lambda p=peer, r=router: self._on_peer_lost(p, r))
        router.start_reader()
        router.start_keepalive(config.KEEPALIVE_INTERVAL_SECONDS, config.KEEPALIVE_TIMEOUT_SECONDS)
        scheduler.start_accept_loop()
        manager.suspended = False
        peer.control_thread = threading.Thread(
            target=self._control_loop, args=(peer,), daemon=True,
            name=f"TransferHost-control-{addr[0]}")
        peer.control_thread.start()
        log.info("Peer connected and authenticated: %s (peer_id=%s)", addr, peer_id[:8])
        return peer

    def _authenticate(self, session: Session, addr) -> bool:
        try:
            auth = session.recv_auth_payload()
            pin = auth.get('pin', '')
        except (SessionError, FrameError, OSError):
            return False

        if not self._pin_attempts.allow(addr[0]):
            session.send_auth_fail('Too many pairing attempts. Wait one minute and try again.')
            return False

        recovery_key = auth.get('recovery_key')
        if recovery_key is not None and (not isinstance(recovery_key, str) or len(recovery_key) != 64
                                        or any(c not in '0123456789abcdef' for c in recovery_key)):
            session.send_auth_fail('invalid recovery identity')
            return False
        with self._pin_lock:
            unexpired = not config.PIN_EXPIRY_SECONDS or (self.pin_generated_at is not None and
                time.time() - self.pin_generated_at < config.PIN_EXPIRY_SECONDS)
            valid_pin = unexpired and isinstance(pin, str) and bool(self.current_pin) and verify_pin(pin, self.current_pin)
            if valid_pin:
                self.current_pin = None  # consume atomically before another auth can compare
        if not valid_pin:
            session.send_auth_fail("invalid PIN")
            log.warning("Rejected connection from %s: bad PIN", addr)
            return False

        # Correct PIN — invalidate it immediately (shoulder-surfing etc),
        # even while we wait for host approval.
        self._regenerate_pin(reason="used")

        pending = self.register_pending(addr)
        session.send_auth_ok(pending=True)
        log.info("Connection pending host approval: %s", addr)

        resolved = pending.event.wait(timeout=config.PENDING_APPROVAL_TIMEOUT_SECONDS)
        if not resolved or pending.approved is not True or self._stop_flag.is_set():
            reason = "timed out waiting for approval" if not resolved else "rejected by host"
            session.send_auth_fail(reason)
            log.info("Connection %s: %s", addr, reason)
            self.clear_pending(pending.id)
            return False

        self.clear_pending(pending.id)
        if recovery_key is not None and session.supports('recovery'):
            session.recovery_id = hashlib.sha256(recovery_key.encode()).hexdigest()
        session.send_auth_ok(pending=False, peer_id=getattr(session, 'recovery_id', None))
        session.mark_authenticated()
        log.info("Connection approved: %s", addr)
        return True

    # ── File listing and deletion over the session protocol ─────────

    def _control_loop(self, peer: PeerConnection) -> None:
        router = peer.router
        while not peer._stop.is_set() and not router.connection_lost.is_set():
            frame = router.next_control_request(timeout=0.5)
            if frame is None:
                continue
            try:
                self._handle_control_frame(peer, frame)
            except Exception:
                log.exception("Error handling control frame %s from %s",
                              frame.frame_type.name, peer.addr)

    def _handle_control_frame(self, peer: PeerConnection, frame: Frame) -> None:
        session = peer.session
        cid = frame.transfer_id
        if frame.frame_type in (FrameType.BATCH_REQUEST, FrameType.BATCH_STATUS, FrameType.BATCH_CANCEL):
            try:
                if not session.authenticated or not session.supports('batch_approval'):
                    raise ValueError('Batch approval was not negotiated.')
                payload = frame.json()
                if frame.frame_type == FrameType.BATCH_REQUEST:
                    result = self.batches.request(peer.peer_id, payload)
                elif frame.frame_type == FrameType.BATCH_CANCEL:
                    result = self.batches.cancel(peer.peer_id, payload.get('batch_id', ''))
                else:
                    result = self.batches.status(peer.peer_id, payload.get('batch_id', ''))
                result = dict(ok=True, **result)
            except (ValueError, TypeError, KeyError) as exc:
                result = dict(ok=False, error=str(exc))
            session.send_json(FrameType.BATCH_RESPONSE, result, transfer_id=cid)
            return
        if frame.frame_type == FrameType.FILE_LIST_REQUEST:
            from .security import shared_file_path
            files = []
            for name in os.listdir(self.dest_dir):
                try:
                    shared_file_path(self.dest_dir, name)
                except (ValueError, OSError):
                    continue
                files.append(name)
            session.send_json(FrameType.FILE_LIST_RESPONSE, {"ok": True, "files": files},
                              transfer_id=cid)
        elif frame.frame_type == FrameType.FILE_DELETE_REQUEST:
            if not self.allow_remote_delete:
                session.send_json(FrameType.FILE_DELETE_RESPONSE,
                                  {'ok': False, 'error': 'Host has not permitted remote deletion.'}, transfer_id=cid)
                return
            raw_name = frame.json().get("filename", "")
            try:
                name = safe_filename(raw_name)
                path = os.path.join(self.dest_dir, name)
                if name.startswith('.') or name.endswith('.part') or os.path.islink(path):
                    raise ValueError('This file cannot be deleted remotely.')
                ok = os.path.isfile(path)
                if ok:
                    os.remove(path)
                session.send_json(FrameType.FILE_DELETE_RESPONSE, {"ok": ok}, transfer_id=cid)
            except (ValueError, OSError) as e:
                session.send_json(FrameType.FILE_DELETE_RESPONSE,
                                  {"ok": False, "error": str(e)}, transfer_id=cid)

    # ── peer teardown ──────────────────────────────────────────────────

    def _on_peer_lost(self, peer: PeerConnection, router=None) -> None:
        """Retain the peer and journal after scheduler loss handling.

        Fresh PIN authentication and host approval can reattach this stable
        identity to the same manager, without changing transfer IDs.
        """
        if router is not None and router is not peer.router:
            return
        peer._stop.set()
        log.info("Peer connection lost: %s (peer_id=%s)", peer.addr, peer.peer_id[:8])

    def _teardown_peer(self, peer: PeerConnection) -> None:
        peer._stop.set()
        if peer.scheduler is None:
            return
        peer.router.declare_lost('host stopping')
        peer.scheduler.stop_accept_loop()
        peer.router.stop()
        try:
            peer.session.close_session("host stopping")
        except OSError:
            pass
        if peer.control_thread:
            if peer.control_thread is not threading.current_thread():
                peer.control_thread.join(timeout=2)
        deadline = time.monotonic() + self.shutdown_join_timeout
        for thread in self._peer_workers(peer):
            if thread is not threading.current_thread():
                thread.join(timeout=max(0, deadline - time.monotonic()))

    # ── lookups used by the FastAPI layer ───────────────────────────────

    def get_peer(self, peer_id: str) -> Optional[PeerConnection]:
        with self._lock:
            return self._peers.get(peer_id)

    def list_peers(self) -> list[dict]:
        with self._lock:
            return [{"peer_id": p.peer_id, "ip": p.addr[0], "port": p.addr[1],
                     'connected': p.session is not None and not p.session.is_lost}
                    for p in self._peers.values()]
