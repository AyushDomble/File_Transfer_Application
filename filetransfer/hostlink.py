"""Client connection management: certificate verification, PIN authentication,
host approval, scheduled transfers, file listing and deletion over one session."""

from __future__ import annotations

import os
import hashlib
import queue
import socket
import ssl
import threading
import time
import uuid
from typing import Optional

from . import config
from .chat import ChatChannel
from .engine import FrameRouter
from .logsetup import get_logger
from .manager import TransferManager
from .persistence import PersistenceStore, default_store, load_transfer_records, save_transfer
from .session_protocol import AuthError, NegotiationError, Session, SessionError
from .scheduler import ConcurrentScheduler
from .transport import Frame, FrameType, FrameError
from .transfer import TransferState

log = get_logger("filetransfer.hostlink")

CONTROL_TIMEOUT_SECONDS = 15.0


class ConnectionRejected(Exception):
    """Bad PIN, host rejected, or approval timed out."""


class ConnectionLost(Exception):
    """The underlying connection died. Treat as 'please reconnect'."""


class HostLink:
    def __init__(self, host: str, port: Optional[int] = None, use_tls: Optional[bool] = None,
                dest_dir: Optional[str] = None, max_concurrent: int = 5,
                persistence: Optional[PersistenceStore] = None, fingerprint: Optional[str] = None):
        # Resolve configurable defaults at call time, not module import time.
        self.host = host
        self.port = port if port is not None else config.PORT
        self.use_tls = use_tls if use_tls is not None else config.TLS_ENABLED
        self.dest_dir = dest_dir or config.CLIENT_DIR
        self.session: Optional[Session] = None
        self.router: Optional[FrameRouter] = None
        self.manager = TransferManager(max_concurrent=max_concurrent)
        self.manager.suspended = True
        self.scheduler: Optional[ConcurrentScheduler] = None
        self._max_concurrent = max_concurrent
        self._connect_lock = threading.RLock()
        self.chat = ChatChannel()
        self.last_error = None
        self._root_store = persistence if persistence is not None else default_store('client', self.dest_dir)
        self.fingerprint = fingerprint
        self._trust_key = f'tls-pin:{self.host.strip().lower()}:{self.port}'
        endpoint = f'{self.host.strip().lower()}:{self.port}:tls={self.use_tls}'
        self.manager.namespace = 'endpoint:' + endpoint
        self._store = self._root_store.scoped('endpoint:' + endpoint)
        self._recovery_key = self._root_store.get_or_create_setting('identity:' + endpoint,
                                                                  lambda: os.urandom(32).hex())
        load_transfer_records(self._store, self.manager)
        for transfer in self.manager.all_transfers():
            if transfer.state in (TransferState.COMPLETED, TransferState.CANCELLED) and getattr(transfer, 'source_owned', False):
                try:
                    os.remove(transfer.source_path)
                except FileNotFoundError:
                    pass
        self.manager.add_listener(self._offline_state_change)

    def _offline_state_change(self, transfer, old, new):
        if self.scheduler is None:
            save_transfer(self._store, transfer)

    def _open_socket(self):
        from .tls_identity import normalize_pin, pinned_socket
        expected = None
        if self.use_tls:
            saved = self._root_store.get_setting(self._trust_key)
            supplied = normalize_pin(self.fingerprint) if self.fingerprint else None
            if saved and supplied and saved != supplied:
                raise ConnectionRejected('Saved host fingerprint differs. Do not reconnect until the host identity is verified; reset trust locally if the host was intentionally replaced.')
            expected = saved or supplied
            if not expected:
                raise ConnectionRejected('First pairing: enter the certificate fingerprint displayed on the host. Do not obtain it from discovery or an untrusted message.')
        raw = socket.create_connection((self.host, self.port), timeout=config.SOCKET_TIMEOUT_SECONDS)
        wrapped = None
        try:
            if not self.use_tls:
                return raw
            raw.settimeout(config.HANDSHAKE_TIMEOUT_SECONDS)
            wrapped = pinned_socket(raw, self.host, expected)
            self._root_store.set_setting(self._trust_key, expected)
            wrapped.settimeout(config.SOCKET_TIMEOUT_SECONDS)
            return wrapped
        except Exception:
            if wrapped is not None:
                wrapped.close()
            raw.close()
            raise

    def connect_with_pin(self, pin: str) -> None:
        with self._connect_lock:
            return self._connect_with_pin(pin)

    def _connect_with_pin(self, pin):
        if self.connected:
            return []
        sock = self._open_socket()
        session = Session(sock, (self.host, self.port), name="client")

        try:
            session.negotiate_as_client(client_name="client")
        except (NegotiationError, SessionError, FrameError, OSError) as e:
            try:
                session.close_session()
            except OSError:
                pass
            raise ConnectionRejected(f"could not negotiate with host: {e}") from e

        try:
            session.send_auth(pin, recovery_key=self._recovery_key)
            result = session.recv_auth_result()
            if result.get("pending"):
                log.info("PIN accepted, waiting for host to approve...")
                # A slow-but-attentive host's own approval timeout is
                # config.PENDING_APPROVAL_TIMEOUT_SECONDS; give this
                # wait real headroom past that rather than the normal
                # quick-handshake timeout.
                session.set_read_timeout(config.PENDING_APPROVAL_TIMEOUT_SECONDS + 15)
                result = session.recv_auth_result()
        except (AuthError, SessionError, FrameError, OSError) as e:
            try:
                session.close_session()
            except OSError:
                pass
            raise ConnectionRejected(str(e)) from e
        finally:
            try:
                session.set_read_timeout(config.SOCKET_IDLE_TIMEOUT_SECONDS)
            except OSError:
                pass

        log.info("Connection approved by host")
        expected_peer_id = hashlib.sha256(self._recovery_key.encode()).hexdigest()
        if session.supports('recovery') and result.get('peer_id') != expected_peer_id:
            session.close_session('recovery identity mismatch')
            raise ConnectionRejected('host did not confirm this recovery identity')
        router = FrameRouter(session)
        self.chat.bind(session, router)
        try:
            if self.scheduler is None:
                self.scheduler = ConcurrentScheduler(session, self.manager, router, dest_dir=self.dest_dir,
                    persistence=self._store, keep_history=True)
                self.scheduler.restore_jobs()
            else:
                self.scheduler.attach_session(session, router, recover=False,
                    keepalive=(config.KEEPALIVE_INTERVAL_SECONDS, config.KEEPALIVE_TIMEOUT_SECONDS))
            self.session, self.router = session, router
            router.add_loss_listener(lambda r=router: self._on_lost(r))
            router.start_reader()
            router.start_keepalive(config.KEEPALIVE_INTERVAL_SECONDS, config.KEEPALIVE_TIMEOUT_SECONDS)
            self.scheduler.start_accept_loop()
            self.manager.suspended = False
            for transfer in self.manager.all_transfers():
                if transfer.state == TransferState.CANCELLED and getattr(transfer, 'cancel_pending', False):
                    session.send_transfer_cancel(transfer.id, 'cancelled while offline')
            recovered = self.scheduler.recover_all()
            self.manager.schedule()
            self._root_store.set_setting('last_endpoint', dict(host=self.host, port=self.port, use_tls=self.use_tls))
            self.last_error = None
            return recovered
        except Exception:
            router.declare_lost('recovery setup failed')
            router.stop()
            session.close_session('recovery setup failed')
            raise

    def _on_lost(self, router):
        if router is self.router:
            self.last_error = 'Connection lost. Reconnect with a fresh PIN to recover saved transfers.'

    @property
    def connected(self) -> bool:
        return self.session is not None and not self.session.is_lost

    def close(self) -> None:
        with self._connect_lock:
            self._close()

    def _close(self):
        self.manager.suspended = True
        if self.router:
            self.router.declare_lost('client disconnecting; transfers retained')
        if self.scheduler:
            self.scheduler.stop_accept_loop()
        if self.router:
            self.router.stop()
        if self.session:
            try:
                self.session.close_session("client disconnecting")
            except OSError:
                pass
        if self.scheduler:
            for thread in self.scheduler._job_threads():
                if thread is not threading.current_thread():
                    thread.join(timeout=5)

    def cancel_all_and_close(self) -> None:
        """Forcibly interrupt everything in flight (mirrors the old
        FileTransferClient.cancel()'s role: a hard, immediate stop
        callable from another thread) then tear the connection down."""
        if self.scheduler:
            self.scheduler.cancel_all()
        self.close()

    def cancel_saved(self, transfer_id):
        transfer = self.manager.get(transfer_id)
        if not self.connected and not transfer.is_terminal:
            transfer.cancel_pending = True
        if self.scheduler is not None:
            return self.scheduler.cancel(transfer_id)
        if self.manager.cancel_transfer(transfer_id):
            if transfer.part_path:
                try:
                    os.remove(transfer.part_path)
                except FileNotFoundError:
                    pass
            if getattr(transfer, 'source_owned', False) and transfer.source_path:
                try:
                    os.remove(transfer.source_path)
                except FileNotFoundError:
                    pass

    # ── API integration: file listing / delete over the control-frame pair ────

    def _control_request(self, request_type: FrameType, response_type: FrameType,
                         payload: dict) -> dict:
        if not self.connected:
            raise ConnectionLost("not connected")
        cid = uuid.uuid4().hex
        router, session = self.router, self.session
        q = router.register(cid)
        try:
            session.send_json(request_type, payload, transfer_id=cid)
            deadline = time.monotonic() + CONTROL_TIMEOUT_SECONDS
            frame = None
            while time.monotonic() < deadline and not session.is_lost:
                try:
                    frame = q.get(timeout=min(0.2, max(0, deadline - time.monotonic())))
                    break
                except queue.Empty:
                    pass
            if frame is None:
                raise ConnectionLost(f"timed out waiting for {response_type.name}")
            if frame.frame_type != response_type:
                raise ConnectionLost(f"unexpected reply {frame.frame_type.name}")
            return frame.json()
        except (OSError, SessionError, FrameError) as e:
            raise ConnectionLost(str(e)) from e
        finally:
            router.unregister(cid)

    def list_files(self) -> list[str]:
        result = self._control_request(FrameType.FILE_LIST_REQUEST,
                                       FrameType.FILE_LIST_RESPONSE, {})
        if not result.get("ok"):
            raise RuntimeError(result.get("error", "list failed"))
        return result.get("files", [])

    def batch(self, action, payload):
        if not self.connected or not self.session.supports('batch_approval'):
            raise ValueError('This host does not support group approval. Update both devices.')
        kind = {'request': FrameType.BATCH_REQUEST, 'status': FrameType.BATCH_STATUS,
                'cancel': FrameType.BATCH_CANCEL}[action]
        result = self._control_request(kind, FrameType.BATCH_RESPONSE, payload)
        if not result.get('ok'):
            raise ValueError(result.get('error', 'Batch request failed'))
        return result

    def delete(self, filename: str) -> bool:
        result = self._control_request(FrameType.FILE_DELETE_REQUEST,
                                       FrameType.FILE_DELETE_RESPONSE,
                                       {"filename": filename})
        return bool(result.get("ok"))
