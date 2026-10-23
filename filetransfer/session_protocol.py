"""
Protocol semantics: version negotiation and the
Session object that enforces the session / transfer separation invariant.

negotiation — Version negotiation
─────────────────────────────
Before any transfer, peers exchange HELLO / HELLO_ACK frames that
declare which protocol version they speak and which capability flags
they support.  Only the intersection of both sets is used.

  CLIENT                           SERVER
    │                                │
    │──── HELLO ──────────────────►  │
    │         version=2              │
    │         features=[pause,       │
    │                  resume,       │
    │                  cancel,       │
    │                  concurrent]   │
    │                                │
    │  ◄── HELLO_ACK ─────────────── │
    │         version=2              │
    │         features=[pause,       │
    │                  resume,       │
    │                  cancel,       │
    │                  concurrent]   │
    │                                │

HELLO_ACK carries the negotiated feature set (server's intersection
with the client's offer).  The agreed_features attribute on the
NegotiatedSession records what was actually agreed.

session isolation — Session / transfer separation
────────────────────────────────────────
The central architectural rule from the design doc:

    A transfer failure must NEVER implicitly mean session failure.
    A cancel of one transfer must NEVER close the connection.

Session encodes this by owning:
  - The socket (and its send lock)
  - Authentication state
  - Negotiated capabilities

Transfers are identified by their transfer_id string.  Session never
holds a reference to Transfer objects — that belongs to TransferManager.
Session only cares about frames on the wire.

The invariant is enforced structurally:
  - send_frame / recv_frame operate on individual frames
  - There is no "send the whole file" method here — that belongs in
    the higher-level transfer engine (transfer creation)
  - close_session() is the ONLY method that touches the socket itself;
    cancel-one-transfer operations send a TRANSFER_CANCEL frame and
    return — they do not call close_session()
"""

from __future__ import annotations

import threading
import socket
import ssl
from typing import Optional

from .logsetup import get_logger
from . import config
from .tls_io import TLSIO
from .transport import (
    Frame,
    FrameError,
    FrameType,
    FrameFlags,
    ConnectionClosed,
    recv_frame,
    send_frame,
    VERSION,
    SUPPORTED_VERSIONS,
    FrameReader,
    _NULL_TRANSFER_ID,
)

log = get_logger("filetransfer.session_protocol")


# ── Capability flags ──────────────────────────────────────────────────────────

class Feature:
    """
    String constants for protocol capability names.
    Both sides advertise a set; the agreed set is the intersection.
    Using strings (not ints) keeps HELLO payloads self-documenting.
    """
    PAUSE       = "pause"
    RESUME      = "resume"
    CANCEL      = "cancel"
    CONCURRENT  = "concurrent"
    COMPRESSION = "compression"
    FLOW_CONTROL = "flow_control"
    RETRY_RANGES = "retry_ranges"
    SCOPED_FLOW = 'scoped_flow_control'
    RECOVERY = 'recovery'
    CHAT = 'chat'
    BATCH_APPROVAL = 'batch_approval'

# Full set this implementation supports
ALL_FEATURES: frozenset[str] = frozenset({
    Feature.PAUSE,
    Feature.RESUME,
    Feature.CANCEL,
    Feature.CONCURRENT,
    Feature.FLOW_CONTROL,
    Feature.RETRY_RANGES,
    Feature.SCOPED_FLOW,
    Feature.RECOVERY,
    Feature.CHAT,
    Feature.BATCH_APPROVAL,
})


# ── Negotiation result ────────────────────────────────────────────────────────

class NegotiatedSession:
    """
    Immutable result of a successful HELLO / HELLO_ACK exchange.
    Attached to a Session after negotiation completes.
    """
    __slots__ = ("protocol_version", "agreed_features", "peer_name")

    def __init__(
        self,
        protocol_version: int,
        agreed_features: frozenset[str],
        peer_name: str = "",
    ) -> None:
        self.protocol_version = protocol_version
        self.agreed_features  = agreed_features
        self.peer_name        = peer_name

    def supports(self, feature: str) -> bool:
        return feature in self.agreed_features

    def to_dict(self) -> dict:
        return {
            "protocol_version": self.protocol_version,
            "agreed_features":  sorted(self.agreed_features),
            "peer_name":        self.peer_name,
        }

    def __repr__(self) -> str:
        return (
            f"NegotiatedSession(v{self.protocol_version}, "
            f"features={sorted(self.agreed_features)})"
        )


# ── Exceptions ────────────────────────────────────────────────────────────────

class NegotiationError(Exception):
    """Raised when HELLO / HELLO_ACK exchange fails."""

class AuthError(Exception):
    """Raised when authentication is rejected by the peer."""

class SessionError(Exception):
    """Raised for session-level protocol violations."""


def _versions(values) -> frozenset[int]:
    if (not isinstance(values, (list, tuple)) or not 1 <= len(values) <= 16
            or any(type(v) is not int or not 1 <= v <= 255 for v in values)):
        raise NegotiationError("versions must be a nonempty list of uint8 versions")
    return frozenset(values)


def _features(values) -> frozenset[str]:
    if (not isinstance(values, list) or len(values) > 64
            or any(not isinstance(v, str) or not 1 <= len(v) <= 64 for v in values)):
        raise NegotiationError("features must be a bounded list of strings")
    return frozenset(values)


# ── Session ───────────────────────────────────────────────────────────────────

class Session:
    """
    Owns one TCP/TLS socket and enforces the session/transfer invariant.

    Responsibilities:
      - Send and receive frames (one at a time, with a send lock)
      - Track authentication state
      - Track negotiated capabilities
      - close_session(): the ONLY method that closes the socket

    Non-responsibilities (by design):
      - File I/O
      - Transfer state (that's TransferManager)
      - Scheduling
      - Anything that knows about specific filenames

    Thread-safety:
      - _send_lock ensures frames are written atomically; multiple
        threads may call send_frame_* safely from different transfers.
      - recv_frame() is NOT protected by a lock — the caller (typically
        a dedicated reader thread) is responsible for serializing reads.
    """

    def __init__(self, sock, peer_addr: tuple, name: str = "", *,
                 supported_versions=SUPPORTED_VERSIONS) -> None:
        """
        sock      — a connected socket (plain or TLS-wrapped)
        peer_addr — (host, port) tuple, used only for logging
        name      — human label for this side ("client" / "server")
        """
        self._sock = sock
        self._io = TLSIO(sock, write_timeout=config.SOCKET_TIMEOUT_SECONDS) if isinstance(sock, ssl.SSLSocket) else sock
        self._supported_versions = _versions(list(supported_versions))
        self._reader = FrameReader(frame_timeout=config.SOCKET_TIMEOUT_SECONDS)
        self.peer_addr = peer_addr
        self.name = name

        self._send_lock = threading.Lock()
        self._authenticated = False
        self._recv_lock = threading.Lock()
        self._negotiation: Optional[NegotiatedSession] = None
        self._closed = False
        # recovery: set once the connection is known dead (a send hit
        # an OS error, or the FrameRouter's reader/keepalive declared
        # it lost). Distinct from _closed, which only records that WE
        # closed it deliberately.
        self._lost = threading.Event()

    # ── properties ────────────────────────────────────────────────────────

    @property
    def authenticated(self) -> bool:
        return self._authenticated

    @property
    def negotiation(self) -> Optional[NegotiatedSession]:
        """None until negotiate_as_client() / negotiate_as_server() succeeds."""
        return self._negotiation

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def is_lost(self) -> bool:
        """True once the connection is dead or has been closed. Engines
        use this to tell a connection loss (recoverable — the transfer
        goes CONNECTION_LOST) from a genuine transfer failure."""
        return self._lost.is_set() or self._closed

    def mark_lost(self) -> None:
        self._lost.set()

    def set_read_timeout(self, timeout):
        """Poll reads independently of TLS frame writes."""
        if self._io is not self._sock:
            self._io.read_timeout = timeout
        else:
            getattr(self._sock, '_s', self._sock).settimeout(timeout)

    def supports(self, feature: str) -> bool:
        """True if the negotiated session includes this feature."""
        return (
            self._negotiation is not None
            and self._negotiation.supports(feature)
        )

    # ── raw frame I/O ─────────────────────────────────────────────────────

    def send(self, frame: Frame) -> None:
        """
        Send one frame.  Thread-safe: acquires send lock.

        Validation errors leave the session usable. A transport write error
        may have sent a partial frame: poison and shut down that stream so
        all transfers recover through a fresh authenticated session.
        """
        if self._closed:
            raise SessionError("Cannot send on a closed session")
        if frame.is_compressed:
            raise SessionError('Compression is not implemented by this session')
        if frame.frame_type in (FrameType.BATCH_REQUEST, FrameType.BATCH_STATUS, FrameType.BATCH_CANCEL, FrameType.BATCH_RESPONSE):
            if not self.authenticated or not self.supports(Feature.BATCH_APPROVAL):
                raise SessionError('Batch approval requires an authenticated negotiated session')
        if frame.frame_type in (FrameType.CHAT_MESSAGE, FrameType.CHAT_ACK):
            if not self.authenticated or not self.supports(Feature.CHAT):
                raise SessionError('Chat requires authentication and negotiated chat support')
        requirement = {FrameType.TRANSFER_PAUSE: Feature.PAUSE,
                       FrameType.TRANSFER_RESUME: Feature.RESUME,
                       FrameType.TRANSFER_CANCEL: Feature.CANCEL,
                       FrameType.WINDOW_UPDATE: Feature.FLOW_CONTROL,
                       FrameType.TRANSFER_RETRY: Feature.RETRY_RANGES}.get(frame.frame_type)
        if self._negotiation is not None and requirement and not self.supports(requirement):
            raise SessionError(f'Feature was not negotiated: {requirement}')
        with self._send_lock:
            if self.is_lost:
                raise SessionError('Cannot send on a lost or closed session')
            try:
                version = self._negotiation.protocol_version if self._negotiation else VERSION
                send_frame(self._io, Frame(frame.frame_type, frame.payload, frame.transfer_id,
                                             frame.sequence, frame.flags, version))
            except OSError:
                self._lost.set()
                # sendall failure has an unknown partial-frame boundary. No
                # writer may append another frame, and wake the reader so the
                # scheduler recovers every transfer on a fresh session.
                try:
                    underlying = getattr(self._sock, '_s', self._sock)
                    if isinstance(underlying, socket.socket):
                        socket.socket.shutdown(underlying, socket.SHUT_RDWR)
                    else:
                        underlying.shutdown(socket.SHUT_RDWR)
                except (OSError, AttributeError):
                    pass
                raise

    def recv(self) -> Frame:
        """
        Receive one frame.  NOT thread-safe — only one thread should
        be calling recv() at a time (the reader loop).

        Raises ConnectionClosed, FrameError, OSError, socket.timeout.
        Does NOT close the session on error.
        """
        if self._closed:
            raise SessionError("Cannot recv on a closed session")
        versions = {self._negotiation.protocol_version} if self._negotiation else SUPPORTED_VERSIONS
        with self._recv_lock:
            if self._closed:
                raise SessionError('Cannot recv on a closed session')
            return self._reader.recv(self._io, versions=versions,
                max_payload=64 * 1024 * 1024 if self._authenticated else 65536,
                allowed_types=None if self._authenticated else {
                    FrameType.HELLO, FrameType.HELLO_ACK, FrameType.AUTH,
                    FrameType.AUTH_OK, FrameType.AUTH_FAIL})

    # ── convenience senders ───────────────────────────────────────────────

    def send_json(
        self,
        frame_type: FrameType,
        data: dict,
        transfer_id: str = _NULL_TRANSFER_ID,
        sequence: int = 0,
        flags: int = FrameFlags.NONE,
    ) -> None:
        """Build a JSON frame and send it."""
        self.send(Frame.json_frame(
            frame_type, data,
            transfer_id=transfer_id,
            sequence=sequence,
            flags=flags,
        ))

    def send_data(
        self,
        transfer_id: str,
        sequence: int,
        chunk: bytes,
        is_final: bool = False,
        compressed: bool = False,
    ) -> None:
        """Send a TRANSFER_DATA frame."""
        self.send(Frame.data_frame(
            transfer_id=transfer_id,
            sequence=sequence,
            chunk=chunk,
            is_final=is_final,
            compressed=compressed,
        ))

    # ── negotiation: version negotiation ──────────────────────────────────────

    def negotiate_as_client(
        self,
        offered_features: Optional[frozenset[str]] = None,
        client_name: str = "client",
    ) -> NegotiatedSession:
        """
        Initiator side of the HELLO handshake.

        Sends HELLO with our offered features, then waits for HELLO_ACK
        from the server containing the agreed (intersection) features.

        Raises NegotiationError on any mismatch or unexpected frame.
        """
        if self._negotiation is not None:
            raise NegotiationError("Session is already negotiated")
        offered_features = ALL_FEATURES if offered_features is None else frozenset(offered_features) & ALL_FEATURES

        log.debug("Sending HELLO (client, features=%s)", sorted(offered_features))
        self.send_json(FrameType.HELLO, {
            "version": VERSION if VERSION in self._supported_versions else max(self._supported_versions),
            "versions": sorted(self._supported_versions, reverse=True),
            "features": sorted(offered_features),
            "name":     client_name,
        })

        try:
            ack_frame = self.recv()
        except FrameError as exc:
            raise NegotiationError(str(exc)) from exc
        if ack_frame.frame_type != FrameType.HELLO_ACK:
            raise NegotiationError(
                f"Expected HELLO_ACK, got {ack_frame.frame_type.name}"
            )

        payload = ack_frame.json()
        if payload.get("error"):
            raise NegotiationError(str(payload["error"]))
        server_version = payload.get("version", 0)
        if type(server_version) is not int or server_version not in self._supported_versions:
            raise NegotiationError(
                f"Protocol version mismatch: server={server_version}, us={VERSION}"
            )

        if "versions" in payload:
            common = self._supported_versions & _versions(payload["versions"])
            if not common or server_version != max(common):
                raise NegotiationError("Server did not select the highest common version")
        agreed = _features(payload.get("features", []))
        if not agreed <= offered_features:
            raise NegotiationError("Server selected features the client did not offer")
        peer_name = payload.get("name", "")
        self._negotiation = NegotiatedSession(
            protocol_version=server_version,
            agreed_features=agreed,
            peer_name=peer_name,
        )
        log.info(
            "Negotiation complete (client): agreed features=%s peer=%r",
            sorted(agreed), peer_name,
        )
        return self._negotiation

    def negotiate_as_server(
        self,
        server_name: str = "server",
        supported_features: Optional[frozenset[str]] = None,
    ) -> NegotiatedSession:
        """
        Responder side of the HELLO handshake.

        Waits for HELLO from the client, computes the intersection of
        features, and replies with HELLO_ACK.

        Raises NegotiationError on any mismatch or unexpected frame.
        """
        if self._negotiation is not None:
            raise NegotiationError("Session is already negotiated")
        supported_features = ALL_FEATURES if supported_features is None else frozenset(supported_features) & ALL_FEATURES

        try:
            hello_frame = self.recv()
        except FrameError as exc:
            raise NegotiationError(str(exc)) from exc
        if hello_frame.frame_type != FrameType.HELLO:
            raise NegotiationError(
                f"Expected HELLO, got {hello_frame.frame_type.name}"
            )

        payload = hello_frame.json()
        try:
            client_versions = _versions(payload.get("versions", [payload.get("version", 0)]))
            client_features = _features(payload.get("features", []))
        except NegotiationError as exc:
            self.send_json(FrameType.HELLO_ACK, {"version": VERSION, "features": [], "error": str(exc)})
            raise
        common = self._supported_versions & client_versions
        if not common:
            # Send a rejection before raising so the client gets a message
            self.send_json(FrameType.HELLO_ACK, {
                "version":  VERSION,
                "features": [],
                "name":     server_name,
                "error":    (
                    f"Version mismatch: client={sorted(client_versions)}, "
                    f"server={sorted(self._supported_versions)}"
                ),
            })
            raise NegotiationError(
                "Protocol version mismatch: no common version"
            )

        selected = max(common)
        peer_name = payload.get("name", "")
        # Agreed set = intersection: only use features both sides know
        agreed = supported_features & client_features

        self.send_json(FrameType.HELLO_ACK, {
            "version": selected,
            "versions": sorted(self._supported_versions, reverse=True),
            "features": sorted(agreed),
            "name":     server_name,
        })

        self._negotiation = NegotiatedSession(
            protocol_version=selected,
            agreed_features=agreed,
            peer_name=peer_name,
        )
        log.info(
            "Negotiation complete (server): agreed features=%s peer=%r",
            sorted(agreed), peer_name,
        )
        return self._negotiation

    # ── negotiation: authentication ───────────────────────────────────────────
    # PIN / approval auth is kept separate from negotiation so they can
    # be composed in any order (negotiate first, auth second).

    def send_auth(self, pin: str, recovery_key: Optional[str] = None) -> None:
        """Client: send AUTH frame with PIN."""
        payload = {'pin': pin}
        if recovery_key is not None and self.supports(Feature.RECOVERY):
            payload['recovery_key'] = recovery_key
        self.send_json(FrameType.AUTH, payload)

    def recv_auth_payload(self) -> dict:
        try:
            frame = self.recv()
        except FrameError as exc:
            raise SessionError(str(exc)) from exc
        if frame.frame_type != FrameType.AUTH:
            raise SessionError(f'Expected AUTH frame, got {frame.frame_type.name}')
        return frame.json()

    def recv_auth(self) -> str:
        """
        Server: receive AUTH frame and return the supplied PIN.
        Raises SessionError if the frame is not AUTH.
        """
        return self.recv_auth_payload().get('pin', '')

    def send_auth_ok(self, pending: bool = False, **metadata) -> None:
        """Server: send AUTH_OK (with pending=True while waiting for host approval)."""
        self.send_json(FrameType.AUTH_OK, {**metadata, "ok": True, "pending": pending})

    def send_auth_fail(self, reason: str) -> None:
        """Server: send AUTH_FAIL."""
        self.send_json(FrameType.AUTH_FAIL, {"ok": False, "error": reason})

    def recv_auth_result(self) -> dict:
        """
        Client: receive AUTH_OK or AUTH_FAIL and return the payload dict.
        Raises AuthError on AUTH_FAIL; SessionError on unexpected frame type.
        """
        try:
            frame = self.recv()
        except FrameError as exc:
            raise SessionError(str(exc)) from exc
        if frame.frame_type == FrameType.AUTH_FAIL:
            payload = frame.json()
            raise AuthError(payload.get("error", "authentication rejected"))
        if frame.frame_type not in (FrameType.AUTH_OK,):
            raise SessionError(
                f"Expected AUTH_OK/AUTH_FAIL, got {frame.frame_type.name}"
            )
        payload = frame.json()
        self._authenticated = payload.get("ok", False) is True and not payload.get('pending', False)
        return payload

    def mark_authenticated(self) -> None:
        """Server: mark this session as fully authenticated."""
        self._authenticated = True

    # ── session isolation: session / transfer separation ────────────────────────────
    #
    # The key invariant: the only method that closes the socket is
    # close_session(). Every transfer-level operation sends a control
    # frame and returns — it does NOT call close_session().
    #
    # These helpers implement that discipline as an explicit API:

    def send_transfer_cancel(self, transfer_id: str, reason: str = "") -> None:
        """
        Send a TRANSFER_CANCEL frame for one specific transfer.

        session isolation invariant: this does NOT close the session or affect
        any other transfer. The session remains open.
        """
        self.send_json(
            FrameType.TRANSFER_CANCEL,
            {"reason": reason},
            transfer_id=transfer_id,
        )
        log.debug("Sent TRANSFER_CANCEL for %s: %r", transfer_id[:8], reason)

    def send_transfer_error(self, transfer_id: str, error: str) -> None:
        """
        Notify the peer that a specific transfer encountered an error.

        session isolation invariant: this does NOT close the session.
        The session and all other transfers continue unaffected.
        """
        self.send_json(
            FrameType.TRANSFER_ERROR,
            {"error": error},
            transfer_id=transfer_id,
        )
        log.debug("Sent TRANSFER_ERROR for %s: %r", transfer_id[:8], error)

    def send_ping(self) -> None:
        """Send a PING keep-alive frame."""
        self.send(Frame(FrameType.PING))

    def send_pong(self) -> None:
        """Reply to a PING."""
        self.send(Frame(FrameType.PONG))

    # ── Session close (the ONLY place the socket is touched for teardown) ─

    def close_session(self, reason: str = "") -> None:
        """
        Gracefully close the session.

        This is the ONLY method in the entire session_protocol module that
        closes the underlying socket.  Nothing else may do so.

        Callers should be explicit: close because the SESSION ended,
        not because a transfer failed.  A transfer failing is handled
        by send_transfer_error() — not this.
        """
        if self._closed:
            return
        self._closed = True
        if reason:
            log.info("Session closing: %s (%s)", self.peer_addr, reason)
        else:
            log.info("Session closing: %s", self.peer_addr)
        try:
            underlying = getattr(self._sock, '_s', self._sock)
            if isinstance(underlying, socket.socket):
                socket.socket.shutdown(underlying, socket.SHUT_RDWR)
            else:
                underlying.shutdown(socket.SHUT_RDWR)
        except (OSError, AttributeError):
            pass
        try:
            with self._recv_lock, self._send_lock:
                self._sock.close()
        except OSError:
            pass

    def __repr__(self) -> str:
        state = "closed" if self._closed else (
            "authed" if self._authenticated else "open"
        )
        neg = repr(self._negotiation) if self._negotiation else "not negotiated"
        return f"Session(peer={self.peer_addr}, {state}, {neg})"
