"""
framing — Transport layer: universal frame codec.

Every byte that crosses the wire is wrapped in one frame format.
There are no more two modes (JSON messages vs raw file bytes).
Everything is a frame.

Frame layout (all integers big-endian):

  Offset  Size  Field
  ──────  ────  ───────────────────────────────────────────────────
       0     4  Magic        0x46543200  ("FT2\x00")
       4     1  Version      Protocol version (currently 2)
       5     1  Frame type   FrameType enum value (uint8)
       6     1  Flags        Bit-field (see FrameFlags)
       7     1  Reserved     Must be 0x00 on send and receive
       8    32  Transfer ID  ASCII hex UUID4 (32 chars); all-zeros
                             for session-level frames (HELLO, AUTH, …)
      40     4  Sequence     uint32 — per-transfer chunk counter
      44     4  Payload len  uint32 — byte length of the payload
      48     ?  Payload      Frame-type-specific bytes

Total header size: 48 bytes.

Payload encoding per frame type:
  - HELLO / HELLO_ACK              → JSON (UTF-8)
  - AUTH / AUTH_OK / AUTH_FAIL     → JSON (UTF-8)
  - TRANSFER_CREATE / _ACCEPT /
    _PAUSE / _RESUME / _CANCEL /
    _COMPLETE / _ERROR / _ACK      → JSON (UTF-8)
  - TRANSFER_DATA                  → raw bytes (the actual file chunk)
  - PING / PONG                    → empty payload

The frame codec lives here and knows nothing about what the payloads
mean.  Semantic interpretation belongs in session_protocol.py.
"""

from __future__ import annotations

import json
import math
import socket
import struct
import threading
import time
import weakref
from enum import IntEnum

# ── Constants ─────────────────────────────────────────────────────────────────

MAGIC       = 0x46543200          # "FT2\x00"
VERSION     = 2
SUPPORTED_VERSIONS = frozenset({2, 3})

HEADER_SIZE = 48                  # bytes — fixed for the supported wire formats
_NULL_TRANSFER_ID = "0" * 32     # used for session-level frames
_STRUCT_HEADER = struct.Struct(">IBBBB32sII")   # magic, ver, type, flags, reserved, tid, seq, plen
# sizes: 4 + 1 + 1 + 1 + 1 + 32 + 4 + 4 = 48  ✓

MAX_PAYLOAD_BYTES = 64 * 1024 * 1024   # 64 MB hard cap — reject anything larger
MAX_CONTROL_BYTES = 65536


# ── Frame types ───────────────────────────────────────────────────────────────

class FrameType(IntEnum):
    """
    Every frame on the wire carries exactly one of these type codes.
    Values are stable across releases — never renumber them.
    """
    # Session lifecycle
    HELLO           = 0x01
    HELLO_ACK       = 0x02

    # Authentication
    AUTH            = 0x10
    AUTH_OK         = 0x11
    AUTH_FAIL       = 0x12

    # Transfer control
    TRANSFER_CREATE  = 0x20
    TRANSFER_ACCEPT  = 0x21
    TRANSFER_REJECT  = 0x22
    TRANSFER_DATA    = 0x23
    TRANSFER_ACK     = 0x24
    TRANSFER_PAUSE   = 0x25
    TRANSFER_RESUME  = 0x26
    TRANSFER_CANCEL  = 0x27
    TRANSFER_COMPLETE = 0x28
    TRANSFER_ERROR   = 0x29
    WINDOW_UPDATE    = 0x2A
    TRANSFER_RETRY   = 0x2B

    # Keep-alive
    PING            = 0x30
    PONG            = 0x31

    # API integration: host browsing/management, outside any one transfer.
    # Carry a per-request correlation id in the frame's transfer_id
    # field (a fresh uuid4 per request, same convention TRANSFER_CREATE
    # already uses) — FrameRouter.dispatch() routes any frame by that
    # field regardless of type, so these reuse register()/get_queue()
    # with no router changes beyond recognizing them as unsolicited
    # inbound requests the way TRANSFER_CREATE already is.
    FILE_LIST_REQUEST   = 0x40
    FILE_LIST_RESPONSE  = 0x41
    FILE_DELETE_REQUEST = 0x42
    FILE_DELETE_RESPONSE = 0x43
    # Optional negotiated chat; correlation IDs never enter transfer queues.
    CHAT_MESSAGE        = 0x44
    CHAT_ACK            = 0x45
    BATCH_REQUEST       = 0x46
    BATCH_STATUS        = 0x47
    BATCH_CANCEL        = 0x48
    BATCH_RESPONSE      = 0x49


# ── Frame flags ───────────────────────────────────────────────────────────────

class FrameFlags(IntEnum):
    """Bit positions in the flags byte."""
    NONE        = 0x00
    FINAL_CHUNK = 0x01    # this DATA frame is the last chunk of a transfer
    COMPRESSED  = 0x02    # payload is gzip-compressed (DATA frames only)


# ── Exceptions ────────────────────────────────────────────────────────────────

class FrameError(Exception):
    """Raised for any malformed or unexpected frame on the wire."""


class ConnectionClosed(FrameError):
    """Raised when the peer closed the connection cleanly mid-read."""


# ── Frame dataclass ───────────────────────────────────────────────────────────

class Frame:
    """
    One decoded frame.  Deliberately not a frozen dataclass so callers
    can build frames by attribute assignment before sending.

    transfer_id is always a 32-char hex string:
      - "0" * 32          for session-level frames
      - uuid4().hex        for transfer-specific frames
    """
    __slots__ = ("frame_type", "flags", "transfer_id", "sequence", "payload", "version")

    def __init__(
        self,
        frame_type: FrameType,
        payload: bytes = b"",
        transfer_id: str = _NULL_TRANSFER_ID,
        sequence: int = 0,
        flags: int = FrameFlags.NONE,
        version: int = VERSION,
    ) -> None:
        self.frame_type  = frame_type
        self.flags       = flags
        self.transfer_id = transfer_id
        self.sequence    = sequence
        self.payload     = payload
        self.version     = version

    # ── convenience constructors ──────────────────────────────────────────

    @classmethod
    def json_frame(
        cls,
        frame_type: FrameType,
        data: dict,
        transfer_id: str = _NULL_TRANSFER_ID,
        sequence: int = 0,
        flags: int = FrameFlags.NONE,
    ) -> "Frame":
        """Build a frame whose payload is a UTF-8 JSON-encoded dict."""
        return cls(
            frame_type=frame_type,
            payload=json.dumps(data, separators=(",", ":")).encode("utf-8"),
            transfer_id=transfer_id,
            sequence=sequence,
            flags=flags,
        )

    @classmethod
    def data_frame(
        cls,
        transfer_id: str,
        sequence: int,
        chunk: bytes,
        is_final: bool = False,
        compressed: bool = False,
    ) -> "Frame":
        """Build a TRANSFER_DATA frame carrying raw file bytes."""
        flags = FrameFlags.NONE
        if is_final:
            flags |= FrameFlags.FINAL_CHUNK
        if compressed:
            flags |= FrameFlags.COMPRESSED
        return cls(
            frame_type=FrameType.TRANSFER_DATA,
            payload=chunk,
            transfer_id=transfer_id,
            sequence=sequence,
            flags=flags,
        )

    # ── flag helpers ──────────────────────────────────────────────────────

    @property
    def is_final(self) -> bool:
        return bool(self.flags & FrameFlags.FINAL_CHUNK)

    @property
    def is_compressed(self) -> bool:
        return bool(self.flags & FrameFlags.COMPRESSED)

    @property
    def is_session_frame(self) -> bool:
        """True for frames that belong to the session, not a specific transfer."""
        return self.transfer_id == _NULL_TRANSFER_ID

    # ── JSON payload helper ───────────────────────────────────────────────

    def json(self) -> dict:
        """Decode the payload as a UTF-8 JSON dict."""
        def unique_object(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError('Duplicate JSON key')
                result[key] = value
            return result
        def invalid_constant(value):
            raise ValueError('Non-finite JSON number')
        try:
            value = json.loads(self.payload.decode("utf-8"),
                               object_pairs_hook=unique_object, parse_constant=invalid_constant)
            if not isinstance(value, dict):
                raise FrameError("JSON frame payload must be an object")
            pending = [(value, 0)]
            while pending:
                item, depth = pending.pop()
                if depth > 64:
                    raise ValueError('JSON nesting exceeds 64 levels')
                if isinstance(item, dict):
                    pending.extend((v, depth+1) for v in item.keys())
                    pending.extend((v, depth+1) for v in item.values())
                elif isinstance(item, list):
                    pending.extend((v, depth+1) for v in item)
                elif isinstance(item, str):
                    item.encode('utf-8')  # Reject unpaired escaped surrogates.
                elif isinstance(item, float) and not math.isfinite(item):
                    raise ValueError('Non-finite JSON number')
            return value
        except (ValueError, UnicodeError, RecursionError) as e:
            raise FrameError(f"Payload is not valid JSON: {e}") from e

    def __repr__(self) -> str:
        tid = self.transfer_id[:8] if self.transfer_id != _NULL_TRANSFER_ID else "session"
        return (
            f"Frame({self.frame_type.name}, tid={tid}, "
            f"seq={self.sequence}, {len(self.payload)}B)"
        )


# ── Wire codec ────────────────────────────────────────────────────────────────

def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """
    Read exactly n bytes from sock, accumulating across short reads.
    Raises ConnectionClosed if the peer disconnected cleanly, or
    FrameError if the socket returns empty mid-read (unexpected close).
    """
    if n == 0:
        return b""
    buf = bytearray(n)
    view = memoryview(buf)
    received = 0
    while received < n:
        chunk = sock.recv_into(view[received:], n - received)
        if chunk == 0:
            if received == 0:
                raise ConnectionClosed("Peer closed the connection")
            raise FrameError(f"Connection closed after {received}/{n} bytes")
        received += chunk
    return bytes(buf)


def validate_transfer_id(transfer_id: str, *, allow_null: bool = True) -> None:
    if (not isinstance(transfer_id, str) or len(transfer_id) != 32
            or any(c not in "0123456789abcdef" for c in transfer_id)
            or (not allow_null and transfer_id == _NULL_TRANSFER_ID)):
        raise FrameError("transfer_id must be 32 lowercase ASCII hex chars")


def send_frame(sock: socket.socket, frame: Frame) -> None:
    """
    Serialize frame and write it atomically to sock via sendall().

    Thread-safety: the caller is responsible for ensuring that only one
    thread calls send_frame on a given socket at a time (use a lock at
    the Session level).
    """
    validate_transfer_id(frame.transfer_id)
    try:
        FrameType(frame.frame_type)
    except ValueError as exc:
        raise FrameError('Unknown frame type') from exc
    tid_bytes = frame.transfer_id.encode("ascii")
    if type(frame.sequence) is not int or not 0 <= frame.sequence <= 0xFFFFFFFF:
        raise FrameError("sequence must be a uint32")
    if frame.version not in SUPPORTED_VERSIONS:
        raise FrameError("Unsupported protocol version")
    if int(frame.flags) & ~int(FrameFlags.FINAL_CHUNK | FrameFlags.COMPRESSED):
        raise FrameError("Unknown frame flags")
    if not isinstance(frame.payload, bytes):
        raise FrameError("Frame payload must be bytes")

    plen = len(frame.payload)
    if plen > MAX_PAYLOAD_BYTES:
        raise FrameError(
            f"Payload too large: {plen} > {MAX_PAYLOAD_BYTES}"
        )
    if frame.frame_type != FrameType.TRANSFER_DATA and plen > MAX_CONTROL_BYTES:
        raise FrameError('Control payload too large')

    header = _STRUCT_HEADER.pack(
        MAGIC,
        frame.version,
        int(frame.frame_type),
        int(frame.flags),
        0,           # reserved
        tid_bytes,
        frame.sequence,
        plen,
    )
    # Session's send lock prevents frame interleaving. sendall can fail after
    # sending a prefix; the session must then be retired, never reuse the stream.
    sock.sendall(header + frame.payload)


class FrameReader:
    """Single-reader incremental decoder. Timeouts retain header AND payload bytes.

    Malformed framing is fatal: its length cannot safely be trusted to find the
    next frame. A caller must close/recover that session rather than scan bytes.
    This object deliberately does not own a socket, so compatibility readers can
    be weakly keyed by socket without keeping closed sockets alive.
    """
    def __init__(self, frame_timeout=60.0):
        self._buffer = bytearray()
        self._scratch = bytearray(65536)
        self._header = None
        self._failed = False
        self._started_at = None
        self._frame_timeout = frame_timeout

    def _check_deadline(self):
        if self._started_at is not None and time.monotonic() - self._started_at >= self._frame_timeout:
            self._failed = True
            raise FrameError('Partial frame assembly timed out')

    def _fill(self, sock, length):
        while len(self._buffer) < length:
            self._check_deadline()
            count = sock.recv_into(self._scratch, min(len(self._scratch), length - len(self._buffer)))
            if count == 0:
                if not self._buffer and self._header is None:
                    raise ConnectionClosed("Peer closed the connection")
                raise FrameError("Connection closed in a partial frame")
            if self._started_at is None:
                self._started_at = time.monotonic()
            self._buffer.extend(memoryview(self._scratch)[:count])

    def recv(self, sock, *, versions=SUPPORTED_VERSIONS, max_payload=MAX_PAYLOAD_BYTES, allowed_types=None):
        if self._failed:
            raise FrameError("Decoder is unusable after malformed framing")
        self._check_deadline()
        try:
            if self._header is None:
                self._fill(sock, HEADER_SIZE)
                magic, version, raw_type, flags, reserved, tid, seq, size = _STRUCT_HEADER.unpack(self._buffer)
                if magic != MAGIC:
                    raise FrameError("Bad frame magic")
                if version not in versions:
                    raise FrameError(f"Unsupported protocol version: {version}")
                if size > max_payload:
                    raise FrameError(f"Frame payload too large: {size} bytes")
                if reserved or flags & ~int(FrameFlags.FINAL_CHUNK | FrameFlags.COMPRESSED):
                    raise FrameError("Unknown frame flags or nonzero reserved byte")
                try:
                    kind = FrameType(raw_type)
                except ValueError as exc:
                    raise FrameError(f"Unknown frame type: {raw_type}") from exc
                if kind != FrameType.TRANSFER_DATA and size > MAX_CONTROL_BYTES:
                    raise FrameError('Control payload too large')
                try:
                    transfer_id = tid.decode("ascii")
                except UnicodeDecodeError as exc:
                    raise FrameError("Transfer ID contains non-ASCII bytes") from exc
                validate_transfer_id(transfer_id)
                self._header = (kind, flags, transfer_id, seq, size, version)
                self._buffer.clear()
            kind, flags, tid, seq, size, version = self._header
            if allowed_types is not None and kind not in allowed_types:
                raise FrameError('Frame type is not permitted before authentication')
            if version not in versions or size > max_payload:
                raise FrameError("Frame exceeds this session's negotiated limits")
            self._fill(sock, size)
            frame = Frame(kind, bytes(self._buffer), tid, seq, flags, version)
            self._buffer.clear()
            self._header = None
            self._started_at = None
            return frame
        except TimeoutError:
            self._check_deadline()
            raise  # A polling timeout is not a frame boundary.
        except FrameError:
            self._failed = True
            raise


_readers = weakref.WeakKeyDictionary()
_readers_lock = threading.Lock()


def recv_frame(sock: socket.socket) -> Frame:
    """Compatibility single-reader API, retaining partial bytes across calls."""
    with _readers_lock:
        reader = _readers.get(sock)
        if reader is None:
            reader = _readers[sock] = FrameReader()
    return reader.recv(sock)
