"""
Tests for framing (transport.py) and (session_protocol.py).

No file I/O, no actual TCP sockets — everything uses in-process
socket pairs (socket.socketpair) so the codec is exercised against
real bytes, not mocks.

Coverage:
  transport.py
    Frame construction
      - json_frame and data_frame helpers
      - flag helpers (is_final, is_compressed, is_session_frame)
      - json() payload decoder
    Wire codec (send_frame / recv_frame)
      - roundtrip for every FrameType
      - session-level frame (null transfer_id)
      - transfer-level frame (real transfer_id)
      - zero-length payload
      - large payload (approaching but not exceeding MAX_PAYLOAD_BYTES)
      - all flag combinations
      - sequence numbers preserved
    Error cases
      - bad magic rejected
      - wrong version rejected
      - unknown frame type rejected
      - oversized payload rejected at both send and recv
      - connection closed mid-header
      - connection closed mid-payload
      - transfer_id not 32 chars rejected at send

  session_protocol.py
    NegotiatedSession
      - supports() and to_dict()
    Negotiation (negotiation)
      - client/server happy path — matching versions, all features
      - feature intersection: server only advertises subset
      - feature intersection: client only offers subset
      - version mismatch rejected (server sends HELLO_ACK with error)
      - wrong first frame type raises NegotiationError
      - negotiation result stored on session
    Authentication
      - send_auth / recv_auth round-trip
      - send_auth_ok / recv_auth_result marks authenticated
      - send_auth_fail / recv_auth_result raises AuthError
      - wrong frame type raises SessionError
    session isolation invariant: session/transfer separation
      - send_transfer_cancel sends TRANSFER_CANCEL, NOT session close
      - send_transfer_error sends TRANSFER_ERROR, NOT session close
      - close_session closes socket; subsequent send raises SessionError
      - close_session idempotent (safe to call twice)
      - cancel one transfer, other transfer's frames still flow
    Keep-alive
      - ping/pong round-trip
    Send on closed session raises SessionError
"""

from __future__ import annotations

import json
import socket
import struct
import threading
from typing import Tuple

import pytest

from filetransfer.transport import (
    Frame,
    FrameError,
    FrameFlags,
    FrameType,
    ConnectionClosed,
    HEADER_SIZE,
    MAGIC,
    VERSION,
    MAX_PAYLOAD_BYTES,
    _NULL_TRANSFER_ID,
    _STRUCT_HEADER,
    send_frame,
    recv_frame,
)
from filetransfer.session_protocol import (
    ALL_FEATURES,
    AuthError,
    Feature,
    NegotiatedSession,
    NegotiationError,
    Session,
    SessionError,
)


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def socket_pair() -> Tuple[socket.socket, socket.socket]:
    """Return a connected pair of sockets (a, b)."""
    return socket.socketpair()


def fake_transfer_id() -> str:
    import uuid
    return uuid.uuid4().hex  # 32 hex chars


def make_session_pair(name_a="client", name_b="server", authenticated=False):
    """Return two Session objects connected to each other."""
    a_sock, b_sock = socket_pair()
    a = Session(a_sock, ("127.0.0.1", 9000), name=name_a)
    b = Session(b_sock, ("127.0.0.1", 9001), name=name_b)
    if authenticated:
        a.mark_authenticated()
        b.mark_authenticated()
    return a, b


def negotiate_pair(client: Session, server: Session,
                   client_features=None, server_features=None):
    """Run HELLO handshake between client and server in threads."""
    results = {}
    errors = {}

    def run_client():
        try:
            results["client"] = client.negotiate_as_client(
                offered_features=client_features,
            )
        except Exception as e:
            errors["client"] = e

    def run_server():
        try:
            results["server"] = server.negotiate_as_server(
                supported_features=server_features,
            )
        except Exception as e:
            errors["server"] = e

    tc = threading.Thread(target=run_client)
    ts = threading.Thread(target=run_server)
    tc.start(); ts.start()
    tc.join(timeout=3); ts.join(timeout=3)

    return results, errors


# ═══════════════════════════════════════════════════════════════════════
# framing — Frame construction
# ═══════════════════════════════════════════════════════════════════════

class TestFrameConstruction:
    def test_default_transfer_id_is_null(self):
        f = Frame(FrameType.PING)
        assert f.transfer_id == _NULL_TRANSFER_ID

    def test_json_frame_encodes_payload(self):
        data = {"key": "value", "num": 42}
        f = Frame.json_frame(FrameType.HELLO, data)
        assert json.loads(f.payload.decode()) == data

    def test_json_frame_type_preserved(self):
        f = Frame.json_frame(FrameType.AUTH, {"pin": "123456"})
        assert f.frame_type == FrameType.AUTH

    def test_json_frame_transfer_id(self):
        tid = fake_transfer_id()
        f = Frame.json_frame(FrameType.TRANSFER_CREATE, {}, transfer_id=tid)
        assert f.transfer_id == tid

    def test_data_frame_type(self):
        f = Frame.data_frame("a" * 32, 0, b"hello")
        assert f.frame_type == FrameType.TRANSFER_DATA

    def test_data_frame_payload(self):
        chunk = b"\x00\x01\x02\x03"
        f = Frame.data_frame("a" * 32, 7, chunk)
        assert f.payload == chunk

    def test_data_frame_sequence(self):
        f = Frame.data_frame("a" * 32, 99, b"x")
        assert f.sequence == 99

    def test_data_frame_not_final_by_default(self):
        f = Frame.data_frame("a" * 32, 0, b"x")
        assert not f.is_final

    def test_data_frame_final_flag(self):
        f = Frame.data_frame("a" * 32, 0, b"x", is_final=True)
        assert f.is_final

    def test_data_frame_compressed_flag(self):
        f = Frame.data_frame("a" * 32, 0, b"x", compressed=True)
        assert f.is_compressed

    def test_data_frame_both_flags(self):
        f = Frame.data_frame("a" * 32, 0, b"x", is_final=True, compressed=True)
        assert f.is_final and f.is_compressed

    def test_is_session_frame_null_id(self):
        f = Frame(FrameType.PING)
        assert f.is_session_frame

    def test_is_session_frame_false_with_real_id(self):
        f = Frame(FrameType.TRANSFER_DATA, transfer_id=fake_transfer_id())
        assert not f.is_session_frame

    def test_json_decode(self):
        data = {"a": 1}
        f = Frame.json_frame(FrameType.HELLO, data)
        assert f.json() == data

    def test_json_decode_invalid_raises(self):
        f = Frame(FrameType.HELLO, payload=b"not json")
        with pytest.raises(FrameError):
            f.json()

    def test_repr_contains_frame_type(self):
        r = repr(Frame(FrameType.PING))
        assert "PING" in r


# ═══════════════════════════════════════════════════════════════════════
# framing — Wire codec roundtrips
# ═══════════════════════════════════════════════════════════════════════

class TestWireRoundtrip:
    def _roundtrip(self, frame: Frame) -> Frame:
        # send_frame() blocks on sendall() once the payload exceeds the
        # socketpair's kernel buffer (a few hundred KB on Linux). Since
        # nothing here is a persistent reader (unlike FrameRouter's
        # background thread in production), a large payload sent
        # before recv_frame() is called would deadlock. Send from a
        # background thread so recv_frame() can drain concurrently,
        # exactly like the real reader does.
        a, b = socket_pair()
        send_exc: list[BaseException] = []

        def _send():
            try:
                send_frame(a, frame)
            except BaseException as e:  # noqa: BLE001 - surfaced below
                send_exc.append(e)

        t = threading.Thread(target=_send, daemon=True)
        t.start()
        try:
            received = recv_frame(b)
        finally:
            t.join(timeout=5)
            a.close(); b.close()
        if send_exc:
            raise send_exc[0]
        assert not t.is_alive(), "send_frame did not complete"
        return received

    def test_session_frame_roundtrip(self):
        original = Frame.json_frame(FrameType.HELLO, {"version": 2})
        received = self._roundtrip(original)
        assert received.frame_type == FrameType.HELLO
        assert received.transfer_id == _NULL_TRANSFER_ID
        assert received.json() == {"version": 2}

    def test_transfer_frame_roundtrip(self):
        tid = fake_transfer_id()
        original = Frame.json_frame(
            FrameType.TRANSFER_CREATE,
            {"filename": "a.bin"},
            transfer_id=tid,
            sequence=5,
        )
        received = self._roundtrip(original)
        assert received.frame_type == FrameType.TRANSFER_CREATE
        assert received.transfer_id == tid
        assert received.sequence == 5
        assert received.json()["filename"] == "a.bin"

    def test_data_frame_roundtrip(self):
        tid = fake_transfer_id()
        chunk = bytes(range(256)) * 100
        original = Frame.data_frame(tid, 42, chunk, is_final=True)
        received = self._roundtrip(original)
        assert received.frame_type == FrameType.TRANSFER_DATA
        assert received.payload == chunk
        assert received.is_final
        assert received.sequence == 42
        assert received.transfer_id == tid

    def test_zero_payload_roundtrip(self):
        original = Frame(FrameType.PING)
        received = self._roundtrip(original)
        assert received.frame_type == FrameType.PING
        assert received.payload == b""

    def test_flags_preserved(self):
        flags = FrameFlags.FINAL_CHUNK | FrameFlags.COMPRESSED
        original = Frame(
            FrameType.TRANSFER_DATA,
            payload=b"x",
            transfer_id=fake_transfer_id(),
            flags=flags,
        )
        received = self._roundtrip(original)
        assert received.flags == flags

    def test_all_frame_types_roundtrip(self):
        for ft in FrameType:
            original = Frame(ft, payload=b"test", transfer_id=_NULL_TRANSFER_ID)
            received = self._roundtrip(original)
            assert received.frame_type == ft

    def test_large_payload_roundtrip(self):
        payload = b"A" * (1024 * 1024)  # 1 MB
        original = Frame(FrameType.TRANSFER_DATA,
                         payload=payload,
                         transfer_id=fake_transfer_id())
        received = self._roundtrip(original)
        assert received.payload == payload

    def test_sequence_number_max_uint32(self):
        original = Frame(FrameType.TRANSFER_DATA,
                         payload=b"x",
                         transfer_id=fake_transfer_id(),
                         sequence=0xFFFFFFFF)
        received = self._roundtrip(original)
        assert received.sequence == 0xFFFFFFFF


# ═══════════════════════════════════════════════════════════════════════
# framing — Error cases
# ═══════════════════════════════════════════════════════════════════════

class TestWireErrors:
    def test_bad_magic_rejected(self):
        a, b = socket_pair()
        try:
            # Write a header with wrong magic
            bad = _STRUCT_HEADER.pack(
                0xDEADBEEF, VERSION, FrameType.PING, 0, 0,
                _NULL_TRANSFER_ID.encode("ascii"), 0, 0
            )
            a.sendall(bad)
            with pytest.raises(FrameError, match="magic"):
                recv_frame(b)
        finally:
            a.close(); b.close()

    def test_wrong_version_rejected(self):
        a, b = socket_pair()
        try:
            bad = _STRUCT_HEADER.pack(
                MAGIC, 99, FrameType.PING, 0, 0,
                _NULL_TRANSFER_ID.encode("ascii"), 0, 0
            )
            a.sendall(bad)
            with pytest.raises(FrameError, match="version"):
                recv_frame(b)
        finally:
            a.close(); b.close()

    def test_unknown_frame_type_rejected(self):
        a, b = socket_pair()
        try:
            bad = _STRUCT_HEADER.pack(
                MAGIC, VERSION, 0xFF, 0, 0,
                _NULL_TRANSFER_ID.encode("ascii"), 0, 0
            )
            a.sendall(bad)
            with pytest.raises(FrameError, match="Unknown frame type"):
                recv_frame(b)
        finally:
            a.close(); b.close()

    def test_oversized_payload_rejected_at_recv(self):
        a, b = socket_pair()
        try:
            bad = _STRUCT_HEADER.pack(
                MAGIC, VERSION, FrameType.PING, 0, 0,
                _NULL_TRANSFER_ID.encode("ascii"), 0,
                MAX_PAYLOAD_BYTES + 1
            )
            a.sendall(bad)
            with pytest.raises(FrameError, match="too large"):
                recv_frame(b)
        finally:
            a.close(); b.close()

    def test_oversized_payload_rejected_at_send(self):
        a, b = socket_pair()
        try:
            oversized = Frame(
                FrameType.TRANSFER_DATA,
                payload=b"x" * (MAX_PAYLOAD_BYTES + 1),
                transfer_id=fake_transfer_id(),
            )
            with pytest.raises(FrameError, match="too large"):
                send_frame(a, oversized)
        finally:
            a.close(); b.close()

    def test_connection_closed_before_header(self):
        a, b = socket_pair()
        a.close()  # close immediately
        with pytest.raises(ConnectionClosed):
            recv_frame(b)
        b.close()

    def test_connection_closed_mid_header(self):
        a, b = socket_pair()
        try:
            # Send only 4 bytes of a 48-byte header
            a.sendall(b"\x00" * 4)
            a.close()
            with pytest.raises(FrameError):
                recv_frame(b)
        finally:
            b.close()

    def test_connection_closed_mid_payload(self):
        a, b = socket_pair()
        try:
            # Valid header claiming 100-byte payload, but close before sending it
            header = _STRUCT_HEADER.pack(
                MAGIC, VERSION, FrameType.PING, 0, 0,
                _NULL_TRANSFER_ID.encode("ascii"), 0, 100
            )
            a.sendall(header)
            a.close()
            with pytest.raises(FrameError):
                recv_frame(b)
        finally:
            b.close()

    def test_transfer_id_wrong_length_rejected_at_send(self):
        a, b = socket_pair()
        try:
            f = Frame(FrameType.PING, transfer_id="tooshort")
            with pytest.raises(FrameError):
                send_frame(a, f)
        finally:
            a.close(); b.close()

    def test_header_size_constant_is_48(self):
        assert HEADER_SIZE == 48
        assert _STRUCT_HEADER.size == 48


# ═══════════════════════════════════════════════════════════════════════
# negotiation — NegotiatedSession
# ═══════════════════════════════════════════════════════════════════════

class TestNegotiatedSession:
    def test_supports_included_feature(self):
        ns = NegotiatedSession(2, frozenset({Feature.PAUSE, Feature.RESUME}))
        assert ns.supports(Feature.PAUSE)
        assert ns.supports(Feature.RESUME)

    def test_supports_excluded_feature(self):
        ns = NegotiatedSession(2, frozenset({Feature.PAUSE}))
        assert not ns.supports(Feature.CANCEL)

    def test_to_dict_keys(self):
        ns = NegotiatedSession(2, frozenset({Feature.PAUSE}), peer_name="srv")
        d = ns.to_dict()
        assert d["protocol_version"] == 2
        assert Feature.PAUSE in d["agreed_features"]
        assert d["peer_name"] == "srv"

    def test_features_sorted_in_to_dict(self):
        ns = NegotiatedSession(2, frozenset(ALL_FEATURES))
        d = ns.to_dict()
        assert d["agreed_features"] == sorted(ALL_FEATURES)


# ═══════════════════════════════════════════════════════════════════════
# negotiation — Version negotiation
# ═══════════════════════════════════════════════════════════════════════

class TestNegotiation:
    def test_happy_path_all_features(self):
        client, server = make_session_pair()
        results, errors = negotiate_pair(client, server)

        assert not errors, f"Unexpected errors: {errors}"
        assert "client" in results and "server" in results
        assert results["client"].agreed_features == ALL_FEATURES
        assert results["server"].agreed_features == ALL_FEATURES

    def test_negotiation_stored_on_session(self):
        client, server = make_session_pair()
        negotiate_pair(client, server)
        assert client.negotiation is not None
        assert server.negotiation is not None

    def test_feature_intersection_server_subset(self):
        """Server supports only PAUSE+RESUME; client offers all."""
        server_features = frozenset({Feature.PAUSE, Feature.RESUME})
        client, server = make_session_pair()
        results, errors = negotiate_pair(
            client, server,
            client_features=ALL_FEATURES,
            server_features=server_features,
        )
        assert not errors
        assert results["client"].agreed_features == server_features
        assert results["server"].agreed_features == server_features

    def test_feature_intersection_client_subset(self):
        """Client offers only CANCEL; server supports all."""
        client_features = frozenset({Feature.CANCEL})
        client, server = make_session_pair()
        results, errors = negotiate_pair(
            client, server,
            client_features=client_features,
            server_features=ALL_FEATURES,
        )
        assert not errors
        assert results["client"].agreed_features == client_features
        assert results["server"].agreed_features == client_features

    def test_empty_intersection_allowed(self):
        """Both sides offer disjoint features — empty agreed set is valid."""
        client_features = frozenset({Feature.PAUSE})
        server_features = frozenset({Feature.CANCEL})
        client, server = make_session_pair()
        results, errors = negotiate_pair(
            client, server,
            client_features=client_features,
            server_features=server_features,
        )
        assert not errors
        assert results["client"].agreed_features == frozenset()

    def test_client_gets_wrong_first_frame(self):
        """Server sends PING instead of HELLO_ACK — client raises NegotiationError."""
        client_sock, server_sock = socket_pair()
        client = Session(client_sock, ("127.0.0.1", 0))

        err = {}

        def client_thread():
            try:
                client.negotiate_as_client()
            except NegotiationError as e:
                err["e"] = e
            finally:
                client_sock.close()

        tc = threading.Thread(target=client_thread)
        tc.start()

        # Server sends a PING instead of HELLO_ACK
        send_frame(server_sock, Frame(FrameType.PING))
        tc.join(timeout=3)
        server_sock.close()

        assert "e" in err, "Expected NegotiationError"

    def test_server_gets_wrong_first_frame(self):
        """Client sends PING instead of HELLO — server raises NegotiationError."""
        client_sock, server_sock = socket_pair()
        server = Session(server_sock, ("127.0.0.1", 0))

        err = {}

        def server_thread():
            try:
                server.negotiate_as_server()
            except NegotiationError as e:
                err["e"] = e
            finally:
                server_sock.close()

        ts = threading.Thread(target=server_thread)
        ts.start()

        send_frame(client_sock, Frame(FrameType.PING))
        ts.join(timeout=3)
        client_sock.close()

        assert "e" in err, "Expected NegotiationError"

    def test_supports_after_negotiation(self):
        client, server = make_session_pair()
        negotiate_pair(client, server,
                       client_features=frozenset({Feature.PAUSE}),
                       server_features=ALL_FEATURES)
        assert client.supports(Feature.PAUSE)
        assert not client.supports(Feature.CANCEL)

    def test_supports_before_negotiation_is_false(self):
        client, _ = make_session_pair()
        assert not client.supports(Feature.PAUSE)


# ═══════════════════════════════════════════════════════════════════════
# negotiation — Authentication
# ═══════════════════════════════════════════════════════════════════════

class TestAuthentication:
    def _auth_roundtrip(self, pin, accept=True):
        """
        Run a complete auth exchange in two threads.
        Returns (client_result, server_pin).
        """
        client, server = make_session_pair()
        results = {}
        errors = {}

        def client_thread():
            try:
                client.send_auth(pin)
                results["client"] = client.recv_auth_result()
            except AuthError as e:
                errors["auth"] = e
            finally:
                client._sock.close()

        def server_thread():
            try:
                received_pin = server.recv_auth()
                results["pin"] = received_pin
                if accept:
                    server.send_auth_ok()
                    server.mark_authenticated()
                else:
                    server.send_auth_fail("wrong PIN")
            finally:
                server._sock.close()

        tc = threading.Thread(target=client_thread)
        ts = threading.Thread(target=server_thread)
        tc.start(); ts.start()
        tc.join(timeout=3); ts.join(timeout=3)
        return results, errors

    def test_auth_ok_roundtrip(self):
        results, errors = self._auth_roundtrip("123456", accept=True)
        assert "auth" not in errors
        assert results.get("pin") == "123456"
        assert results["client"]["ok"] is True

    def test_auth_fail_raises_auth_error(self):
        results, errors = self._auth_roundtrip("wrongpin", accept=False)
        assert "auth" in errors
        assert isinstance(errors["auth"], AuthError)

    def test_mark_authenticated(self):
        client, server = make_session_pair()
        assert not server.authenticated
        server.mark_authenticated()
        assert server.authenticated
        client._sock.close(); server._sock.close()

    def test_recv_auth_wrong_frame_raises(self):
        client_sock, server_sock = socket_pair()
        server = Session(server_sock, ("127.0.0.1", 0))

        err = {}

        def server_thread():
            try:
                server.recv_auth()
            except SessionError as e:
                err["e"] = e
            finally:
                server_sock.close()

        ts = threading.Thread(target=server_thread)
        ts.start()

        # Client sends PING instead of AUTH
        send_frame(client_sock, Frame(FrameType.PING))
        ts.join(timeout=3)
        client_sock.close()

        assert "e" in err

    def test_recv_auth_result_wrong_frame_raises(self):
        client_sock, server_sock = socket_pair()
        client = Session(client_sock, ("127.0.0.1", 0))

        err = {}

        def client_thread():
            try:
                client.recv_auth_result()
            except SessionError as e:
                err["e"] = e
            finally:
                client_sock.close()

        tc = threading.Thread(target=client_thread)
        tc.start()

        # Server sends PING instead of AUTH_OK/AUTH_FAIL
        send_frame(server_sock, Frame(FrameType.PING))
        tc.join(timeout=3)
        server_sock.close()

        assert "e" in err


# ═══════════════════════════════════════════════════════════════════════
# session isolation — Session / transfer separation
# ═══════════════════════════════════════════════════════════════════════

class TestSessionTransferSeparation:
    """
    The central invariant of session isolation:
    cancel / error on one transfer must not close the session.
    close_session() is the ONLY method that closes the socket.
    """

    def test_cancel_sends_cancel_frame_not_close(self):
        """send_transfer_cancel sends a TRANSFER_CANCEL frame; session stays open."""
        client, server = make_session_pair(authenticated=True)
        tid = fake_transfer_id()

        frame_received = {}

        def server_thread():
            frame_received["f"] = server.recv()
            server._sock.close()

        ts = threading.Thread(target=server_thread)
        ts.start()

        client.send_transfer_cancel(tid, reason="user clicked cancel")
        ts.join(timeout=3)
        client._sock.close()

        f = frame_received["f"]
        assert f.frame_type == FrameType.TRANSFER_CANCEL
        assert f.transfer_id == tid
        assert f.json()["reason"] == "user clicked cancel"
        # Session was NOT closed by send_transfer_cancel
        assert not client.closed

    def test_error_sends_error_frame_not_close(self):
        """send_transfer_error sends TRANSFER_ERROR; session stays open."""
        client, server = make_session_pair(authenticated=True)
        tid = fake_transfer_id()

        frame_received = {}

        def server_thread():
            frame_received["f"] = server.recv()
            server._sock.close()

        ts = threading.Thread(target=server_thread)
        ts.start()

        client.send_transfer_error(tid, "checksum mismatch")
        ts.join(timeout=3)
        client._sock.close()

        f = frame_received["f"]
        assert f.frame_type == FrameType.TRANSFER_ERROR
        assert f.transfer_id == tid
        assert f.json()["error"] == "checksum mismatch"
        assert not client.closed

    def test_close_session_closes_socket(self):
        """close_session() marks the session closed and prevents further sends."""
        client, server = make_session_pair(authenticated=True)
        assert not client.closed
        client.close_session("test done")
        assert client.closed
        server._sock.close()

    def test_send_after_close_raises(self):
        """Sending on a closed session raises SessionError, not OSError."""
        client, server = make_session_pair(authenticated=True)
        client.close_session()
        with pytest.raises(SessionError):
            client.send(Frame(FrameType.PING))
        server._sock.close()

    def test_recv_after_close_raises(self):
        client, server = make_session_pair(authenticated=True)
        client.close_session()
        with pytest.raises(SessionError):
            client.recv()
        server._sock.close()

    def test_close_session_idempotent(self):
        """Calling close_session() twice must not raise."""
        client, server = make_session_pair(authenticated=True)
        client.close_session()
        client.close_session()  # second call is a no-op
        server._sock.close()

    def test_cancel_one_other_transfer_frames_still_flow(self):
        """
        Cancel transfer A; frames for transfer B must still be
        receivable on the same session — no session teardown.
        """
        client, server = make_session_pair(authenticated=True)
        tid_a = fake_transfer_id()
        tid_b = fake_transfer_id()

        received = []

        def server_thread():
            # Expect 3 frames: cancel(A), data(B), data(B final)
            for _ in range(3):
                received.append(server.recv())
            server._sock.close()

        ts = threading.Thread(target=server_thread)
        ts.start()

        client.send_transfer_cancel(tid_a, "cancelled by user")
        client.send_data(tid_b, sequence=0, chunk=b"chunk1")
        client.send_data(tid_b, sequence=1, chunk=b"chunk2", is_final=True)
        ts.join(timeout=3)
        client._sock.close()

        assert len(received) == 3
        assert received[0].frame_type == FrameType.TRANSFER_CANCEL
        assert received[0].transfer_id == tid_a
        assert received[1].frame_type == FrameType.TRANSFER_DATA
        assert received[1].transfer_id == tid_b
        assert received[2].frame_type == FrameType.TRANSFER_DATA
        assert received[2].is_final
        assert received[2].transfer_id == tid_b

    def test_multiple_transfers_interleaved_on_one_session(self):
        """
        Frames from three different transfers can be interleaved on the
        same session — the receiver distinguishes them by transfer_id.
        """
        client, server = make_session_pair(authenticated=True)
        tids = [fake_transfer_id() for _ in range(3)]
        n_per_transfer = 5

        received = []

        def server_thread():
            for _ in range(len(tids) * n_per_transfer):
                received.append(server.recv())
            server._sock.close()

        ts = threading.Thread(target=server_thread)
        ts.start()

        # Interleave: send one chunk per transfer, cycling
        for seq in range(n_per_transfer):
            for tid in tids:
                client.send_data(tid, sequence=seq, chunk=b"chunk")

        ts.join(timeout=3)
        client._sock.close()

        assert len(received) == len(tids) * n_per_transfer
        # Every transfer_id appears exactly n_per_transfer times
        for tid in tids:
            count = sum(1 for f in received if f.transfer_id == tid)
            assert count == n_per_transfer, f"{tid[:8]} appeared {count} times"

    def test_cancel_does_not_affect_closed_flag(self):
        """send_transfer_cancel must not set session._closed."""
        client, server = make_session_pair(authenticated=True)
        tid = fake_transfer_id()

        def server_thread():
            server.recv()
            server._sock.close()

        ts = threading.Thread(target=server_thread)
        ts.start()

        client.send_transfer_cancel(tid)
        ts.join(timeout=3)

        assert not client.closed  # session is still open
        client._sock.close()


# ═══════════════════════════════════════════════════════════════════════
# Keep-alive
# ═══════════════════════════════════════════════════════════════════════

class TestKeepAlive:
    def test_ping_pong_roundtrip(self):
        client, server = make_session_pair(authenticated=True)
        received = {}

        def server_thread():
            f = server.recv()
            received["ping"] = f
            server.send_pong()
            server._sock.close()

        ts = threading.Thread(target=server_thread)
        ts.start()

        client.send_ping()
        pong = client.recv()
        ts.join(timeout=3)
        client._sock.close()

        assert received["ping"].frame_type == FrameType.PING
        assert pong.frame_type == FrameType.PONG


# ═══════════════════════════════════════════════════════════════════════
# Thread safety: concurrent senders on one session
# ═══════════════════════════════════════════════════════════════════════

class TestSessionThreadSafety:
    def test_concurrent_senders_no_interleaving(self):
        """
        10 threads each send 50 frames concurrently on the same session.
        The receiver must get exactly 500 intact frames (no corruption).
        """
        client, server = make_session_pair(authenticated=True)
        N_THREADS = 10
        N_FRAMES = 50
        total = N_THREADS * N_FRAMES

        received = []
        done = threading.Event()

        def server_thread():
            while len(received) < total:
                try:
                    f = server.recv()
                    received.append(f)
                except (FrameError, ConnectionClosed, OSError):
                    break
            done.set()
            server._sock.close()

        ts = threading.Thread(target=server_thread)
        ts.start()

        barrier = threading.Barrier(N_THREADS)
        errors = []

        def sender(tid):
            barrier.wait()
            for seq in range(N_FRAMES):
                try:
                    client.send_data(tid, sequence=seq, chunk=b"x" * 64)
                except Exception as e:
                    errors.append(e)

        threads = [
            threading.Thread(target=sender, args=(fake_transfer_id(),))
            for _ in range(N_THREADS)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        done.wait(timeout=5)
        client._sock.close()
        ts.join(timeout=3)

        assert not errors, f"Sender errors: {errors}"
        assert len(received) == total, (
            f"Expected {total} frames, got {len(received)}"
        )
        # Every received frame must be a valid TRANSFER_DATA frame
        for f in received:
            assert f.frame_type == FrameType.TRANSFER_DATA
            assert f.payload == b"x" * 64
