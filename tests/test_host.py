"""TransferHost and HostLink integration: pairing, policy, transfers and peer isolation."""

from __future__ import annotations

import os
import hashlib
import threading
import time

import pytest

from filetransfer.host import TransferHost
from filetransfer.hostlink import ConnectionLost, ConnectionRejected, HostLink
from filetransfer.transfer import TransferState

TERMINAL = {TransferState.COMPLETED, TransferState.FAILED, TransferState.CANCELLED}


def _wait_state(transfer, states=TERMINAL, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if transfer.state in states:
            return transfer
        time.sleep(0.01)
    raise AssertionError(f"transfer never reached {states} (last={transfer.state})")


def _auto_approve(host, accept=True, delay=0.0):
    def worker():
        deadline = time.time() + 5
        while time.time() < deadline:
            p = host.list_pending()
            if p:
                if delay:
                    time.sleep(delay)
                if accept:
                    host.accept_pending(p[0]["id"])
                else:
                    host.reject_pending(p[0]["id"])
                return
            time.sleep(0.01)
    threading.Thread(target=worker, daemon=True).start()


@pytest.fixture()
def host(tmp_path, unused_tcp_port_factory=None):
    import socket as _socket
    s = _socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    os.environ["FT_PORT"] = str(port)
    os.environ["FT_SERVER_DIR"] = str(tmp_path / "server_files")
    os.environ["FT_CLIENT_DIR"] = str(tmp_path / "client_files")
    os.environ["FT_PENDING_TIMEOUT"] = "3"
    os.environ["FT_PIN_EXPIRY_SECONDS"] = "0"
    import importlib
    from filetransfer import config as _config
    importlib.reload(_config)

    h = TransferHost(dest_dir=str(tmp_path / "server_files"))
    h.start()
    time.sleep(0.15)
    yield h, port
    h.stop()


def _connect(host, port, pin=None, accept=True, **kw):
    _auto_approve(host, accept=accept)
    link = HostLink("127.0.0.1", port=port, use_tls=True, fingerprint=host.certificate_fingerprint, **kw)
    link.connect_with_pin(pin or host.current_pin)
    peer_id = hashlib.sha256(link._recovery_key.encode()).hexdigest()
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        peer = host.get_peer(peer_id)
        if peer is not None and peer.session is not None and not peer.session.is_lost:
            break
        time.sleep(0.01)
    assert peer is not None and not peer.session.is_lost
    return link


# ═══════════════════════════════════════════════════════════════════════
# Pairing: PIN + pending approval
# ═══════════════════════════════════════════════════════════════════════

def test_connect_with_correct_pin_and_approval(host, tmp_path):
    h, port = host
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    assert link.connected
    link.close()


def test_connect_with_wrong_pin_is_rejected(host, tmp_path):
    h, port = host
    link = HostLink("127.0.0.1", port=port, use_tls=True, fingerprint=h.certificate_fingerprint,
                    dest_dir=str(tmp_path / "client_files"))
    with pytest.raises(ConnectionRejected):
        link.connect_with_pin("000000")


def test_correct_pin_is_invalidated_immediately_after_use(host, tmp_path):
    """Matches the old server: a PIN can't be reused, even by the same
    peer, once it's been consumed by one connection attempt."""
    h, port = host
    used_pin = h.current_pin
    _auto_approve(h)
    link = HostLink("127.0.0.1", port=port, use_tls=True, fingerprint=h.certificate_fingerprint,
                    dest_dir=str(tmp_path / "client_files"))
    link.connect_with_pin(used_pin)
    assert link.connected
    assert h.current_pin != used_pin
    link.close()


def test_host_rejection_of_pending_request(host, tmp_path):
    h, port = host
    link = HostLink("127.0.0.1", port=port, use_tls=True, fingerprint=h.certificate_fingerprint,
                    dest_dir=str(tmp_path / "client_files"))
    with pytest.raises(ConnectionRejected):
        _connect_and_expect_reject(h, link)


def _connect_and_expect_reject(h, link):
    _auto_approve(h, accept=False)
    link.connect_with_pin(h.current_pin)


def test_pending_approval_timeout_rejects_connection(tmp_path):
    import socket as _socket
    s = _socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    os.environ["FT_PORT"] = str(port)
    os.environ["FT_SERVER_DIR"] = str(tmp_path / "server_files")
    os.environ["FT_CLIENT_DIR"] = str(tmp_path / "client_files")
    os.environ["FT_PENDING_TIMEOUT"] = "1"
    os.environ["FT_PIN_EXPIRY_SECONDS"] = "0"
    import importlib
    from filetransfer import config as _config
    importlib.reload(_config)

    h = TransferHost(dest_dir=str(tmp_path / "server_files"))
    h.start()
    time.sleep(0.15)
    try:
        link = HostLink("127.0.0.1", port=port, use_tls=True, fingerprint=h.certificate_fingerprint,
                        dest_dir=str(tmp_path / "client_files"))
        with pytest.raises(ConnectionRejected):
            link.connect_with_pin(h.current_pin)   # nobody ever approves
    finally:
        h.stop()


def test_multiple_peers_are_isolated(host, tmp_path):
    h, port = host
    a = _connect(h, port, dest_dir=str(tmp_path / "a_files"))
    b = _connect(h, port, dest_dir=str(tmp_path / "b_files"))
    assert len(h.list_peers()) == 2

    src = tmp_path / "iso.bin"
    src.write_bytes(os.urandom(20_000))
    ta = a.scheduler.submit_upload(str(src))
    _wait_state(ta)
    assert ta.state == TransferState.COMPLETED

    # b never touched this transfer and has no way to see it.
    from filetransfer.manager import TransferNotFound
    with pytest.raises(TransferNotFound):
        b.manager.get(ta.id)

    a.close(); b.close()


# ═══════════════════════════════════════════════════════════════════════
# File listing / delete over the new control frames
# ═══════════════════════════════════════════════════════════════════════

def test_list_files_empty_then_after_upload(host, tmp_path):
    h, port = host
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    assert link.list_files() == []
    src = tmp_path / "doc.txt"
    src.write_text("hello")
    t = link.scheduler.submit_upload(str(src))
    _wait_state(t)
    assert t.state == TransferState.COMPLETED
    assert link.list_files() == ["doc.txt"]
    link.close()


def test_delete_removes_file_and_is_idempotent_false_after(host, tmp_path):
    h, port = host
    h.allow_remote_delete = True
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    src = tmp_path / "gone.txt"
    src.write_text("x")
    _wait_state(link.scheduler.submit_upload(str(src)))
    assert link.delete("gone.txt") is True
    assert link.list_files() == []
    assert link.delete("gone.txt") is False       # already gone
    link.close()


def test_delete_rejects_path_traversal(host, tmp_path):
    h, port = host
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    outside = tmp_path / "outside.txt"
    outside.write_text("do not delete me")
    assert link.delete("../outside.txt") is False
    assert outside.exists()
    link.close()


def test_control_requests_do_not_interfere_with_concurrent_transfer(host, tmp_path):
    """The control-frame correlation-id mechanism shares the wire with
    TRANSFER_* frames; a list/delete call mid-transfer must not
    disturb it."""
    h, port = host
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    src = tmp_path / "big.bin"
    src.write_bytes(os.urandom(3_000_000))
    t = link.scheduler.submit_upload(str(src), progress_cb=lambda o, tt: time.sleep(0.001))
    for _ in range(5):
        link.list_files()
    _wait_state(t, timeout=20)
    assert t.state == TransferState.COMPLETED
    link.close()


# ═══════════════════════════════════════════════════════════════════════
# Settings enforcement (max size / blocked extensions)
# ═══════════════════════════════════════════════════════════════════════

def test_blocked_extension_is_rejected(host, tmp_path):
    h, port = host
    h.update_settings(blocked_extensions=".exe,.bat")
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    src = tmp_path / "virus.exe"
    src.write_bytes(b"not really a virus")
    t = link.scheduler.submit_upload(str(src))
    final = _wait_state(t)
    assert final.state == TransferState.FAILED
    assert not os.path.exists(os.path.join(str(tmp_path / "server_files"), "virus.exe"))
    link.close()


def test_oversized_upload_is_rejected(host, tmp_path):
    h, port = host
    h.update_settings(max_upload_mb=0.001)     # ~1 KB
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    src = tmp_path / "toobig.bin"
    src.write_bytes(os.urandom(50_000))
    t = link.scheduler.submit_upload(str(src))
    final = _wait_state(t)
    assert final.state == TransferState.FAILED
    link.close()


def test_settings_apply_to_already_connected_peer(host, tmp_path):
    """update_settings() while a peer is connected affects that peer's
    NEXT transfer too, not just future connections."""
    h, port = host
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    src = tmp_path / "later.exe"
    src.write_bytes(b"x")
    h.update_settings(blocked_extensions=".exe")
    t = link.scheduler.submit_upload(str(src))
    final = _wait_state(t)
    assert final.state == TransferState.FAILED
    link.close()


def test_upload_conflict_without_overwrite_fails_cleanly(host, tmp_path):
    h, port = host
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    src = tmp_path / "dup.txt"
    src.write_text("first")
    _wait_state(link.scheduler.submit_upload(str(src)))
    t2 = link.scheduler.submit_upload(str(src))     # overwrite=False by default
    final = _wait_state(t2)
    assert final.state == TransferState.FAILED
    link.close()


def test_upload_conflict_with_overwrite_succeeds(host, tmp_path):
    h, port = host
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    src = tmp_path / "dup2.txt"
    src.write_text("first")
    _wait_state(link.scheduler.submit_upload(str(src)))
    src.write_text("second, longer content")
    t2 = link.scheduler.submit_upload(str(src), overwrite=True)
    final = _wait_state(t2)
    assert final.state == TransferState.COMPLETED
    dest = os.path.join(str(tmp_path / "server_files"), "dup2.txt")
    assert open(dest).read() == "second, longer content"
    link.close()


# ═══════════════════════════════════════════════════════════════════════
# Disconnect / cancel_all_and_close
# ═══════════════════════════════════════════════════════════════════════

def test_disconnect_retains_offline_peer_and_reconnect_reuses_manager(host, tmp_path):
    h, port = host
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    assert len(h.list_peers()) == 1
    peer_id = h.list_peers()[0]['peer_id']
    manager = h.get_peer(peer_id).manager
    link.close()
    time.sleep(0.3)
    assert len(h.list_peers()) == 1 and not h.list_peers()[0]['connected']
    _auto_approve(h)
    link.connect_with_pin(h.current_pin)
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and not h.list_peers()[0]['connected']:
        time.sleep(0.01)
    assert h.get_peer(peer_id).manager is manager
    assert h.list_peers()[0]['connected']
    link.close()


def test_cancel_all_and_close_interrupts_blocked_call(host, tmp_path):
    h, port = host
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    src = tmp_path / "cx.bin"
    src.write_bytes(os.urandom(5_000_000))
    t = link.scheduler.submit_upload(str(src), progress_cb=lambda o, tt: time.sleep(0.002))

    result = {}
    def blocked_call():
        try:
            result["files"] = link.list_files()
        except ConnectionLost as e:
            result["lost"] = str(e)

    th = threading.Thread(target=blocked_call)
    time.sleep(0.05)
    th.start()
    time.sleep(0.05)
    link.cancel_all_and_close()
    th.join(timeout=3)
    assert not th.is_alive()


# ═══════════════════════════════════════════════════════════════════════
# Transfer-event listener (transfer events hook)
# ═══════════════════════════════════════════════════════════════════════

def test_transfer_listener_fires_for_the_right_peer(host, tmp_path):
    h, port = host
    events = []
    h.add_transfer_listener(lambda pid, t, old, new: events.append((pid, t.id, new.value)))
    link = _connect(h, port, dest_dir=str(tmp_path / "client_files"))
    src = tmp_path / "ev.bin"
    src.write_bytes(os.urandom(5_000))
    t = link.scheduler.submit_upload(str(src))
    _wait_state(t)
    peer_id = h.list_peers()[0]["peer_id"]
    matching = [e for e in events if e[1] == t.id]
    assert matching, events
    assert all(e[0] == peer_id for e in matching)
    assert matching[-1][2] == "completed"
    link.close()
