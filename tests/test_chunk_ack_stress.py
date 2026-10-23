"""
Stress tests for chunked data (chunked DATA frames) and acknowledgements (ACKs).

These push the chunk/ACK loop in engine.py harder than the baseline
correctness tests in test_scheduler.py: chunk-boundary edge cases,
many small chunks (hundreds of ACK round-trips per transfer), random
sizes with byte-exact comparison (not just checksum — a checksum
match can't rule out e.g. two swapped equal-length chunks), back-to-
back transfers reusing one session (state-leak check), and many
concurrent transfers of mixed sizes sharing one socket (transfer_id
cross-talk check).
"""

from __future__ import annotations

import hashlib
import os
import random
import socket
import threading
import time

import pytest

from filetransfer.engine import FrameRouter, teardown_session
from filetransfer.manager import TransferManager
from filetransfer.session_protocol import Session
from filetransfer.scheduler import ConcurrentScheduler
from filetransfer.transfer import TransferState


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            buf = f.read(65536)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def _write_random_file(path: str, size: int, seed: int = None) -> bytes:
    rnd = random.Random(seed)
    data = rnd.randbytes(size) if hasattr(rnd, "randbytes") else bytes(
        rnd.getrandbits(8) for _ in range(size)
    )
    with open(path, "wb") as f:
        f.write(data)
    return data


class Peer:
    def __init__(self, sock, name: str, dest_dir: str, max_concurrent: int = 3):
        self.session = Session(sock, ("127.0.0.1", 0), name=name)
        self.session.mark_authenticated()  # This fixture tests post-pairing transfer behavior.
        self.router = FrameRouter(self.session)
        self.manager = TransferManager(max_concurrent=max_concurrent)
        self.scheduler = ConcurrentScheduler(
            self.session, self.manager, self.router, dest_dir=dest_dir
        )

    def start(self) -> None:
        self.router.start_reader()

    def teardown(self) -> None:
        self.scheduler.stop_accept_loop()
        teardown_session(self.session, self.router)


def _make_peers(tmp_path, max_concurrent=3):
    a_sock, b_sock = socket.socketpair()
    client_dir = tmp_path / "client_files"
    server_dir = tmp_path / "server_files"
    client_dir.mkdir(exist_ok=True)
    server_dir.mkdir(exist_ok=True)

    client = Peer(a_sock, "client", str(client_dir), max_concurrent=max_concurrent)
    server = Peer(b_sock, "server", str(server_dir), max_concurrent=max_concurrent)

    results, errors = {}, {}

    def do_client():
        try:
            results["client"] = client.session.negotiate_as_client()
        except Exception as e:
            errors["client"] = e

    def do_server():
        try:
            results["server"] = server.session.negotiate_as_server()
        except Exception as e:
            errors["server"] = e

    tc = threading.Thread(target=do_client)
    ts = threading.Thread(target=do_server)
    tc.start(); ts.start()
    tc.join(timeout=3); ts.join(timeout=3)
    assert not errors, errors

    client.start()
    server.start()
    server.scheduler.start_accept_loop()
    return client, server


@pytest.fixture()
def peers(tmp_path):
    client, server = _make_peers(tmp_path)
    yield client, server
    client.teardown()
    server.teardown()


def _wait_for_state(manager: TransferManager, transfer_id: str,
                     states, timeout: float = 20.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        t = manager.get(transfer_id)
        last = t.state
        if t.state in states:
            return t
        time.sleep(0.01)
    raise AssertionError(
        f"transfer {transfer_id[:8]} never reached {states} (last={last})"
    )


def _upload_and_verify(client, server, src_path: str, src_data: bytes,
                        chunk_size: int, timeout: float = 20.0) -> None:
    transfer = client.scheduler.submit_upload(str(src_path), chunk_size=chunk_size)
    final = _wait_for_state(client.manager, transfer.id,
                             {TransferState.COMPLETED, TransferState.FAILED}, timeout)
    assert final.state == TransferState.COMPLETED, final.error

    dest = os.path.join(server.scheduler._dest_dir, os.path.basename(str(src_path)))
    assert os.path.isfile(dest)
    with open(dest, "rb") as f:
        received = f.read()
    assert received == src_data, (
        f"byte mismatch: sent {len(src_data)} bytes, received {len(received)} "
        f"(chunk_size={chunk_size})"
    )


# ═══════════════════════════════════════════════════════════════════════
# Chunk-boundary edge cases
# ═══════════════════════════════════════════════════════════════════════

CHUNK_SIZE = 4096

BOUNDARY_SIZES = [
    0,                      # empty file — zero DATA frames, straight to COMPLETE
    1,                      # smaller than one chunk
    CHUNK_SIZE - 1,
    CHUNK_SIZE,             # exactly one full chunk
    CHUNK_SIZE + 1,         # one full chunk + 1 byte
    2 * CHUNK_SIZE,         # exactly two full chunks
    2 * CHUNK_SIZE + 777,   # two full chunks + a remainder
    5 * CHUNK_SIZE - 1,     # just under N chunks
]


@pytest.mark.parametrize("size", BOUNDARY_SIZES, ids=[f"{s}b" for s in BOUNDARY_SIZES])
def test_chunk_boundary_sizes(peers, tmp_path, size):
    client, server = peers
    src = tmp_path / f"boundary_{size}.bin"
    data = _write_random_file(str(src), size, seed=size)
    _upload_and_verify(client, server, src, data, chunk_size=CHUNK_SIZE)


# ═══════════════════════════════════════════════════════════════════════
# Many small chunks — hundreds of ACK round-trips in one transfer
# ═══════════════════════════════════════════════════════════════════════

def test_many_small_chunks_high_round_trip_count(peers, tmp_path):
    client, server = peers
    tiny_chunk = 256
    size = 300 * tiny_chunk + 37  # ~301 chunks, non-aligned final chunk
    src = tmp_path / "many_chunks.bin"
    data = _write_random_file(str(src), size, seed=1)

    start = time.time()
    _upload_and_verify(client, server, src, data, chunk_size=tiny_chunk, timeout=30)
    elapsed = time.time() - start
    # Not a hard perf assertion — just a sanity check that stop-and-wait
    # ACK over ~300 round trips doesn't stall or blow past a sane bound
    # on a loopback socket.
    assert elapsed < 15, f"301 chunks took {elapsed:.1f}s — investigate a stall"


# ═══════════════════════════════════════════════════════════════════════
# Random sizes, byte-exact comparison (not just checksum)
# ═══════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("trial", range(8))
def test_random_sizes_byte_exact(peers, tmp_path, trial):
    client, server = peers
    rnd = random.Random(1000 + trial)
    size = rnd.randint(0, 6 * CHUNK_SIZE)
    src = tmp_path / f"random_{trial}.bin"
    data = _write_random_file(str(src), size, seed=2000 + trial)
    _upload_and_verify(client, server, src, data, chunk_size=CHUNK_SIZE)


# ═══════════════════════════════════════════════════════════════════════
# Back-to-back transfers reusing the same session — state-leak check
# ═══════════════════════════════════════════════════════════════════════

def test_sequential_transfers_reuse_session_no_state_leak(peers, tmp_path):
    client, server = peers
    for i in range(6):
        size = (i * 991) % (3 * CHUNK_SIZE)  # varied, including 0
        src = tmp_path / f"seq_{i}.bin"
        data = _write_random_file(str(src), size, seed=3000 + i)
        _upload_and_verify(client, server, src, data, chunk_size=CHUNK_SIZE)

    # If sequence numbers, router queues, or offsets leaked across
    # transfers, at least one of the 6 would have failed or corrupted
    # above. Also confirm the manager's bookkeeping matches: 6 attempts,
    # 6 completions, nothing stuck.
    completed = [t for t in client.manager.all_transfers() if t.state == TransferState.COMPLETED]
    assert len(completed) == 6


# ═══════════════════════════════════════════════════════════════════════
# Concurrent stress — many transfers, mixed sizes, one socket
# ═══════════════════════════════════════════════════════════════════════

def test_concurrent_stress_many_mixed_size_transfers(tmp_path):
    client, server = _make_peers(tmp_path, max_concurrent=4)
    try:
        rnd = random.Random(42)
        specs = []
        for i in range(14):
            size = rnd.choice([
                0, 1, 500,
                CHUNK_SIZE - 1, CHUNK_SIZE, CHUNK_SIZE + 500,
                3 * CHUNK_SIZE + rnd.randint(0, CHUNK_SIZE),
            ])
            specs.append((f"mix_{i}.bin", size))

        sources = {}
        for name, size in specs:
            path = tmp_path / name
            data = _write_random_file(str(path), size, seed=hash(name) & 0xFFFF)
            sources[name] = (str(path), data)

        transfers = {
            name: client.scheduler.submit_upload(path, chunk_size=CHUNK_SIZE)
            for name, (path, _data) in sources.items()
        }

        for name, transfer in transfers.items():
            final = _wait_for_state(client.manager, transfer.id,
                                     {TransferState.COMPLETED, TransferState.FAILED},
                                     timeout=30)
            assert final.state == TransferState.COMPLETED, f"{name}: {final.error}"

        # Byte-exact check per file — this is what would catch
        # transfer_id cross-talk (one transfer's chunks landing in
        # another's .part file) that a checksum-only check might miss
        # if two files happened to collide in size.
        for name, (path, data) in sources.items():
            dest = os.path.join(server.scheduler._dest_dir, name)
            with open(dest, "rb") as f:
                received = f.read()
            assert received == data, f"{name}: byte mismatch under concurrent load"
    finally:
        client.teardown()
        server.teardown()
