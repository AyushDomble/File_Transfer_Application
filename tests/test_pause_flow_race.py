"""
Regression: a pause that lands while the sender is between reading a
chunk and reserving flow-control budget for it.

SendEngine reads a chunk (advancing the file position past it) and only
then acquires its share of the session window. A pause arriving in that
gap abandoned the chunk but left the file position one chunk ahead of
`offset`; resuming at the same offset skipped the seek, so the next
chunk was read from the wrong place — one chunk silently dropped, later
chunks mislabeled, the last chunk never flagged FINAL, both sides hung.
The window is normally free so the gap is microseconds wide (a ~1-in-50
flake in the receiver-restart tests); here the window is smaller than a
chunk so the sender parks in the gap deterministically.
"""

from __future__ import annotations

import os
import time

from filetransfer.flowcontrol import GlobalWindow
from filetransfer.transfer import TransferState

from helpers.flow_peers import CHUNK_SIZE, _make_peers, _wait_for_state, _write_random_file


def test_pause_while_blocked_on_flow_window_does_not_skip_a_chunk(tmp_path):
    # Hold all credits as if another transfer owned them. A merely small
    # capacity no longer stalls scoped-flow senders: they subdivide chunks.
    window = GlobalWindow(capacity=CHUNK_SIZE)
    window.acquire(CHUNK_SIZE)
    client, server = _make_peers(tmp_path, client_window=window)
    try:
        size = 6 * CHUNK_SIZE + 123
        src = tmp_path / "blocked.bin"
        data = _write_random_file(str(src), size, seed=99)

        t = client.scheduler.submit_upload(str(src), chunk_size=CHUNK_SIZE)
        _wait_for_state(client.manager, t.id, {TransferState.ACTIVE})
        time.sleep(0.5)                       # sender is parked in acquire()
        assert t.offset == 0

        client.scheduler.pause(t.id)
        _wait_for_state(client.manager, t.id, {TransferState.PAUSED})
        assert t.offset == 0

        window.release(CHUNK_SIZE)
        window.resize(64 * CHUNK_SIZE)        # unblock, then resume
        client.scheduler.resume(t.id)

        final = _wait_for_state(
            client.manager, t.id,
            {TransferState.COMPLETED, TransferState.FAILED}, timeout=20)
        assert final.state == TransferState.COMPLETED, final.error
        got = os.path.join(str(tmp_path / "server_files"), "blocked.bin")
        assert open(got, "rb").read() == data       # byte-exact: nothing skipped
    finally:
        client.teardown()
        server.teardown()
