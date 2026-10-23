"""
Tests for transfer model (transfer.py) and transfer manager (manager.py).

No network code is exercised here — these tests prove that the state
machine and the manager's lifecycle / scheduling logic are correct in
isolation before any socket code touches them.

Coverage:
  Transfer
    - Initial state is QUEUED
    - All legal transitions succeed
    - Illegal transitions raise TransferStateError
    - Terminal states cannot be left
    - progress / is_terminal / is_running derived properties
    - update_progress updates offset and total_size
    - to_dict() returns JSON-safe snapshot
    - Timestamps recorded correctly

  TransferManager
    - create_transfer returns QUEUED transfer
    - Scheduler auto-starts up to max_concurrent
    - QUEUED transfers wait when slots are full
    - Slot freed when a running transfer completes → queued promoted
    - Slot freed when a running transfer is cancelled
    - Slot freed when a running transfer is paused
    - pause / confirm_paused / resume round-trip
    - cancel_transfer from QUEUED, ACTIVE, PAUSING, PAUSED
    - fail_transfer records error message
    - retry_transfer: FAILED → RETRYING → ACTIVE
    - complete_transfer records checksum
    - cancel_all cancels every non-terminal transfer
    - State-change listener is called correctly
    - get() raises TransferNotFound for unknown ID
    - Illegal transitions propagate out of manager methods
    - to_dict() snapshot
    - Thread safety: concurrent creates don't exceed max_concurrent
"""

import threading
import time

import pytest

from filetransfer.transfer import (
    Transfer,
    TransferDirection,
    TransferState,
    TransferStateError,
    TERMINAL_STATES,
    RUNNING_STATES,
)
from filetransfer.manager import TransferManager, TransferNotFound


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def make_transfer(**kwargs) -> Transfer:
    defaults = dict(
        direction=TransferDirection.UPLOAD,
        filename="test.bin",
        total_size=1_000_000,
    )
    defaults.update(kwargs)
    return Transfer(**defaults)


def make_manager(max_concurrent: int = 3) -> TransferManager:
    return TransferManager(max_concurrent=max_concurrent)


def _wait_for_state(transfer: Transfer, state: TransferState, timeout: float = 1.0) -> bool:
    """Spin-wait until transfer reaches `state` or timeout expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if transfer.state == state:
            return True
        time.sleep(0.005)
    return False


# ═══════════════════════════════════════════════════════════════════════
# transfer model — Transfer model
# ═══════════════════════════════════════════════════════════════════════

class TestTransferInitialState:
    def test_initial_state_is_queued(self):
        t = make_transfer()
        assert t.state == TransferState.QUEUED

    def test_id_is_hex_string(self):
        t = make_transfer()
        assert isinstance(t.id, str)
        assert len(t.id) == 32  # uuid4().hex

    def test_two_transfers_have_different_ids(self):
        assert make_transfer().id != make_transfer().id

    def test_direction_stored(self):
        t = make_transfer(direction=TransferDirection.DOWNLOAD)
        assert t.direction == TransferDirection.DOWNLOAD

    def test_filename_stored(self):
        t = make_transfer(filename="movie.mp4")
        assert t.filename == "movie.mp4"

    def test_total_size_stored(self):
        t = make_transfer(total_size=500)
        assert t.total_size == 500

    def test_offset_starts_at_zero(self):
        assert make_transfer().offset == 0

    def test_error_starts_none(self):
        assert make_transfer().error is None

    def test_timestamps_on_creation(self):
        t = make_transfer()
        assert t.created_at > 0
        assert t.started_at is None
        assert t.ended_at is None


class TestTransferDerivedProperties:
    def test_progress_zero_when_no_bytes_transferred(self):
        t = make_transfer(total_size=1000)
        assert t.progress == 0.0

    def test_progress_half(self):
        t = make_transfer(total_size=1000)
        t.update_progress(500)
        assert t.progress == 0.5

    def test_progress_full(self):
        t = make_transfer(total_size=1000)
        t.update_progress(1000)
        assert t.progress == 1.0

    def test_progress_capped_at_one(self):
        t = make_transfer(total_size=1000)
        t.update_progress(1500)  # over-reporting shouldn't exceed 1.0
        assert t.progress == 1.0

    def test_progress_zero_when_total_unknown(self):
        t = make_transfer(total_size=0)
        assert t.progress == 0.0

    def test_is_terminal_false_for_queued(self):
        assert not make_transfer().is_terminal

    def test_is_terminal_true_for_terminal_states(self):
        for state in TERMINAL_STATES:
            t = make_transfer()
            # Drive to terminal state via legal path
            if state == TransferState.COMPLETED:
                t.set_state(TransferState.ACTIVE)
                t.set_state(TransferState.COMPLETED)
            elif state == TransferState.CANCELLED:
                t.set_state(TransferState.CANCELLED)
            elif state == TransferState.FAILED:
                t.set_state(TransferState.ACTIVE)
                t.set_state(TransferState.FAILED)
            assert t.is_terminal, f"expected is_terminal for {state}"

    def test_is_running_true_for_active(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        assert t.is_running

    def test_is_running_true_for_pausing(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        t.set_state(TransferState.PAUSING)
        assert t.is_running

    def test_is_running_false_for_paused(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        t.set_state(TransferState.PAUSING)
        t.set_state(TransferState.PAUSED)
        assert not t.is_running

    def test_is_running_false_for_queued(self):
        assert not make_transfer().is_running


class TestTransferLegalTransitions:
    def test_queued_to_active(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        assert t.state == TransferState.ACTIVE

    def test_queued_to_cancelled(self):
        t = make_transfer()
        t.set_state(TransferState.CANCELLED)
        assert t.state == TransferState.CANCELLED

    def test_active_to_pausing(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        t.set_state(TransferState.PAUSING)
        assert t.state == TransferState.PAUSING

    def test_pausing_to_paused(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        t.set_state(TransferState.PAUSING)
        t.set_state(TransferState.PAUSED)
        assert t.state == TransferState.PAUSED

    def test_paused_to_active(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        t.set_state(TransferState.PAUSING)
        t.set_state(TransferState.PAUSED)
        t.set_state(TransferState.ACTIVE)
        assert t.state == TransferState.ACTIVE

    def test_active_to_failed(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        t.set_state(TransferState.FAILED, error="disk full")
        assert t.state == TransferState.FAILED
        assert t.error == "disk full"

    def test_failed_to_retrying(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        t.set_state(TransferState.FAILED)
        t.set_state(TransferState.RETRYING)
        assert t.state == TransferState.RETRYING

    def test_retrying_to_active(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        t.set_state(TransferState.FAILED)
        t.set_state(TransferState.RETRYING)
        t.set_state(TransferState.ACTIVE)
        assert t.state == TransferState.ACTIVE

    def test_active_to_completed(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        t.set_state(TransferState.COMPLETED)
        assert t.state == TransferState.COMPLETED

    def test_pausing_to_completed(self):
        # A pause() sets PAUSING immediately, but the engine only
        # actually honors it at its next between-chunk safe point. If
        # the chunk already in flight when pause() was called turns
        # out to be the last one, the transfer legitimately finishes
        # instead of ever reaching PAUSED — that must complete
        # successfully, not raise.
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        t.set_state(TransferState.PAUSING)
        t.set_state(TransferState.COMPLETED)
        assert t.state == TransferState.COMPLETED

    def test_paused_to_completed(self):
        # Mirrors test_pausing_to_completed: a resume attempt can race
        # TRANSFER_COMPLETE arriving from the peer while still waiting
        # for confirmation (PAUSED -> ACTIVE only happens once that
        # confirmation lands — see engine.py). The transfer is done at
        # that point regardless of whether the resume ever confirmed.
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        t.set_state(TransferState.PAUSING)
        t.set_state(TransferState.PAUSED)
        t.set_state(TransferState.COMPLETED)
        assert t.state == TransferState.COMPLETED

    def test_same_state_is_idempotent(self):
        t = make_transfer()
        t.set_state(TransferState.QUEUED)  # already QUEUED, no-op
        assert t.state == TransferState.QUEUED


class TestTransferIllegalTransitions:
    def _illegal(self, from_state_path, target):
        t = make_transfer()
        for s in from_state_path:
            t.set_state(s)
        with pytest.raises(TransferStateError):
            t.set_state(target)

    def test_queued_cannot_go_to_retrying(self):
        self._illegal([], TransferState.RETRYING)

    def test_queued_cannot_go_to_completed(self):
        self._illegal([], TransferState.COMPLETED)

    def test_queued_cannot_go_to_failed(self):
        self._illegal([], TransferState.FAILED)

    def test_active_cannot_go_to_queued(self):
        self._illegal([TransferState.ACTIVE], TransferState.QUEUED)

    def test_active_cannot_go_to_retrying(self):
        self._illegal([TransferState.ACTIVE], TransferState.RETRYING)

    def test_paused_cannot_go_to_retrying(self):
        # PAUSED -> COMPLETED is now legal (see TestTransferLegalTransitions.
        # test_paused_to_completed) — this checks a transition that's
        # still illegal from PAUSED, so PAUSED doesn't become a
        # transition source with no illegal targets left to test.
        self._illegal(
            [TransferState.ACTIVE, TransferState.PAUSING, TransferState.PAUSED],
            TransferState.RETRYING,
        )

    def test_completed_is_terminal(self):
        self._illegal(
            [TransferState.ACTIVE, TransferState.COMPLETED],
            TransferState.QUEUED,
        )

    def test_cancelled_is_terminal(self):
        self._illegal([TransferState.CANCELLED], TransferState.ACTIVE)

    def test_failed_cannot_go_to_completed_directly(self):
        self._illegal([TransferState.ACTIVE, TransferState.FAILED], TransferState.COMPLETED)


class TestTransferTimestamps:
    def test_started_at_set_on_first_active(self):
        t = make_transfer()
        assert t.started_at is None
        t.set_state(TransferState.ACTIVE)
        assert t.started_at is not None

    def test_started_at_not_overwritten_on_resume(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        first_start = t.started_at
        t.set_state(TransferState.PAUSING)
        t.set_state(TransferState.PAUSED)
        t.set_state(TransferState.ACTIVE)
        assert t.started_at == first_start  # unchanged on re-activation

    def test_ended_at_set_on_completed(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        assert t.ended_at is None
        t.set_state(TransferState.COMPLETED)
        assert t.ended_at is not None

    def test_ended_at_set_on_cancelled(self):
        t = make_transfer()
        t.set_state(TransferState.CANCELLED)
        assert t.ended_at is not None

    def test_ended_at_set_on_failed(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        t.set_state(TransferState.FAILED)
        assert t.ended_at is not None


class TestTransferUpdateProgress:
    def test_update_offset(self):
        t = make_transfer(total_size=1000)
        t.update_progress(400)
        assert t.offset == 400

    def test_update_total_size(self):
        t = make_transfer(total_size=0)
        t.update_progress(0, total_size=1000)
        assert t.total_size == 1000

    def test_update_both(self):
        t = make_transfer(total_size=0)
        t.update_progress(250, total_size=1000)
        assert t.offset == 250
        assert t.total_size == 1000

    def test_zero_total_size_not_overwritten(self):
        t = make_transfer(total_size=1000)
        t.update_progress(100, total_size=0)  # 0 is ignored
        assert t.total_size == 1000


class TestTransferToDict:
    def test_to_dict_keys(self):
        t = make_transfer(filename="video.mp4", total_size=500)
        d = t.to_dict()
        for key in ("id", "direction", "filename", "total_size", "offset",
                    "progress", "state", "error", "checksum",
                    "created_at", "started_at", "ended_at", "elapsed_sec"):
            assert key in d, f"missing key: {key}"

    def test_to_dict_state_is_string(self):
        t = make_transfer()
        assert t.to_dict()["state"] == "queued"

    def test_to_dict_direction_is_string(self):
        t = make_transfer(direction=TransferDirection.DOWNLOAD)
        assert t.to_dict()["direction"] == "download"

    def test_to_dict_elapsed_after_start(self):
        t = make_transfer()
        t.set_state(TransferState.ACTIVE)
        time.sleep(0.05)
        d = t.to_dict()
        assert d["elapsed_sec"] is not None
        assert d["elapsed_sec"] >= 0.0

    def test_to_dict_elapsed_none_before_start(self):
        t = make_transfer()
        assert t.to_dict()["elapsed_sec"] is None


class TestTransferOnStateChangeCallback:
    def test_callback_called_on_transition(self):
        calls = []
        t = make_transfer()
        t._on_state_change = lambda tr, old, new: calls.append((old, new))
        t.set_state(TransferState.ACTIVE)
        assert calls == [(TransferState.QUEUED, TransferState.ACTIVE)]

    def test_callback_not_called_for_same_state(self):
        calls = []
        t = make_transfer()
        t._on_state_change = lambda tr, old, new: calls.append((old, new))
        t.set_state(TransferState.QUEUED)  # idempotent no-op
        assert calls == []


# ═══════════════════════════════════════════════════════════════════════
# transfer manager — TransferManager
# ═══════════════════════════════════════════════════════════════════════

class TestManagerCreation:
    def test_create_returns_transfer(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin", total_size=100)
        assert isinstance(t, Transfer)

    def test_created_transfer_registered(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        assert mgr.get(t.id) is t

    def test_create_with_checksum(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin", checksum="abc123")
        assert t.checksum == "abc123"

    def test_invalid_max_concurrent(self):
        with pytest.raises(ValueError):
            TransferManager(max_concurrent=0)


class TestManagerScheduler:
    def test_single_transfer_starts_immediately(self):
        mgr = make_manager(max_concurrent=1)
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        assert _wait_for_state(t, TransferState.ACTIVE), "should auto-start"

    def test_second_transfer_queued_when_slot_full(self):
        mgr = make_manager(max_concurrent=1)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t1, TransferState.ACTIVE)

        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        time.sleep(0.05)  # give scheduler a moment
        assert t2.state == TransferState.QUEUED

    def test_queued_starts_when_slot_freed_by_completion(self):
        mgr = make_manager(max_concurrent=1)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t1, TransferState.ACTIVE)

        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        time.sleep(0.05)
        assert t2.state == TransferState.QUEUED

        mgr.complete_transfer(t1.id)
        assert _wait_for_state(t2, TransferState.ACTIVE), "t2 should start after t1 completes"

    def test_queued_starts_when_slot_freed_by_cancel(self):
        mgr = make_manager(max_concurrent=1)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t1, TransferState.ACTIVE)

        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        time.sleep(0.05)
        assert t2.state == TransferState.QUEUED

        mgr.cancel_transfer(t1.id)
        assert _wait_for_state(t2, TransferState.ACTIVE), "t2 should start after t1 is cancelled"

    def test_queued_starts_when_slot_freed_by_pause(self):
        mgr = make_manager(max_concurrent=1)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t1, TransferState.ACTIVE)

        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        time.sleep(0.05)
        assert t2.state == TransferState.QUEUED

        mgr.pause_transfer(t1.id)
        mgr.confirm_paused(t1.id)  # simulate network layer confirming
        assert _wait_for_state(t2, TransferState.ACTIVE), "t2 should start after t1 is paused"

    def test_max_concurrent_three(self):
        mgr = make_manager(max_concurrent=3)
        transfers = [
            mgr.create_transfer(TransferDirection.UPLOAD, f"{i}.bin")
            for i in range(5)
        ]
        time.sleep(0.1)

        active = [t for t in transfers if t.state == TransferState.ACTIVE]
        queued = [t for t in transfers if t.state == TransferState.QUEUED]
        assert len(active) == 3
        assert len(queued) == 2

    def test_scheduling_order_is_fifo(self):
        mgr = make_manager(max_concurrent=1)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "first.bin")
        _wait_for_state(t1, TransferState.ACTIVE)

        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "second.bin")
        t3 = mgr.create_transfer(TransferDirection.UPLOAD, "third.bin")
        time.sleep(0.05)

        mgr.complete_transfer(t1.id)
        assert _wait_for_state(t2, TransferState.ACTIVE), "t2 should start before t3"
        assert t3.state == TransferState.QUEUED


class TestManagerPauseResume:
    def test_pause_resume_cycle(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)

        mgr.pause_transfer(t.id)
        assert t.state == TransferState.PAUSING

        mgr.confirm_paused(t.id)
        assert t.state == TransferState.PAUSED

        mgr.resume_transfer(t.id)
        assert t.state == TransferState.ACTIVE

    def test_pause_queued_transfer_holds_it_without_starting(self):
        mgr = make_manager(max_concurrent=1)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t1, TransferState.ACTIVE)

        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        time.sleep(0.05)
        assert t2.state == TransferState.QUEUED

        mgr.pause_transfer(t2.id)
        assert t2.state == TransferState.PAUSED
        assert t2.started_at is None

    def test_confirm_paused_requires_pausing_state(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)
        # Haven't called pause_transfer first
        with pytest.raises(TransferStateError):
            mgr.confirm_paused(t.id)


class TestManagerCancel:
    def test_cancel_queued(self):
        mgr = make_manager(max_concurrent=1)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t1, TransferState.ACTIVE)
        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        time.sleep(0.05)
        assert t2.state == TransferState.QUEUED

        mgr.cancel_transfer(t2.id)
        assert t2.state == TransferState.CANCELLED

    def test_cancel_active(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)
        mgr.cancel_transfer(t.id)
        assert t.state == TransferState.CANCELLED

    def test_cancel_pausing(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)
        mgr.pause_transfer(t.id)
        mgr.cancel_transfer(t.id)
        assert t.state == TransferState.CANCELLED

    def test_cancel_paused(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)
        mgr.pause_transfer(t.id)
        mgr.confirm_paused(t.id)
        mgr.cancel_transfer(t.id)
        assert t.state == TransferState.CANCELLED

    def test_cancel_terminal_is_noop(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)
        mgr.complete_transfer(t.id)
        mgr.cancel_transfer(t.id)  # should not raise
        assert t.state == TransferState.COMPLETED  # unchanged

    def test_cancel_does_not_affect_other_transfers(self):
        mgr = make_manager(max_concurrent=3)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        _wait_for_state(t1, TransferState.ACTIVE)
        _wait_for_state(t2, TransferState.ACTIVE)

        mgr.cancel_transfer(t1.id)
        assert t1.state == TransferState.CANCELLED
        assert t2.state == TransferState.ACTIVE  # unaffected


class TestManagerFail:
    def test_fail_records_error(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)
        mgr.fail_transfer(t.id, error="checksum mismatch")
        assert t.state == TransferState.FAILED
        assert t.error == "checksum mismatch"

    def test_fail_terminal_is_noop(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)
        mgr.complete_transfer(t.id)
        mgr.fail_transfer(t.id, "late error")  # should not raise
        assert t.state == TransferState.COMPLETED

    def test_fail_does_not_affect_other_transfers(self):
        mgr = make_manager(max_concurrent=3)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        _wait_for_state(t1, TransferState.ACTIVE)
        _wait_for_state(t2, TransferState.ACTIVE)

        mgr.fail_transfer(t1.id, "oops")
        assert t1.state == TransferState.FAILED
        assert t2.state == TransferState.ACTIVE


class TestManagerRetry:
    def test_retry_moves_to_retrying_then_active(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)
        mgr.fail_transfer(t.id, "network error")
        mgr.retry_transfer(t.id)
        assert _wait_for_state(t, TransferState.ACTIVE), "should reach ACTIVE after retry"

    def test_retry_resets_offset(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin", total_size=1000)
        _wait_for_state(t, TransferState.ACTIVE)
        t.update_progress(500)
        mgr.fail_transfer(t.id, "dropped")
        mgr.retry_transfer(t.id)
        assert t.offset == 0

    def test_retry_only_from_failed(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)
        with pytest.raises(TransferStateError):
            mgr.retry_transfer(t.id)  # ACTIVE is not FAILED


class TestManagerComplete:
    def test_complete_sets_state(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)
        mgr.complete_transfer(t.id)
        assert t.state == TransferState.COMPLETED

    def test_complete_records_checksum(self):
        mgr = make_manager()
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)
        mgr.complete_transfer(t.id, checksum="deadbeef")
        assert t.checksum == "deadbeef"


class TestManagerCancelAll:
    def test_cancel_all_returns_count(self):
        mgr = make_manager(max_concurrent=3)
        for i in range(3):
            t = mgr.create_transfer(TransferDirection.UPLOAD, f"{i}.bin")
            _wait_for_state(t, TransferState.ACTIVE)
        n = mgr.cancel_all()
        assert n == 3

    def test_cancel_all_skips_terminal(self):
        mgr = make_manager(max_concurrent=3)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        _wait_for_state(t1, TransferState.ACTIVE)
        _wait_for_state(t2, TransferState.ACTIVE)
        mgr.complete_transfer(t1.id)

        n = mgr.cancel_all()
        assert n == 1  # only t2
        assert t1.state == TransferState.COMPLETED  # unchanged
        assert t2.state == TransferState.CANCELLED


class TestManagerQuery:
    def test_get_unknown_id_raises(self):
        mgr = make_manager()
        with pytest.raises(TransferNotFound):
            mgr.get("nonexistent")

    def test_all_transfers_empty_initially(self):
        assert make_manager().all_transfers() == []

    def test_all_transfers_returns_all(self):
        mgr = make_manager(max_concurrent=10)
        ids = {mgr.create_transfer(TransferDirection.UPLOAD, f"{i}.bin").id for i in range(5)}
        assert {t.id for t in mgr.all_transfers()} == ids

    def test_active_transfers(self):
        mgr = make_manager(max_concurrent=2)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        t3 = mgr.create_transfer(TransferDirection.UPLOAD, "c.bin")  # queued
        time.sleep(0.05)
        active_ids = {t.id for t in mgr.active_transfers()}
        assert t1.id in active_ids
        assert t2.id in active_ids
        assert t3.id not in active_ids

    def test_queued_transfers(self):
        mgr = make_manager(max_concurrent=1)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t1, TransferState.ACTIVE)
        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        time.sleep(0.05)
        queued = mgr.queued_transfers()
        assert len(queued) == 1
        assert queued[0].id == t2.id

    def test_running_count(self):
        mgr = make_manager(max_concurrent=3)
        for i in range(3):
            mgr.create_transfer(TransferDirection.UPLOAD, f"{i}.bin")
        time.sleep(0.1)
        assert mgr.running_count() == 3

    def test_len(self):
        mgr = make_manager()
        assert len(mgr) == 0
        mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        assert len(mgr) == 2

    def test_iter(self):
        mgr = make_manager()
        mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        assert len(list(mgr)) == 2


class TestManagerListener:
    def test_listener_called_on_state_change(self):
        events = []
        mgr = make_manager()
        mgr.add_listener(lambda t, old, new: events.append((old.value, new.value)))

        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)

        # Give listener thread a moment to append
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            if any(e[1] == "active" for e in events):
                break
            time.sleep(0.01)

        assert any(e[1] == "active" for e in events)

    def test_listener_removed(self):
        events = []
        mgr = make_manager()

        def fn(t, old, new):
            events.append(new.value)

        mgr.add_listener(fn)
        mgr.remove_listener(fn)

        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)
        time.sleep(0.05)
        assert events == []  # listener was removed

    def test_failing_listener_doesnt_crash_manager(self):
        def bad_listener(t, old, new):
            raise RuntimeError("boom")

        mgr = make_manager()
        mgr.add_listener(bad_listener)
        # Should not raise
        t = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        _wait_for_state(t, TransferState.ACTIVE)


class TestManagerToDict:
    def test_to_dict_keys(self):
        mgr = make_manager()
        d = mgr.to_dict()
        for key in ("max_concurrent", "running", "queued", "completed",
                    "failed", "cancelled", "transfers"):
            assert key in d

    def test_to_dict_counts(self):
        mgr = make_manager(max_concurrent=2)
        t1 = mgr.create_transfer(TransferDirection.UPLOAD, "a.bin")
        t2 = mgr.create_transfer(TransferDirection.UPLOAD, "b.bin")
        t3 = mgr.create_transfer(TransferDirection.UPLOAD, "c.bin")
        _wait_for_state(t1, TransferState.ACTIVE)
        _wait_for_state(t2, TransferState.ACTIVE)
        time.sleep(0.05)
        d = mgr.to_dict()
        assert d["running"] == 2
        assert d["queued"] == 1


class TestManagerThreadSafety:
    def test_concurrent_creates_dont_exceed_max_concurrent(self):
        """
        50 threads each create a transfer simultaneously.
        At no point should more than max_concurrent be ACTIVE.
        """
        mgr = make_manager(max_concurrent=5)
        barrier = threading.Barrier(50)
        transfers = []
        lock = threading.Lock()

        def worker():
            barrier.wait()
            t = mgr.create_transfer(TransferDirection.UPLOAD, "x.bin")
            with lock:
                transfers.append(t)

        threads = [threading.Thread(target=worker) for _ in range(50)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=5)

        time.sleep(0.2)
        active = sum(1 for t in transfers if t.state == TransferState.ACTIVE)
        assert active <= 5, f"Expected ≤5 active, got {active}"
        assert active + sum(1 for t in transfers if t.state == TransferState.QUEUED) == 50
