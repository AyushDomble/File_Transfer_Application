"""
flow control: Flow control.

Stop-and-wait ACK (acknowledgements) already bounds any *single* transfer's own
outstanding bytes to one chunk — a SendEngine never sends chunk N+1
until chunk N is acknowledged. What it does NOT bound is the sum
across every transfer concurrently ACTIVE on one session: scheduling's
`max_concurrent` caps how many transfers may run at once, not how many
bytes they may collectively have in flight. With a large chunk_size
and a generous max_concurrent, a separate byte budget is required
to bound aggregate outstanding data.

GlobalWindow closes that gap with one shared session budget. Each
SendEngine also has an independent TransferWindow, so a large transfer
cannot reserve more than its own allowance. Engines reserve both budgets
and release them on acknowledgement, retry, cancellation or failure.

WINDOW_UPDATE (transport.py) lets the receiver change the budget at
any time, globally or for one transfer. Scoped windows are negotiated;
receivers advertise initial limits and refresh them after verified chunks.
Shrinking preserves outstanding debt; negotiated senders adapt chunk size
to smaller positive limits without waiting forever for an oversized grant.
"""

from __future__ import annotations

import threading
import time
from typing import Optional

DEFAULT_WINDOW_BYTES = 8 * 1024 * 1024   # 8 MB — a handful of default-size chunks
DEFAULT_TRANSFER_WINDOW_BYTES = 2 * 1024 * 1024
MAX_WINDOW_BYTES = 64 * 1024 * 1024


def validate_capacity(value):
    if type(value) is not int or not 1 <= value <= MAX_WINDOW_BYTES:
        raise ValueError(f'window capacity must be an integer in 1..{MAX_WINDOW_BYTES}')
    return value


class GlobalWindow:
    """
    A shared byte budget. acquire(n) blocks until n bytes are
    available (or the given cancel_event fires, or timeout elapses),
    then reserves them. release(n) returns them. resize() changes the
    total capacity — growing it immediately frees the difference;
    shrinking it just stops new acquires from succeeding until enough
    outstanding bytes have been released to bring availability back
    under the new, smaller capacity (existing reservations are never
    revoked — there is no way to "unsend" a chunk already on the
    wire).
    """

    def __init__(self, capacity: int = DEFAULT_WINDOW_BYTES) -> None:
        self._capacity = validate_capacity(capacity)
        self._outstanding = 0
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)

    @property
    def capacity(self) -> int:
        with self._lock:
            return self._capacity

    @property
    def available(self) -> int:
        with self._lock:
            return max(0, self._capacity - self._outstanding)

    @property
    def outstanding(self) -> int:
        with self._lock:
            return self._outstanding

    def acquire(
        self,
        nbytes: int,
        cancel_event: Optional[threading.Event] = None,
        pause_flag: Optional[threading.Event] = None,
        poll_interval: float = 0.1,
        timeout: Optional[float] = None,
    ) -> bool:
        """
        Block until `nbytes` of budget are available, then reserve
        them. Returns True once reserved. Returns False without
        reserving anything if `cancel_event` or `pause_flag` becomes
        set first, or `timeout` elapses first — callers pass their own
        cancel_event/pause_flag so a transfer blocked here (e.g.
        behind a much larger one that's slow to free budget, or a
        WINDOW_UPDATE that temporarily shrank capacity below this
        chunk's size) still responds to cancel/pause promptly instead
        of sitting stuck indefinitely.

        Deliberately does NOT raise or refuse a request larger than
        the *current* capacity — WINDOW_UPDATE is meant to allow
        exactly that, temporarily (a receiver under memory pressure
        clamping the budget below one chunk's size, meaning to raise
        it again shortly): such a request should just wait for either
        capacity to grow back or timeout/cancel/pause, not fail the
        whole transfer over what may be a momentary state. Without a
        timeout and with capacity never rising again, this legitimately
        blocks forever — pass cancel_event/pause_flag (or a timeout)
        if the caller needs a way out of that.
        """
        if type(nbytes) is not int or nbytes < 0:
            raise ValueError('reservation size must be a nonnegative integer')
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cv:
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    return False
                if pause_flag is not None and pause_flag.is_set():
                    return False
                if self._outstanding + nbytes <= self._capacity:
                    self._outstanding += nbytes
                    return True
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._cv.wait(timeout=min(poll_interval, remaining))
                else:
                    self._cv.wait(timeout=poll_interval)

    def release(self, nbytes: int) -> None:
        """Return previously acquired budget. Never over-fills past
        capacity — a resize() that shrank capacity while bytes were
        outstanding is respected rather than silently exceeded."""
        if nbytes <= 0:
            return
        with self._cv:
            # Keep debt when capacity shrinks below outstanding bytes. Clamping
            # available to zero loses that debt and admits too many new bytes.
            self._outstanding = max(0, self._outstanding - nbytes)
            self._cv.notify_all()

    def resize(self, new_capacity: int) -> None:
        """Change the total budget — e.g. on an inbound WINDOW_UPDATE.
        Growing it wakes any blocked acquire() immediately; shrinking
        it takes effect gradually as outstanding bytes are released,
        since already-reserved bytes are never forcibly reclaimed."""
        validate_capacity(new_capacity)
        with self._cv:
            self._capacity = new_capacity
            self._cv.notify_all()


class TransferWindow(GlobalWindow):
    """Independent outstanding-byte budget for one transfer."""

    def __init__(self, capacity=DEFAULT_TRANSFER_WINDOW_BYTES):
        super().__init__(capacity)
