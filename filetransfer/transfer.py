"""
transfer model: Transfer model.

Defines what a "transfer" is, independently of any network code, UI,
or API layer. Every other module in the redesign imports from here —
nothing invents its own interpretation of "cancelled" or "paused".

A Transfer has:
  - A unique ID (UUID4 hex string)
  - A direction (upload or download)
  - Metadata (filename, total size, current offset, checksum)
  - A state drawn from TransferState

The state machine is:

                    ┌──────────────────────────────────┐
                    │                                  │
  QUEUED ──► ACTIVE ──► PAUSING ──► PAUSED ──► ACTIVE │
                │                                      │
                ├──────────────────────────► CANCELLED │
                │                                      │
                ├──────────────────────────► FAILED    │
                │                                      │
                └──────────────────────────► COMPLETED │
                                                       │
  FAILED ──────────────────────────────────► RETRYING ─┘

  recovery: ACTIVE/PAUSING/RETRYING/PAUSED ──► CONNECTION_LOST ──► QUEUED
  (connection died; recoverable — re-queued once a new session attaches)

Only legal transitions are enforced by Transfer.set_state(). Illegal
ones raise TransferStateError so bugs surface immediately rather than
silently leaving a transfer in a contradictory state.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class TransferDirection(str, Enum):
    UPLOAD = "upload"
    DOWNLOAD = "download"


class TransferState(str, Enum):
    """
    Every transfer lives in exactly one of these states.

    String values are intentionally lowercase so they serialize
    directly to JSON / WebSocket payloads without a conversion step.
    """
    QUEUED     = "queued"       # created, waiting for a free slot
    ACTIVE     = "active"       # bytes are flowing
    PAUSING    = "pausing"      # pause requested, waiting for a safe checkpoint
    PAUSED     = "paused"       # deterministically stopped at a known offset
    CANCELLED  = "cancelled"    # explicitly cancelled by the user
    FAILED     = "failed"       # error; may be retried
    RETRYING   = "retrying"     # transient state while retry is being set up
    COMPLETED  = "completed"    # all bytes received and verified
    # recovery: the connection carrying this transfer died. Not a
    # failure and not terminal — the transfer keeps its offset and .part
    # file and is recoverable: once a new session is attached it is
    # re-queued and resumed from the receiver's confirmed offset.
    CONNECTION_LOST = "connection_lost"


# Adjacency list: which states each state may transition *into*.
# Keys are the current state; values are the set of legal next states.
_LEGAL_TRANSITIONS: dict[TransferState, set[TransferState]] = {
    TransferState.QUEUED:    {TransferState.ACTIVE, TransferState.PAUSED, TransferState.CANCELLED},
    TransferState.ACTIVE:    {TransferState.PAUSING, TransferState.CANCELLED,
                              TransferState.FAILED,  TransferState.COMPLETED,
                              TransferState.CONNECTION_LOST},
    TransferState.PAUSING:   {TransferState.PAUSED,    TransferState.CANCELLED,
                              TransferState.FAILED,    TransferState.COMPLETED,
                              TransferState.CONNECTION_LOST},
    # PAUSING -> COMPLETED: pause() sets PAUSING immediately, but the
    # engine only actually honors it at its next between-chunk safe
    # point (see engine.py's pause_flag checks). If that safe point
    # never comes because the chunk already in flight when pause()
    # was called turns out to be the last one, the engine legitimately
    # finishes the whole transfer instead. The data is complete and
    # correct at that point — reporting FAILED (the old behavior: this
    # transition was illegal, so complete_transfer() raised) would be
    # wrong. A pause that loses a race with the transfer's own natural
    # completion is a no-op, not a failure.
    TransferState.PAUSED:    {TransferState.ACTIVE,    TransferState.CANCELLED,
                              TransferState.FAILED,    TransferState.COMPLETED,
                              # A restart-restored PAUSED transfer whose
                              # peer link is gone (see persistence.
                              # restore_transfer(connection_lost=True)).
                              TransferState.CONNECTION_LOST},
    # PAUSED -> COMPLETED: mirrors PAUSING -> COMPLETED above. A resume
    # attempt can race TRANSFER_COMPLETE arriving from the peer while
    # still in _wait_for_resume (before PAUSED -> ACTIVE has happened —
    # see engine.py, where that transition now only happens on a
    # confirmed resuming ACK, not preemptively). The transfer is
    # legitimately finished at that point; there's no reason to force
    # it through ACTIVE first, and no reason to report FAILED.
    TransferState.CANCELLED: set(),   # terminal
    TransferState.FAILED:    {TransferState.RETRYING},
    TransferState.RETRYING:  {TransferState.ACTIVE,    TransferState.CANCELLED,
                              TransferState.FAILED,    TransferState.CONNECTION_LOST},
    # Recovery re-queues (the concurrency limit still applies) rather
    # than jumping straight to ACTIVE. A lost transfer can also be
    # cancelled, or given up on.
    TransferState.CONNECTION_LOST: {TransferState.QUEUED, TransferState.CANCELLED,
                                    TransferState.FAILED},
    TransferState.COMPLETED: set(),   # terminal
}

# States from which no further progress is possible.
TERMINAL_STATES: frozenset[TransferState] = frozenset({
    TransferState.CANCELLED,
    TransferState.FAILED,
    TransferState.COMPLETED,
})

# States that count toward the "active / running" concurrency slot.
RUNNING_STATES: frozenset[TransferState] = frozenset({
    TransferState.ACTIVE,
    TransferState.PAUSING,
})


class TransferStateError(Exception):
    """Raised when an illegal state transition is attempted."""


def _finished_event():
    event = threading.Event()
    event.set()
    return event


@dataclass
class Transfer:
    """
    The single source of truth for one file transfer.

    Thread-safety: all mutations go through set_state() / update_progress(),
    both of which hold _lock. Callers that need an atomic snapshot of
    multiple fields should hold the lock themselves:

        with transfer.lock:
            s = transfer.state
            o = transfer.offset
    """

    # ── identity ──────────────────────────────────────────────────────────
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    direction: TransferDirection = TransferDirection.UPLOAD

    # ── file metadata ─────────────────────────────────────────────────────
    filename: str = ""
    total_size: int = 0          # bytes; 0 if unknown at creation time
    offset: int = 0              # bytes confirmed received / sent so far
    checksum: Optional[str] = None   # SHA-256 of the complete file; None until known
    part_path: Optional[str] = None
    dest_path: Optional[str] = None
    overwrite: bool = True
    ready: bool = True  # inbound jobs cannot run until ACCEPT is on the wire
    resume_requested: bool = False  # PAUSED, waiting for scheduler admission
    attempt: int = 0
    revision: int = 0  # process-local monotonic UI snapshot version
    _event_generation: str = field(default_factory=lambda: uuid.uuid4().hex, repr=False, compare=False)
    _on_progress: Optional[Callable] = field(default=None, repr=False, compare=False)
    io_finished: threading.Event = field(default_factory=_finished_event, repr=False, compare=False)

    # ── state ─────────────────────────────────────────────────────────────
    state: TransferState = TransferState.QUEUED
    error: Optional[str] = None  # populated when state == FAILED

    # ── timestamps ────────────────────────────────────────────────────────
    created_at: float = field(default_factory=time.monotonic)
    started_at: Optional[float] = None
    ended_at: Optional[float] = None

    # ── concurrency primitives (not serialized) ───────────────────────────
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False, compare=False)

    # ── optional state-change hook ────────────────────────────────────────
    # Registered by TransferManager so it can react to every transition
    # without polling.  Called *inside* _lock — keep it fast.
    _on_state_change: Optional[Callable[["Transfer", TransferState, TransferState], None]] = (
        field(default=None, repr=False, compare=False)
    )

    # expose the lock so callers can do atomic multi-field reads
    @property
    def lock(self) -> threading.Lock:
        return self._lock

    # ── derived properties ────────────────────────────────────────────────

    @property
    def progress(self) -> float:
        """0.0–1.0 completion fraction; 0.0 if total_size is unknown."""
        if self.total_size > 0:
            return min(self.offset / self.total_size, 1.0)
        return 0.0

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def is_running(self) -> bool:
        return self.state in RUNNING_STATES

    # ── mutations ─────────────────────────────────────────────────────────

    def set_state(self, new_state: TransferState, error: Optional[str] = None) -> None:
        """
        Transition to new_state, enforcing the legal-transition graph.

        Raises TransferStateError for illegal moves so bugs surface
        immediately. Records timestamps for the first ACTIVE transition
        and for any terminal transition.  Fires _on_state_change after
        updating (still inside the lock).
        """
        with self._lock:
            if new_state == self.state:
                return  # idempotent for same-state; not an error

            allowed = _LEGAL_TRANSITIONS.get(self.state, set())
            if new_state not in allowed:
                raise TransferStateError(
                    f"Transfer {self.id[:8]}: illegal transition "
                    f"{self.state.value!r} → {new_state.value!r}"
                )

            old_state = self.state
            self.state = new_state
            self.revision += 1

            if new_state == TransferState.ACTIVE and self.started_at is None:
                self.started_at = time.monotonic()

            if new_state in TERMINAL_STATES:
                self.ended_at = time.monotonic()
            elif new_state == TransferState.RETRYING:
                self.ended_at = None
                self.error = None

            if error is not None:
                self.error = error
            elif new_state not in (TransferState.FAILED,):
                # Clear a previous error when we successfully leave FAILED
                # (e.g. RETRYING → ACTIVE means the retry is working).
                if old_state == TransferState.RETRYING:
                    self.error = None

            if self._on_state_change is not None:
                self._on_state_change(self, old_state, new_state)

    def update_progress(self, offset: int, total_size: Optional[int] = None) -> None:
        """
        Update the byte offset (and optionally refine total_size if it
        wasn't known at creation time). Safe to call from any thread.
        """
        with self._lock:
            changed = self.offset != offset or (total_size is not None and total_size > 0 and total_size != self.total_size)
            self.offset = offset
            if total_size is not None and total_size > 0:
                self.total_size = total_size
            if changed:
                self.revision += 1
                if self._on_progress is not None:
                    self._on_progress(self)

    def to_dict(self) -> dict:
        """
        Serializable snapshot for API responses and WebSocket events.
        All values are JSON-safe primitives.
        """
        with self._lock:
            elapsed: Optional[float] = None
            if self.started_at is not None:
                end = self.ended_at if self.ended_at is not None else time.monotonic()
                elapsed = round(end - self.started_at, 3)

            return {
                "id":          self.id,
                "direction":   self.direction.value,
                "filename":    self.filename,
                "total_size":  self.total_size,
                "offset":      self.offset,
                "progress":    round(self.progress, 4),
                "state":       self.state.value,
                "error":       self.error,
                "checksum":    self.checksum,
                "attempt":     self.attempt,
                "revision":    self.revision,
                "generation":  self._event_generation,
                "resume_requested": self.resume_requested,
                "created_at":  round(self.created_at, 3),
                "started_at":  round(self.started_at, 3) if self.started_at else None,
                "ended_at":    round(self.ended_at, 3)   if self.ended_at   else None,
                "elapsed_sec": elapsed,
            }

    def __repr__(self) -> str:
        pct = f"{self.progress*100:.1f}%"
        return (
            f"Transfer(id={self.id[:8]}, {self.direction.value}, "
            f"{self.filename!r}, {pct}, {self.state.value})"
        )
