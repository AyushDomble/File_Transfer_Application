"""
transfer manager: TransferManager.

The single authority over every transfer's lifecycle. Nothing outside
this module changes a Transfer's state directly; everything goes through
the manager.

Responsibilities:
  - Create transfers and assign them IDs
  - Enforce the concurrency limit (max N ACTIVE transfers at once)
  - Schedule QUEUED transfers when a running slot opens up
  - Expose pause / resume / cancel / retry operations
  - Notify listeners on every state change (for WebSocket events, UI)
  - Keep the transfer registry and provide query methods

What it deliberately does NOT do:
  - Any network I/O (that belongs in session_protocol / transport)
  - Any file I/O
  - Know anything about sockets, HTTP, or FastAPI

The concurrency model is intentionally simple for now:
  QUEUED transfers start (ACTIVE) as soon as a slot is free.
  max_concurrent governs how many may be ACTIVE at once.
  The scheduler runs after every state change that might open a slot.
"""

from __future__ import annotations

import threading
from typing import Callable, Iterator, Optional

from .logsetup import get_logger
from .transfer import (
    Transfer,
    TransferDirection,
    TransferState,
    TransferStateError,
    RUNNING_STATES,
    TERMINAL_STATES,
)

log = get_logger("filetransfer.manager")


# Listener signature: fn(transfer, old_state, new_state)
StateChangeListener = Callable[[Transfer, TransferState, TransferState], None]


class TransferNotFound(Exception):
    """Raised when an operation targets an ID that doesn't exist."""


class TransferManager:
    """
    Central registry and scheduler for all transfers.

    Thread-safety: the internal registry (_transfers) is protected by
    _lock.  Individual Transfer mutations are protected by each
    Transfer's own lock (see transfer.py). The registry lock is never
    held while acquiring an existing transfer's lock or notifying listeners.
    """

    def __init__(self, max_concurrent: int = 3) -> None:
        self.max_concurrent = max_concurrent

        # transfer_id -> Transfer (insertion-ordered; Python 3.7+)
        self._transfers: dict[str, Transfer] = {}
        self._lock = threading.Lock()
        # Reservation is atomic, but this lock is NEVER held while acquiring
        # a transfer/registry lock or invoking a listener. Admission always
        # locks one transfer first, then briefly reserves its slot.
        self._slot_lock = threading.Lock()
        self._slots: set[str] = set()
        self.suspended = False  # session loss freezes admission, not user intent
        self._admission_check = lambda: True  # scheduler's lock-free liveness gate
        self.namespace = ''

        # External listeners notified after every state transition.
        # Called from whatever thread triggered the transition —
        # keep implementations fast (queue to another thread if needed).
        self._listeners: list[StateChangeListener] = []
        self._progress_listeners = []

    @property
    def max_concurrent(self):
        return self._max_concurrent

    @max_concurrent.setter
    def max_concurrent(self, value):
        if type(value) is not int or value < 1:
            raise ValueError('max_concurrent must be a positive integer')
        self._max_concurrent = value

    # ── listener management ───────────────────────────────────────────────

    def add_progress_listener(self, fn):
        with self._lock:
            if fn not in self._progress_listeners:
                self._progress_listeners.append(fn)

    def remove_progress_listener(self, fn):
        with self._lock:
            if fn in self._progress_listeners:
                self._progress_listeners.remove(fn)

    def _notify_progress(self, transfer):
        for fn in list(self._progress_listeners):
            try:
                fn(transfer)
            except Exception:
                log.exception('Progress listener raised')

    def add_listener(self, fn: StateChangeListener) -> None:
        """Register a callback: fn(transfer, old_state, new_state)."""
        with self._lock:
            self._listeners.append(fn)

    def remove_listener(self, fn: StateChangeListener) -> None:
        with self._lock:
            try:
                self._listeners.remove(fn)
            except ValueError:
                pass

    def _notify(self, transfer: Transfer, old: Optional[TransferState], new: TransferState) -> None:
        """
        Internal hook wired into each Transfer._on_state_change, and
        also called directly once by create_transfer() with old=None
        to announce creation (see there). Called while the Transfer's
        own lock is held. Registry-only queries are safe, but callbacks
        must not mutate other transfers or take their locks (deadlock risk).
        """
        if new in RUNNING_STATES:
            with self._slot_lock:
                self._slots.add(transfer.id)
            transfer.resume_requested = False
        else:
            with self._slot_lock:
                self._slots.discard(transfer.id)
            if new in TERMINAL_STATES or new == TransferState.CONNECTION_LOST:
                transfer.resume_requested = False
        if new in TERMINAL_STATES or new == TransferState.CONNECTION_LOST:
            from .transferfiles import release_destination
            release_destination(transfer)
        listeners = list(self._listeners)  # snapshot without holding _lock
        for fn in listeners:
            try:
                fn(transfer, old, new)
            except Exception:
                log.exception("State-change listener raised an exception")

        # After any transition that might open a slot, run the scheduler.
        # We do this outside the Transfer's lock (schedule() acquires
        # _lock and may call set_state on other transfers).
        if new in TERMINAL_STATES or new == TransferState.PAUSED:
            # Defer to avoid holding the caller's lock while scheduling.
            t = threading.Thread(target=self._schedule, daemon=True)
            t.start()

    # ── creation ──────────────────────────────────────────────────────────

    def create_transfer(
        self,
        direction: TransferDirection,
        filename: str,
        total_size: int = 0,
        checksum: Optional[str] = None,
        start_immediately: bool = True,
        transfer_id: Optional[str] = None,
        on_created: Optional[Callable[[Transfer], None]] = None,
        ready: bool = True,
    ) -> Transfer:
        """
        Register a new transfer and return it in QUEUED state.
        The manager immediately tries to start it if a slot is free,
        unless start_immediately=False.

        transfer_id, when given, is used as-is instead of generating a
        random one — for the receiving side of a handshake, which must
        share the peer's ID. Registering the final ID atomically prevents
        concurrent scheduling from observing a temporary lookup key.

        on_created(transfer), when given, runs synchronously while
        still holding the manager's lock, immediately after the
        transfer becomes visible in the registry — before
        start_immediately's own _schedule() call and before any other
        thread's _schedule() call (e.g. one triggered by an unrelated
        transfer finishing) can possibly see this transfer as a
        candidate. This is the only race-free place to record
        something keyed by the new transfer's ID that must exist
        before the transfer can be promoted to ACTIVE — for example a
        scheduler's pending-engine-job entry (see scheduler.py).
        start_immediately=False alone does NOT provide this guarantee:
        it only suppresses this call's own scheduling pass, not a
        concurrent one triggered by something else entirely.
        """
        kwargs = {"id": transfer_id} if transfer_id is not None else {}
        transfer = Transfer(
            direction=direction,
            filename=filename,
            total_size=total_size,
            checksum=checksum,
            ready=False,
            **kwargs,
        )
        transfer._on_state_change = self._notify
        transfer._on_progress = self._notify_progress
        transfer.namespace = self.namespace

        with transfer.lock:
            with self._lock:
                if transfer.id in self._transfers:
                    raise ValueError("transfer ID already exists")
                self._transfers[transfer.id] = transfer
                try:
                    if on_created is not None:
                        on_created(transfer)
                except Exception:
                    self._transfers.pop(transfer.id, None)
                    raise
            # transfer events: Transfer starts life already in QUEUED — there is
            # no transition INTO it for set_state() to notice, so without
            # this, no listener would ever see a "created" event at all.
            # old=None is listeners' signal that this is a creation, not
            # a transition (see webapp/api.py's _event_name_for).
            self._notify(transfer, None, transfer.state)
            transfer.ready = ready

        log.info(
            "Transfer created: %s %s %r (%s bytes)",
            transfer.id[:8], direction.value, filename, total_size,
        )

        if start_immediately:
            self._schedule()
        return transfer

    def schedule(self) -> None:
        """Public entry point to (re-)run the scheduler. Only needed
        after create_transfer(start_immediately=False) — prefer
        on_created for anything that must be recorded before this
        transfer can legally go ACTIVE; see its docstring above."""
        self._schedule()

    def discard_unaccepted(self, transfer_id):
        """Rollback an inbound CREATE whose ACCEPT could not be sent."""
        transfer = self.get(transfer_id)
        if transfer.ready or transfer.state != TransferState.QUEUED:
            raise TransferStateError('only an unaccepted queued transfer can be discarded')
        self.cancel_transfer(transfer_id)
        with self._lock:
            self._transfers.pop(transfer_id, None)

    # ── queries ───────────────────────────────────────────────────────────

    def get(self, transfer_id: str) -> Transfer:
        """Return the Transfer for this ID, or raise TransferNotFound."""
        with self._lock:
            t = self._transfers.get(transfer_id)
        if t is None:
            raise TransferNotFound(transfer_id)
        return t

    def all_transfers(self) -> list[Transfer]:
        """Return a snapshot of all transfers (any state), oldest first."""
        with self._lock:
            return list(self._transfers.values())

    def active_transfers(self) -> list[Transfer]:
        """Transfers currently occupying a concurrency slot."""
        with self._lock:
            return [t for t in self._transfers.values() if t.state in RUNNING_STATES]

    def queued_transfers(self) -> list[Transfer]:
        """Transfers waiting for a concurrency slot."""
        with self._lock:
            return [t for t in self._transfers.values()
                    if t.state == TransferState.QUEUED]

    def running_count(self) -> int:
        """Number of transfers currently occupying a concurrency slot."""
        with self._lock:
            return sum(1 for t in self._transfers.values() if t.state in RUNNING_STATES)

    def __iter__(self) -> Iterator[Transfer]:
        return iter(self.all_transfers())

    def __len__(self) -> int:
        with self._lock:
            return len(self._transfers)

    # ── lifecycle operations ──────────────────────────────────────────────

    def start_transfer(self, transfer_id: str) -> None:
        """
        Explicitly move a QUEUED transfer to ACTIVE.

        Direct calls obey readiness and the same concurrency reservations
        as automatic scheduling; this is not a bypass for the limit.
        """
        transfer = self.get(transfer_id)
        try:
            if not self._admit(transfer):
                raise TransferStateError('no concurrency slot available or transfer not ready')
            log.info("Transfer started: %s %r", transfer.id[:8], transfer.filename)
        except TransferStateError as e:
            log.warning("start_transfer: %s", e)
            raise

    def pause_transfer(self, transfer_id: str) -> None:
        """
        Request a pause on an ACTIVE transfer.

        Moves to PAUSING immediately; the actual network layer is
        responsible for reaching a safe checkpoint and then calling
        set_state(PAUSED). Callers that handle the network themselves
        must do that second step — the manager only issues the request.
        """
        transfer = self.get(transfer_id)
        try:
            with transfer.lock:
                if transfer.state == TransferState.PAUSED:
                    if transfer.resume_requested:
                        transfer.resume_requested = False
                        transfer.revision += 1
                        # Withdrawing admission intent is a real snapshot
                        # change even though the safe-point state is unchanged.
                        self._notify(transfer, transfer.state, transfer.state)
                    return
                transfer.resume_requested = False
                transfer.set_state(TransferState.PAUSED if transfer.state == TransferState.QUEUED
                                   else TransferState.PAUSING)
            log.info("Transfer pause requested: %s %r", transfer.id[:8], transfer.filename)
        except TransferStateError as e:
            log.warning("pause_transfer: %s", e)
            raise

    def confirm_paused(self, transfer_id: str) -> None:
        """
        Called by the network layer when a safe checkpoint has been
        reached after a pause request.  Moves PAUSING → PAUSED.
        """
        transfer = self.get(transfer_id)
        try:
            transfer.set_state(TransferState.PAUSED)
            log.info("Transfer paused: %s %r", transfer.id[:8], transfer.filename)
        except TransferStateError as e:
            log.warning("confirm_paused: %s", e)
            raise

    def resume_transfer(self, transfer_id: str) -> None:
        """
        Request admission for a PAUSED transfer. It stays PAUSED with
        resume_requested=True until capacity is available. The engine must
        wait for ACTIVE before restarting I/O.
        """
        transfer = self.get(transfer_id)
        with transfer.lock:
            if transfer.state == TransferState.ACTIVE:
                return
            if transfer.state != TransferState.PAUSED:
                raise TransferStateError('only a paused transfer can resume')
            if not transfer.resume_requested:
                transfer.resume_requested = True
                transfer.revision += 1
                # Same-state notification: intent precedes admission, not a
                # fictitious protocol state or a premature ACTIVE transition.
                self._notify(transfer, transfer.state, transfer.state)
        self._schedule()

    def cancel_transfer(self, transfer_id: str) -> None:
        """
        Cancel a transfer from any non-terminal state.

        This is the clean cancellation path — it does NOT close the
        session or any other transfer.  The network layer is responsible
        for acting on the CANCELLED state (stopping I/O, cleaning up
        .part files, etc.).
        """
        transfer = self.get(transfer_id)
        with transfer.lock:
            if transfer.is_terminal:
                return False
            transfer.set_state(TransferState.CANCELLED)
            log.info("Transfer cancelled: %s %r", transfer.id[:8], transfer.filename)
            return True

    def fail_transfer(self, transfer_id: str, error: str) -> None:
        """
        Mark a transfer as FAILED with a human-readable error message.
        Called by the network layer when an unrecoverable error occurs.
        The transfer may subsequently be retried via retry_transfer().
        """
        transfer = self.get(transfer_id)
        if transfer.is_terminal:
            log.debug(
                "fail_transfer: %s already terminal (%s), ignoring",
                transfer.id[:8], transfer.state.value,
            )
            return
        try:
            transfer.set_state(TransferState.FAILED, error=error)
            log.error(
                "Transfer failed: %s %r — %s",
                transfer.id[:8], transfer.filename, error,
            )
        except TransferStateError as e:
            log.warning("fail_transfer: %s", e)
            raise

    def complete_transfer(self, transfer_id: str, checksum: Optional[str] = None) -> None:
        """
        Mark a transfer as successfully COMPLETED.
        Optionally record the verified checksum for reference.
        """
        transfer = self.get(transfer_id)
        with transfer.lock:
            if transfer.state == TransferState.CANCELLED:
                return False
            if transfer.state == TransferState.COMPLETED:
                if checksum is not None and checksum != transfer.checksum:
                    raise TransferStateError('completed transfer checksum cannot change')
                return True
            if checksum is not None:
                transfer.checksum = checksum
            transfer.set_state(TransferState.COMPLETED)
            log.info("Transfer completed: %s %r", transfer.id[:8], transfer.filename)
            return True

    def retry_transfer(self, transfer_id: str) -> None:
        """
        Request a retry for a FAILED transfer.

        Moves FAILED → RETRYING, then _schedule() will move it to
        ACTIVE when a slot is available (treating RETRYING the same as
        QUEUED for scheduling purposes).
        """
        transfer = self.get(transfer_id)
        with transfer.lock:
            if transfer.state != TransferState.FAILED:
                raise TransferStateError('only a failed transfer can retry')
            # Set all retry metadata BEFORE listeners can observe RETRYING.
            transfer.offset = 0
            transfer.attempt += 1
            transfer.set_state(TransferState.RETRYING)
            log.info("Transfer retry requested: %s %r", transfer.id[:8], transfer.filename)

        self._schedule()

    def mark_connection_lost(self, transfer_id: str) -> bool:
        """
        recovery: the connection carrying this transfer died. Moves an
        ACTIVE / PAUSING / RETRYING transfer to CONNECTION_LOST (frees
        its concurrency slot; keeps offset and .part). Idempotent and
        race-tolerant: returns False, changing nothing, if the transfer
        is QUEUED (never started — nothing to lose), PAUSED (stays
        paused: the user's intent survives the outage), already lost,
        or terminal (it finished or was cancelled first).
        """
        transfer = self.get(transfer_id)
        with transfer.lock:
            cur = transfer.state
        if cur not in (TransferState.ACTIVE, TransferState.PAUSING,
                       TransferState.RETRYING):
            return False
        try:
            transfer.set_state(TransferState.CONNECTION_LOST)
        except TransferStateError:
            return False       # lost a race with completion/cancel — fine
        log.warning("Transfer connection lost: %s %r at offset %d",
                    transfer.id[:8], transfer.filename, transfer.offset)
        # Frees a slot for anything QUEUED.
        threading.Thread(target=self._schedule, daemon=True).start()
        return True

    def requeue_transfer(self, transfer_id: str) -> None:
        """
        recovery: CONNECTION_LOST -> QUEUED. The scheduler then promotes
        it to ACTIVE as soon as a slot is free, exactly like a new
        transfer, so recovery respects max_concurrent.
        """
        transfer = self.get(transfer_id)
        transfer.set_state(TransferState.QUEUED)
        log.info("Transfer re-queued for recovery: %s %r",
                 transfer.id[:8], transfer.filename)
        self._schedule()

    def cancel_all(self) -> int:
        """
        Cancel every non-terminal transfer. Returns how many were cancelled.

        The manager remains I/O-free. When a ConcurrentScheduler is
        attached, its cancellation listener stops I/O and notifies the peer.
        scheduler.cancel_all() additionally handles dormant partial files.
        """
        cancelled = 0
        with self._lock:
            ids = list(self._transfers.keys())
        for tid in ids:
            try:
                t = self.get(tid)
                if not t.is_terminal:
                    cancelled += int(self.cancel_transfer(tid))
            except (TransferNotFound, TransferStateError):
                pass
        log.info("cancel_all: cancelled %d transfer(s)", cancelled)
        return cancelled

    # ── scheduler ─────────────────────────────────────────────────────────

    def _admit(self, transfer: Transfer) -> bool:
        with transfer.lock:
            if transfer.state in RUNNING_STATES:
                return True
            if transfer.state not in (TransferState.QUEUED, TransferState.RETRYING,
                                      TransferState.PAUSED):
                raise TransferStateError('transfer cannot be admitted from this state')
            if not transfer.ready:
                return False
            if self.suspended or not self._admission_check():
                return False
            with self._slot_lock:
                if len(self._slots) >= self.max_concurrent:
                    return False
                self._slots.add(transfer.id)
            try:
                transfer.set_state(TransferState.ACTIVE)
            except BaseException:
                with self._slot_lock:
                    self._slots.discard(transfer.id)
                raise
            return True

    def _schedule(self) -> None:
        """
        Start QUEUED (or RETRYING) transfers up to max_concurrent.

        Called automatically after every state change that may free a
        slot (terminal state or PAUSED).  Also called explicitly after
        create_transfer() and retry_transfer().

        Multiple passes may overlap; _admit reserves each slot atomically.
        No registry/slot lock is held across transitions or callbacks.
        """
        with self._lock:
            # Stable creation order includes retries and requested resumes.
            candidates = [
                t for t in self._transfers.values()
                if t.ready and (t.state in (TransferState.QUEUED, TransferState.RETRYING)
                                or (t.state == TransferState.PAUSED and t.resume_requested))
            ]

        # Promote up to free_slots candidates.
        # We call set_state() outside _lock (Transfer has its own lock).
        for transfer in candidates:
            try:
                # Recheck eligibility under the transfer lock: concurrent
                # passes must not resurrect a pause or retry an active job.
                with transfer.lock:
                    if transfer.state == TransferState.PAUSED and not transfer.resume_requested:
                        continue
                    if not self._admit(transfer):
                        break
            except TransferStateError:
                # Race: transfer was cancelled/failed between the snapshot
                # and now.  Skip it and try the next candidate.
                pass

    # ── serialization helper ──────────────────────────────────────────────

    def to_dict(self) -> dict:
        """Snapshot of manager state for API/WebSocket responses."""
        transfers = self.all_transfers()
        return {
            "max_concurrent":    self.max_concurrent,
            "running":           sum(1 for t in transfers if t.state in RUNNING_STATES),
            "queued":            sum(1 for t in transfers if t.state == TransferState.QUEUED),
            "completed":         sum(1 for t in transfers if t.state == TransferState.COMPLETED),
            "failed":            sum(1 for t in transfers if t.state == TransferState.FAILED),
            "cancelled":         sum(1 for t in transfers if t.state == TransferState.CANCELLED),
            "connection_lost":   sum(1 for t in transfers if t.state == TransferState.CONNECTION_LOST),
            "transfers":         [t.to_dict() for t in transfers],
        }

    def __repr__(self) -> str:
        return (
            f"TransferManager(max_concurrent={self.max_concurrent}, "
            f"total={len(self)}, running={self.running_count()})"
        )
