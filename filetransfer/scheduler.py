"""Concurrent transfer scheduler.

One ConcurrentScheduler coordinates a Session, FrameRouter and TransferManager.
Submissions register queued jobs; engine threads and creation handshakes start
when admission permits. Inbound requests follow the same concurrency limit.
Pause, resume, cancel and retry target the running engine or retain pending intent
while engine construction completes. FrameRouter demultiplexes frames and the
session send lock keeps writes atomic. TransferManager remains independent of I/O."""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Callable, Optional, Union

from . import config
from .engine import (
    AlreadyComplete,
    DEFAULT_CHUNK_SIZE,
    FrameRouter,
    RecvEngine,
    SendEngine,
    TransferError,
    _compute_sha256,
    recv_transfer_create,
    require_feature,
    send_transfer_create,
)
from .flowcontrol import GlobalWindow, TransferWindow, validate_capacity
from .transferfiles import claim_destination, destination_for_part, partial_path, release_destination
from .logsetup import get_logger
from .manager import TransferManager, TransferNotFound
from .session_protocol import Session
from .persistence import transfer_metadata
from .transfer import Transfer, TransferDirection, TransferState, TransferStateError
from .transport import Frame, FrameType, FrameError

log = get_logger("filetransfer.scheduler")

EngineType = Union[SendEngine, RecvEngine]


@dataclass
class _Job:
    """
    Per-transfer bookkeeping between submission and completion.

    `build` constructs the engine — it does the TRANSFER_CREATE
    handshake for outbound jobs — and is only called the first time
    this transfer is promoted to ACTIVE, never at submission time.
    `engine`/`thread` are cached afterward so pause()/resume()/
    cancel()/retry() can reach the same running instance, and so a
    PAUSED -> ACTIVE resume doesn't mistakenly start a second thread.

    `pending_pause`/`pending_cancel` cover the window between a
    transfer going ACTIVE and its engine actually finishing
    construction (build() does a network handshake — it isn't
    instant). If pause()/cancel() is called in that window,
    `_engine_for()` finds nothing yet: without these flags, pause()
    would fall back to a manager-only state change that sets PAUSING
    and then never resolves it (nothing left to call confirm_paused()
    — the transfer is stuck forever), and cancel() would fall back to
    marking the transfer CANCELLED while the in-flight build() keeps
    going regardless and hands back a live engine that runs a full
    transfer nobody asked for anymore. Setting the flag here instead
    and having _launch() check it the instant the engine exists (scheduling's _on_state_change/_launch, below) closes both.
    """
    build: Callable[[], Optional[EngineType]]
    engine: Optional[EngineType] = None
    thread: Optional[threading.Thread] = None
    pending_pause: bool = False
    pending_cancel: bool = False
    # recovery: True when THIS side sent the TRANSFER_CREATE (submit_*
    # / reattach_*). Only an initiator's job can rebuild its engine on
    # a new connection — build() re-sends CREATE. A peer-initiated
    # (passive) transfer just waits for the peer's re-CREATE.
    initiator: bool = False


class ConcurrentScheduler:
    """
    Bridges TransferManager's pure state machine to real engine I/O.

    Thread-safety: `_lock` protects `_jobs` only. It is never held
    while calling into the manager, the engine, or the network —
    `_on_state_change` (the manager listener) always hands off to a
    fresh thread before doing anything slower than a dict lookup,
    because it runs synchronously inside the Transfer's own lock (see
    transfer.Transfer.set_state).
    """

    def __init__(
        self,
        session: Session,
        manager: TransferManager,
        router: FrameRouter,
        dest_dir: Optional[str] = None,
        persistence: Optional["PersistenceStore"] = None,
        send_window: Optional[GlobalWindow] = None,
        max_upload_size: int = 0,
        blocked_extensions: Optional[set] = None,
        keep_history: bool = False,
    ) -> None:
        self._session = session
        self._manager = manager
        self._router = router
        # Loss is observable before asynchronous loss listeners finish. Never
        # promote queued work in that gap, even if another worker frees a slot.
        manager._admission_check = lambda: not (getattr(self._session, 'is_lost', False)
                                               or self._router.connection_lost.is_set())
        self._dest_dir = dest_dir or config.SERVER_DIR
        self._persistence = persistence
        self._keep_history = keep_history
        # API integration: host-configurable limits, enforced by
        # recv_transfer_create() on every inbound push. Runtime-
        # adjustable — see update_settings() — so a host UI can change
        # these without restarting the connection.
        self._max_upload_size = max_upload_size
        self._blocked_extensions = blocked_extensions or set()
        # flow control: every scheduler-built sender has a shared session
        # budget and its own transfer budget. None selects the configured
        # global default; wire-level scoped limits remain negotiated.
        self._send_window = send_window if send_window is not None else GlobalWindow(config.GLOBAL_WINDOW_BYTES)
        self._transfer_windows = {}
        self._receive_global = GlobalWindow(config.GLOBAL_WINDOW_BYTES)
        self._receive_transfers = {}

        self._lock = threading.Lock()
        self._jobs: dict[str, _Job] = {}
        self._worker_threads: set[threading.Thread] = set()

        # Path info per transfer, for PersistenceStore.save()'s optional
        # part_path/dest_path columns — populated wherever a job is
        # created (submit_upload/submit_download/_handle_inbound_create),
        # read back only from _on_state_change. Uploads have no
        # meaningful part_path (we're reading a local file, not writing
        # one) — only dest_path is set, to the local source path, for
        # whatever informational value it has; it isn't read back
        # automatically the way a receiver's offset/part_path are.
        self._paths: dict[str, tuple[Optional[str], Optional[str]]] = {}

        self._accept_thread: Optional[threading.Thread] = None
        self._accept_stop = threading.Event()

        # recovery: passive (peer-initiated) transfers left PAUSED by a
        # connection loss. They have no job (nothing here can rebuild
        # an engine without the peer's CREATE), so a local resume()
        # must be refused — see resume().
        self._passive_dormant: set[str] = set()

        manager.add_listener(self._on_state_change)
        self._bind_router_loss(router)

    def _send_budget(self, transfer):
        with self._lock:
            budget = self._transfer_windows.setdefault(transfer.id, TransferWindow(config.TRANSFER_WINDOW_BYTES))
        if hasattr(transfer, 'peer_window'):
            budget.resize(min(budget.capacity, transfer.peer_window))
        if hasattr(transfer, 'peer_global_window'):
            self._send_window.resize(min(self._send_window.capacity, transfer.peer_global_window))
        return budget

    def _receive_limits(self, transfer_id):
        with self._lock:
            window = self._receive_transfers.setdefault(transfer_id, TransferWindow(config.TRANSFER_WINDOW_BYTES))
        return self._receive_global.capacity, window.capacity

    def _on_window_update(self, frame):
        try:
            payload = frame.json()
            capacity = validate_capacity(payload.get('capacity'))
            scope = payload.get('scope', 'global')
            if scope not in ('global', 'transfer'):
                raise ValueError('invalid window scope')
            if scope == 'transfer' and not self._session.supports('scoped_flow_control'):
                raise ValueError('scoped flow control was not negotiated')
            if scope == 'global':
                budget = self._send_window
            else:
                with self._lock:
                    budget = self._transfer_windows.get(frame.transfer_id)
                if budget is None:
                    return
            budget.resize(min(capacity, budget.capacity) if payload.get('automatic') else capacity)
        except (ValueError, FrameError) as exc:
            try:
                self._session.send_transfer_error(frame.transfer_id, str(exc))
                self._manager.fail_transfer(frame.transfer_id, str(exc))
            except (TransferNotFound, TransferStateError):
                pass

    def restore_jobs(self):
        """Bind durable initiator records; passive jobs wait for peer CREATE."""
        for transfer in self._manager.all_transfers():
            with self._lock:
                self._paths[transfer.id] = (transfer.part_path, transfer.dest_path or getattr(transfer, 'source_path', None))
            if transfer.is_terminal and transfer.state != TransferState.FAILED:
                continue
            if not getattr(transfer, 'initiator', False):
                if transfer.state == TransferState.PAUSED:
                    self._passive_dormant.add(transfer.id)
                continue
            try:
                if transfer.direction == TransferDirection.UPLOAD:
                    self.reattach_upload(transfer, transfer.source_path,
                        chunk_size=transfer.chunk_size, overwrite=transfer.overwrite)
                else:
                    self.reattach_download(transfer, transfer.part_path, chunk_size=transfer.chunk_size)
            except (OSError, ValueError, TypeError) as exc:
                if not transfer.is_terminal:
                    self._manager.fail_transfer(transfer.id, f'recovery source unavailable: {exc}')

    # ── outbound: we initiate ────────────────────────────────────────

    def _upload_build(
        self, file_path: str, chunk_size: int, overwrite: bool,
        progress_cb: Optional[Callable[[int, int], None]],
    ) -> Callable[[Transfer], Optional[SendEngine]]:
        """Factory for an initiator upload job's build(): (re)does the
        TRANSFER_CREATE handshake against whatever session/router this
        scheduler is bound to AT CALL TIME — so the same job works
        unchanged on a fresh connection after recovery."""
        def _make(engine_t: Transfer) -> Optional[SendEngine]:
            try:
                resume_offset = send_transfer_create(
                    self._session, engine_t, self._router,
                    chunk_size=chunk_size, overwrite=overwrite,
                )
            except AlreadyComplete as ac:
                # Receiver finished before the connection died.
                if engine_t.total_size:
                    engine_t.update_progress(engine_t.total_size)
                self._manager.complete_transfer(engine_t.id, checksum=ac.sha256)
                return None
            engine_t.update_progress(resume_offset)
            q = self._router.get_queue(engine_t.id)
            return SendEngine(
                session=self._session, transfer=engine_t, manager=self._manager,
                frame_queue=q, file_path=file_path, resume_offset=resume_offset,
                chunk_size=getattr(engine_t, 'chunk_size', chunk_size), progress_cb=progress_cb,
                persistence=self._persistence, flow_window=self._send_window,
                transfer_window=self._send_budget(engine_t),
            )
        return _make

    def _download_build(
        self, part_path: str, chunk_size: int,
        progress_cb: Optional[Callable[[int, int], None]],
    ) -> Callable[[Transfer], Optional[RecvEngine]]:
        from .persistence import reconcile_offset

        def _make(engine_t: Transfer) -> Optional[RecvEngine]:
            claim_destination(engine_t, engine_t.dest_path, part_path,
                              overwrite=engine_t.overwrite)
            # A download that already made progress (recovery) tells
            # the serving side what it holds; the .part file is
            # reconciled against the recorded offset first.
            have = 0
            if engine_t.offset > 0:
                have = reconcile_offset(part_path, engine_t.offset)
            try:
                resume_offset = send_transfer_create(
                    self._session, engine_t, self._router,
                    chunk_size=chunk_size, have_offset=have,
                    receive_window=min(self._receive_limits(engine_t.id)),
                    receive_global_window=self._receive_global.capacity,
                )
            except AlreadyComplete:
                return None  # cannot happen for a pull; defensive
            engine_t.update_progress(resume_offset)
            q = self._router.get_queue(engine_t.id)
            return RecvEngine(
                session=self._session, transfer=engine_t, manager=self._manager,
                frame_queue=q, part_path=part_path, resume_offset=resume_offset,
                progress_cb=progress_cb, persistence=self._persistence,
                receive_limits=lambda: self._receive_limits(engine_t.id),
            )
        return _make

    def submit_upload(
        self,
        file_path: str,
        filename: Optional[str] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        overwrite: bool = False,
        progress_cb: Optional[Callable[[int, int], None]] = None,
        source_owned: bool = False,
        batch_id: Optional[str] = None,
        _prepared_checksum: Optional[str] = None,
    ) -> Transfer:
        """Register an upload and return immediately (QUEUED, or ACTIVE
        if a slot happened to be free). Unlike engine.run_upload(),
        this never blocks the calling thread on the transfer itself."""
        if not os.path.isfile(file_path):
            raise FileNotFoundError(file_path)
        fname  = filename or os.path.basename(file_path)
        size   = os.path.getsize(file_path)
        # Only the local batch preparer can supply this internal optimization.
        # Approved manifests and the final source/destination rehash still check
        # these exact bytes; arbitrary external paths never bypass hashing.
        if _prepared_checksum is not None:
            if not source_owned or not batch_id or len(_prepared_checksum) != 64 or any(c not in '0123456789abcdef' for c in _prepared_checksum):
                raise ValueError('Prepared checksum requires an owned batch source and SHA-256')
            sha256 = _prepared_checksum
        else:
            sha256 = _compute_sha256(file_path)
        make = self._upload_build(file_path, chunk_size, overwrite, progress_cb)

        def _on_created(t: Transfer) -> None:
            t.initiator, t.source_path, t.source_owned = True, os.path.abspath(file_path), source_owned
            t.batch_id = batch_id
            t.overwrite, t.chunk_size = overwrite, chunk_size
            with self._lock:
                self._jobs[t.id] = _Job(build=lambda et=t: make(et), initiator=True)
                self._paths[t.id] = (None, file_path)

        return self._manager.create_transfer(
            direction=TransferDirection.UPLOAD,
            filename=fname, total_size=size, checksum=sha256,
            on_created=_on_created,
        )

    def submit_download(
        self,
        remote_filename: str,
        dest_dir: Optional[str] = None,
        dest_path: Optional[str] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        progress_cb: Optional[Callable[[int, int], None]] = None,
    ) -> Transfer:
        """Register a download (pull) request and return immediately."""
        from .security import safe_filename as _sf
        safe_name = _sf(remote_filename)
        target_dir = dest_dir or self._dest_dir

        if dest_path:
            os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
        else:
            os.makedirs(target_dir, exist_ok=True)
            dest_path = os.path.join(target_dir, safe_name)

        def _on_created(t: Transfer) -> None:
            t.initiator, t.chunk_size = True, chunk_size
            part = partial_path(dest_path, t.id, self._manager.namespace)
            claim_destination(t, dest_path, part, overwrite=True)
            make = self._download_build(part, chunk_size, progress_cb)
            with self._lock:
                self._jobs[t.id] = _Job(build=lambda et=t: make(et), initiator=True)
                self._paths[t.id] = (part, dest_path)

        return self._manager.create_transfer(
            direction=TransferDirection.DOWNLOAD,
            filename=safe_name, total_size=0,
            on_created=_on_created,
        )

    # ── recovery: initiator jobs for transfers restored after a restart ──

    def reattach_upload(
        self,
        transfer: Transfer,
        file_path: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        overwrite: bool = False,
        progress_cb: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        """
        Give a transfer rebuilt by persistence.restore_transfer(...,
        connection_lost=True) an initiator job, so recover_all() /
        recover_transfer() can re-CREATE it on a fresh connection —
        the restart-plus-reconnect case. (resume_restored_upload() is
        for the different case where the old connection is still
        alive.) `file_path` is required for the same reason as there:
        the persistence schema has no source-path column.
        """
        if transfer.direction != TransferDirection.UPLOAD:
            raise TransferStateError("reattach_upload needs an UPLOAD transfer")
        if transfer.state not in (TransferState.CONNECTION_LOST, TransferState.PAUSED, TransferState.FAILED):
            raise TransferStateError(
                f"reattach_upload expects CONNECTION_LOST or PAUSED, "
                f"got {transfer.state.value}")
        if not os.path.isfile(file_path):
            raise FileNotFoundError(file_path)
        if not transfer.checksum:
            transfer.checksum = _compute_sha256(file_path)
        make = self._upload_build(file_path, chunk_size, overwrite, progress_cb)
        with self._lock:
            self._jobs[transfer.id] = _Job(build=lambda et=transfer: make(et), initiator=True)
            self._paths[transfer.id] = (None, file_path)
            self._passive_dormant.discard(transfer.id)

    def reattach_download(
        self,
        transfer: Transfer,
        part_path: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        progress_cb: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        """Download counterpart of reattach_upload(). `part_path` is the
        restored row's part_path; the on-disk .part is reconciled
        against the recorded offset when the transfer is recovered."""
        if transfer.direction != TransferDirection.DOWNLOAD:
            raise TransferStateError("reattach_download needs a DOWNLOAD transfer")
        if transfer.state not in (TransferState.CONNECTION_LOST, TransferState.PAUSED, TransferState.FAILED):
            raise TransferStateError(
                f"reattach_download expects CONNECTION_LOST or PAUSED, "
                f"got {transfer.state.value}")
        make = self._download_build(part_path, chunk_size, progress_cb)
        with self._lock:
            self._jobs[transfer.id] = _Job(build=lambda et=transfer: make(et), initiator=True)
            destination = transfer.dest_path or destination_for_part(part_path, transfer.id)
            transfer.dest_path = destination
            self._paths[transfer.id] = (part_path, destination)
            self._passive_dormant.discard(transfer.id)

    def resume_restored_upload(
        self,
        transfer: Transfer,
        file_path: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        progress_cb: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        """
        Re-attach a Transfer reconstructed by
        persistence.restore_transfer() (PAUSED, in this scheduler's own
        TransferManager) so pause()/resume()/cancel() work on it exactly
        as they would on one this scheduler created itself.

        Unlike submit_upload(), this does no TRANSFER_CREATE handshake —
        the transfer already exists on both sides from before whatever
        restart or crash intervened; that continuity is the entire point
        of persisting it. Call resume(transfer.id) after this to actually
        start it: the built engine's first action is a normal wire-level
        RESUME/resuming-ACK exchange (SendEngine's needs_resume_handshake),
        using the peer's confirmed offset rather than blindly trusting
        the last offset this process had on disk — the peer may have
        accepted a few more in-flight bytes before whatever interrupted
        this side actually did.

        `file_path` is the local source file to resume reading from —
        PersistenceStore's schema has no column for it (it's receiver-
        oriented: part_path/dest_path), so the caller must supply it,
        the same as submit_upload() requires it.
        """
        if transfer.state != TransferState.PAUSED:
            raise TransferStateError(
                f"resume_restored_upload expects a PAUSED transfer, "
                f"got {transfer.state.value}"
            )

        def _build(engine_t=transfer) -> SendEngine:
            q = self._router.register(engine_t.id)
            return SendEngine(
                session=self._session, transfer=engine_t, manager=self._manager,
                frame_queue=q, file_path=file_path, resume_offset=engine_t.offset,
                chunk_size=chunk_size, progress_cb=progress_cb,
                needs_resume_handshake=True, persistence=self._persistence,
                flow_window=self._send_window,
            )

        with self._lock:
            self._jobs[transfer.id] = _Job(build=_build)
            self._paths[transfer.id] = (None, file_path)

    def resume_restored_download(
        self,
        transfer: Transfer,
        part_path: str,
        dest_path: Optional[str] = None,
        progress_cb: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """
        Receiver-side counterpart of resume_restored_upload(): re-attach
        a download reconstructed by persistence.restore_transfer()
        (PAUSED) after THIS side restarted. No TRANSFER_CREATE
        handshake — the transfer already exists on both sides. Call
        resume(transfer.id) afterwards to start it; the built engine
        then does the normal RESUME / resuming-ACK exchange with the
        sender, which has been paused waiting for exactly that.

        The offset is reconciled against the .part file on disk
        (persistence.reconcile_offset): the smaller of the saved offset
        and the file's actual size wins, and the file is truncated to
        it, so we never claim bytes that aren't really there and never
        keep a possibly-torn tail. A missing .part restarts from 0.
        Returns the reconciled offset.

        `part_path` is the row's part_path (see PersistenceStore).
        """
        from .persistence import reconcile_offset

        if transfer.state != TransferState.PAUSED:
            raise TransferStateError(
                f"resume_restored_download expects a PAUSED transfer, "
                f"got {transfer.state.value}"
            )
        if transfer.direction != TransferDirection.DOWNLOAD:
            raise TransferStateError("resume_restored_download needs a DOWNLOAD transfer")

        offset = reconcile_offset(part_path, transfer.offset)
        transfer.update_progress(offset)
        final_path = dest_path or transfer.dest_path or destination_for_part(part_path, transfer.id)
        claim_destination(transfer, final_path, part_path, overwrite=transfer.overwrite)

        # Register the frame queue NOW, not lazily in _build(): frames
        # for an unregistered transfer_id are dropped by the router, and
        # the sender may send something (e.g. a duplicate PAUSE) before
        # the user calls resume().
        q = self._router.register(transfer.id)

        def _build(engine_t=transfer, off=offset) -> RecvEngine:
            return RecvEngine(
                session=self._session, transfer=engine_t, manager=self._manager,
                frame_queue=q, part_path=part_path, resume_offset=off,
                progress_cb=progress_cb, persistence=self._persistence,
                needs_resume_handshake=True,
            )

        with self._lock:
            self._jobs[transfer.id] = _Job(build=_build)
            self._paths[transfer.id] = (part_path, final_path)
        return offset

    # ── inbound: peer initiates ──────────────────────────────────────

    def start_accept_loop(self) -> None:
        """Background thread: consumes inbound TRANSFER_CREATE requests
        via router.next_create() and registers a job for each, same
        atomic on_created path as submit_upload/submit_download — so
        max_concurrent applies to peer-initiated transfers too."""
        if self._accept_thread is not None:
            return
        self._accept_stop.clear()
        self._accept_thread = threading.Thread(
            target=self._accept_loop, daemon=True, name="ConcurrentScheduler-accept"
        )
        self._accept_thread.start()

    def stop_accept_loop(self, timeout: float = 2.0) -> bool:
        self._accept_stop.set()
        if self._accept_thread:
            if self._accept_thread is not threading.current_thread():
                self._accept_thread.join(timeout=timeout)
            if self._accept_thread.is_alive():
                return False  # retain its identity; never hide a surviving writer
            self._accept_thread = None
        return True

    def _accept_loop(self) -> None:
        while not self._accept_stop.is_set():
            frame = self._router.next_create(timeout=0.5)
            if frame is None:
                continue
            try:
                self._handle_inbound_create(frame)
            except TransferError as e:
                log.warning("Inbound TRANSFER_CREATE rejected: %s", e)
            except Exception:
                log.exception("Error handling inbound TRANSFER_CREATE")

    def _handle_inbound_create(self, frame: Frame) -> None:
        validator = None
        guard = getattr(self, 'receive_guard', None)
        if guard:
            try:
                validator = guard(frame)
            except (ValueError, TypeError, KeyError) as exc:
                self._session.send_json(FrameType.TRANSFER_REJECT, {'reason': str(exc)},
                                        transfer_id=frame.transfer_id)
                return
        if self._router.is_cancelled(frame.transfer_id):
            self._session.send_json(FrameType.TRANSFER_REJECT, {'reason': 'transfer was cancelled'},
                                    transfer_id=frame.transfer_id)
            return
        def _on_created(t: Transfer, resume_offset: int, result_path: str) -> None:
            t.initiator = False
            t._receive_validator = validator
            t.batch_id = frame.json().get('batch_id')
            if t.direction == TransferDirection.UPLOAD:
                t.source_path = os.path.abspath(result_path)
            # t.id == frame.transfer_id here: recv_transfer_create()
            # passes transfer_id=tid straight into create_transfer(),
            # so there's no throwaway-ID window to worry about (see
            # its docstring).
            if t.direction == TransferDirection.DOWNLOAD:
                def _build(engine_t=t, p=result_path, off=resume_offset) -> RecvEngine:
                    return RecvEngine(
                        session=self._session, transfer=engine_t, manager=self._manager,
                        frame_queue=self._router.get_queue(engine_t.id),
                        part_path=p, resume_offset=off, persistence=self._persistence,
                        receive_limits=lambda: self._receive_limits(engine_t.id),
                    )
                path_entry = (result_path, t.dest_path or destination_for_part(result_path, t.id))
            else:
                def _build(engine_t=t, p=result_path, off=resume_offset) -> SendEngine:
                    return SendEngine(
                        session=self._session, transfer=engine_t, manager=self._manager,
                        frame_queue=self._router.get_queue(engine_t.id),
                        file_path=p, resume_offset=off, persistence=self._persistence,
                        chunk_size=getattr(engine_t, 'chunk_size', DEFAULT_CHUNK_SIZE),
                        flow_window=self._send_window,
                        transfer_window=self._send_budget(engine_t),
                    )
                path_entry = (None, result_path)
            with self._lock:
                self._jobs[t.id] = _Job(build=_build)
                self._paths[t.id] = path_entry

        transfer, _off, _path = recv_transfer_create(
            self._session, frame, self._dest_dir, self._manager, self._router,
            max_size=self._max_upload_size, blocked_extensions=self._blocked_extensions,
            on_created=_on_created, reattach_ok=self._reattach_ok,
            receive_window=min(self._receive_limits(frame.transfer_id)),
            receive_global_window=self._receive_global.capacity,
        )

        # recovery: a re-attached transfer comes back still CONNECTION_LOST
        # (or dormant-PAUSED). It's re-activated only now, after the
        # ACCEPT has gone out — an engine that started sending before
        # the peer saw ACCEPT would break the handshake.
        with transfer.lock:
            st = transfer.state
        if st == TransferState.CONNECTION_LOST:
            with self._lock:
                self._passive_dormant.discard(transfer.id)
            self._manager.requeue_transfer(transfer.id)
        elif st == TransferState.PAUSED:
            with self._lock:
                self._passive_dormant.discard(transfer.id)
            self._manager.resume_transfer(transfer.id)
        elif st == TransferState.FAILED:
            self._manager.retry_transfer(transfer.id)

    def _reattach_ok(self, transfer: Transfer) -> bool:
        """May an inbound re-CREATE take over this transfer? Only if no
        engine is currently driving it — otherwise a peer (or a bug)
        could hijack a live transfer by replaying its ID."""
        with transfer.lock:
            st = transfer.state
        if st == TransferState.CONNECTION_LOST:
            return True
        with self._lock:
            job = self._jobs.get(transfer.id)
        if st == TransferState.FAILED:
            if job is not None and job.thread is not None and job.thread is not threading.current_thread():
                job.thread.join(timeout=1)
            return transfer.io_finished.is_set() and (job is None or job.thread is None
                                                      or not job.thread.is_alive())
        return st == TransferState.PAUSED and (job is None or job.engine is None)

    # ── control: route to the real engine, not just the state label ──

    def pause(self, transfer_id: str) -> None:
        require_feature(self._session, 'pause')
        transfer = self._manager.get(transfer_id)
        with transfer.lock:
            if transfer.state == TransferState.QUEUED:
                self._manager.pause_transfer(transfer_id)
                return
        with self._lock:
            job = self._jobs.get(transfer_id)
            engine = job.engine if job is not None else None
            if job is not None and engine is None:
                job.pending_pause = True
        if engine is not None:
            engine.pause()
        else:
            try:
                self._manager.pause_transfer(transfer_id)
            except TransferStateError:
                # Not ACTIVE yet (still QUEUED) — fine if a job is
                # pending: pending_pause fires the instant it goes
                # ACTIVE and its engine is built. No job at all means
                # this transfer isn't ours to pause; let it raise.
                if job is None:
                    raise

    def resume(self, transfer_id: str) -> None:
        require_feature(self._session, 'resume')
        with self._lock:
            dormant = transfer_id in self._passive_dormant
        if dormant:
            raise TransferStateError(
                "this transfer was paused when the connection dropped and the "
                "peer started it; only the peer can resume it after reconnecting"
            )
        with self._lock:
            job = self._jobs.get(transfer_id)
            engine = job.engine if job is not None else None
            if job is not None and engine is None:
                job.pending_pause = False  # cancel a pause that hasn't applied yet
        if engine is not None:
            engine.resume()
        else:
            self._manager.resume_transfer(transfer_id)

    def cancel(self, transfer_id: str, *, notify_peer: bool = True) -> None:
        if notify_peer:
            require_feature(self._session, 'cancel')
        with self._lock:
            job = self._jobs.get(transfer_id)
            engine = job.engine if job is not None else None
            thread = job.thread if job is not None else None
            # recovery: a job's `engine` field is only cleared when the
            # job itself is rebuilt or popped, never the instant its
            # thread exits — so right after a connection loss, a job
            # can still be sitting there holding a reference to an
            # engine whose run() already returned. Calling .cancel()
            # on that stale engine would set manager state to
            # CANCELLED (its own cancel_transfer() call succeeds, since
            # CONNECTION_LOST -> CANCELLED is legal) but skip the file
            # cleanup a *live* engine's own cancel handling does — so
            # liveness is checked via the thread, not just the
            # reference — and a thread that's alive but already
            # mid-abort_connection_lost() is not "live" for this
            # purpose either: it's unwinding toward exit without doing
            # any cancel-time cleanup (deliberately — see
            # RecvEngine._run_inner's `if self._conn_lost: ... return`),
            # so calling its .cancel() would flip manager state to
            # CANCELLED but leave the .part file behind. Treat that
            # case like "no engine" so the cleanup below runs instead.
            live = (engine is not None and thread is not None
                    and thread.is_alive()
                    and not getattr(engine, "_conn_lost", False))
            if job is not None and not live:
                job.pending_cancel = True
        if live:
            engine.cancel(notify_peer=notify_peer)
        else:
            # recovery: a transfer with no engine can still own a .part
            # file — one waiting after a connection loss, or dormant
            # after a restart. No engine means nobody else will remove
            # it (RecvEngine._handle_cancel normally does), so do it
            # here, after the state change has succeeded.
            with self._lock:
                part_path = self._paths.get(transfer_id, (None, None))[0]
            try:
                waiting = self._manager.get(transfer_id).state in (
                    TransferState.CONNECTION_LOST, TransferState.PAUSED)
            except TransferNotFound:
                waiting = False
            self._manager.cancel_transfer(transfer_id)
            if part_path:
                # A lost engine may still be closing a Windows file handle.
                # Its job may already have been detached by recovery logic.
                self._manager.get(transfer_id).io_finished.wait(2)
                try:
                    os.remove(part_path)
                except OSError:
                    pass
            if notify_peer:
                try:
                    self._session.send_transfer_cancel(transfer_id, 'cancelled by user')
                except Exception:
                    pass
            # No further cleanup needed here even when job is not
            # None: _on_state_change already pops the job on the
            # CANCELLED transition this just triggered (or will, once
            # the in-flight build() reaches _launch() and honors
            # pending_cancel).

    def retry(self, transfer_id: str) -> None:
        """Re-arm a FAILED transfer's job so the next ACTIVE promotion
        rebuilds and reruns its engine (a fresh TRANSFER_CREATE
        handshake re-establishes the real resume offset)."""
        with self._lock:
            job = self._jobs.get(transfer_id)
        if job is None:
            raise TransferNotFound(transfer_id)
        transfer = self._manager.get(transfer_id)
        if not job.initiator:
            raise TransferStateError('only the initiating peer can retry this transfer')
        if transfer.state != TransferState.FAILED:
            raise TransferStateError('only a failed transfer can retry')
        if not transfer.io_finished.wait(5):
            raise TransferStateError('previous attempt is still stopping; retry later')
        if job.thread is not None:
            job.thread.join(timeout=1)
            if job.thread.is_alive():
                raise TransferStateError('previous attempt is still stopping; retry later')
        with transfer.lock:
            if transfer.state != TransferState.FAILED or not transfer.ready:
                raise TransferStateError('transfer changed while preparing retry')
            transfer.ready = False
            with self._lock:
                job.engine = None
                job.thread = None
                job.pending_pause = job.pending_cancel = False
        try:
            self._manager.retry_transfer(transfer_id)
        finally:
            with transfer.lock:
                transfer.ready = True
        self._manager.schedule()

    # ── bulk actions: global (apply-to-every-transfer) actions ────────────
    #
    # Each of these is a thin loop over the SAME per-transfer method
    # above (self.cancel(), self.pause(), ...) -- never
    # self._manager.*_transfer() directly. That distinction matters:
    # TransferManager.cancel_all() (see manager.py) only flips each
    # Transfer's state and has no idea an engine thread might be
    # live for it -- it never sets that engine's _cancel_event, never
    # sends TRANSFER_CANCEL to the peer, and never cleans up a .part
    # or frontend queue scratch file. That's fine for a bare TransferManager
    # used without a scheduler (its own tests), but silently wrong the
    # instant a real connection is attached, which every one of
    # these always has. Going through self.cancel()/pause()/etc. here
    # is what makes these actually safe to expose to a "Cancel all" /
    # "Pause all" button.
    #
    # Each one is a best-effort sweep, not an all-or-nothing batch: a
    # per-transfer race (it reached a terminal state, or moved out of
    # the state being targeted, between the snapshot and the call) is
    # expected under concurrency and is silently skipped rather than
    # aborting the whole sweep or raising. The return value is how
    # many transfers the action was actually applied to.

    def cancel_all(self) -> int:
        """Cancel every non-terminal transfer this scheduler knows
        about. Mirrors TransferManager.cancel_all()'s role (a "stop
        everything" button, or cleanup before tearing the connection
        down) but engine-aware -- see the note above."""
        count = 0
        for t in self._manager.all_transfers():
            if t.is_terminal:
                continue
            try:
                self.cancel(t.id)
                count += 1
            except (TransferNotFound, TransferStateError):
                pass
        log.info("cancel_all: cancelled %d transfer(s)", count)
        return count

    def pause_all(self) -> int:
        """Pause every transfer currently QUEUED or ACTIVE. A transfer
        already PAUSED/PAUSING, or in any terminal state, is silently
        left alone -- not an error, just nothing to do."""
        count = 0
        for t in self._manager.all_transfers():
            if t.state not in (TransferState.QUEUED, TransferState.ACTIVE):
                continue
            try:
                self.pause(t.id)
                count += 1
            except (TransferNotFound, TransferStateError):
                pass
        log.info("pause_all: paused %d transfer(s)", count)
        return count

    def resume_all(self) -> int:
        """Resume every transfer currently PAUSED -- and also PAUSING,
        deliberately: pause_all() returning success only means pause
        was requested for each transfer, not that every one of them
        has already reached PAUSED (that requires the engine to land
        on a safe checkpoint first, which is asynchronous — see
        transfer.py's PAUSING state). resume() itself has no state
        gate of its own; it just arms the engine's resume signal
        unconditionally (see engine.py's resume(), by design, for
        exactly this kind of race), which is honored the moment that
        engine does reach PAUSED. Restricting this sweep to PAUSED
        only would silently skip a transfer still finishing its pause
        at the exact moment this runs — precisely the sequence a user
        clicking "Resume all" shortly after "Pause all" can trigger —
        leaving it stranded in PAUSED with nothing left to resume it.

        A passive-dormant transfer (see resume()'s own docstring --
        paused by the peer after a restart, only the peer's
        reconnection can resume it) is skipped like any other transfer
        that isn't this sweep's to act on, not treated as a failure.
        """
        count = 0
        for t in self._manager.all_transfers():
            if t.state not in (TransferState.PAUSED, TransferState.PAUSING):
                continue
            try:
                self.resume(t.id)
                count += 1
            except (TransferNotFound, TransferStateError):
                pass
        log.info("resume_all: resumed %d transfer(s)", count)
        return count

    def retry_all(self) -> int:
        """Retry every transfer currently FAILED."""
        count = 0
        for t in self._manager.all_transfers():
            if t.state != TransferState.FAILED:
                continue
            try:
                self.retry(t.id)
                count += 1
            except (TransferNotFound, TransferStateError):
                pass
        log.info("retry_all: retried %d transfer(s)", count)
        return count

    def send_window_update(self, transfer_id: str, new_capacity: int, scope='global') -> None:
        """
        Change the receiver's global capacity, or just this transfer's
        capacity with scope='transfer', and advertise the new limit.
        Subsequent automatic refreshes preserve the explicit setting.
        """
        with self._lock:
            job = self._jobs.get(transfer_id)
            engine = job.engine if job is not None else None
        if not isinstance(engine, RecvEngine):
            raise TransferNotFound(
                f"no active inbound transfer {transfer_id[:8]} to send a "
                f"WINDOW_UPDATE through"
            )
        validate_capacity(new_capacity)
        if scope not in ('global', 'transfer'):
            raise ValueError('invalid window scope')
        if scope == 'transfer':
            require_feature(self._session, 'scoped_flow_control')
            self._receive_limits(transfer_id)
            self._receive_transfers[transfer_id].resize(new_capacity)
        else:
            self._receive_global.resize(new_capacity)
        engine.send_window_update(new_capacity, scope=scope)

    # ── the actual bridge: state change -> thread start ──────────────

    def _on_state_change(
        self, transfer: Transfer, old: TransferState, new: TransferState
    ) -> None:
        if new == TransferState.FAILED:
            with self._lock:
                failed_job = self._jobs.get(transfer.id)
                failed_engine = failed_job.engine if failed_job is not None else None
                external = failed_job is not None and failed_job.thread is not threading.current_thread()
                if failed_engine is not None:
                    failed_engine._cancel_event.set()
                    failed_engine._resume_event.set()
            if external and failed_engine is not None:
                def notify_failure():
                    try:
                        self._session.send_transfer_error(transfer.id, transfer.error or 'transfer failed')
                    except Exception:
                        pass
                threading.Thread(target=notify_failure, daemon=True, name='TransferFailure-notify').start()
        if new == TransferState.CANCELLED:
            if getattr(transfer, 'initiator', False):
                transfer.cancel_pending = True
            # The manager is the lifecycle authority even when a caller uses
            # it directly. Signal I/O without reacquiring another transfer's
            # lock or blocking this callback on socket writes.
            with self._lock:
                cancelled_job = self._jobs.get(transfer.id)
                if cancelled_job is not None:
                    cancelled_job.pending_cancel = True
                    if cancelled_job.engine is not None:
                        cancelled_job.engine._cancel_event.set()
                        cancelled_job.engine._resume_event.set()
            def notify_cancel():
                try:
                    self._session.send_transfer_cancel(transfer.id, 'transfer cancelled')
                except Exception:
                    pass
            threading.Thread(target=notify_cancel, daemon=True, name='TransferCancel-notify').start()
        if self._persistence is not None:
            # The callback already owns transfer.lock. Commit this snapshot
            # synchronously before starting the worker, so initial job
            # metadata exists before any chunk checkpoint or ACK.
            try:
                if new in (TransferState.CANCELLED, TransferState.COMPLETED) and not self._keep_history:
                    # Not recoverable — nothing to resume, don't keep it
                    # around. Matches PersistenceStore.RECOVERABLE_STATES,
                    # which excludes both.
                    self._persistence.delete(transfer.id)
                else:
                    part_path, dest_path = self._paths.get(transfer.id, (None, None))
                    self._persistence.save_snapshot(
                        transfer.id, transfer.direction.value, transfer.filename,
                        transfer.total_size, transfer.offset, transfer.checksum,
                        new.value, transfer.error, transfer.created_at,
                        part_path=part_path, dest_path=dest_path,
                        metadata=transfer_metadata(transfer),
                        retry_count=transfer.attempt,
                    )
            except Exception as exc:
                transfer.persistence_error = str(exc)
                log.exception("Persistence update failed for %s", transfer.id[:8])
                if new == TransferState.ACTIVE:
                    threading.Thread(target=self._manager.fail_transfer,
                                     args=(transfer.id, f'persistence unavailable: {exc}'), daemon=True).start()
                    return

        if new in (TransferState.CANCELLED, TransferState.COMPLETED) and getattr(transfer, 'source_owned', False):
            def cleanup_owned_source():
                transfer.io_finished.wait()
                try:
                    os.remove(transfer.source_path)
                except FileNotFoundError:
                    pass
                except OSError:
                    log.exception('Could not remove owned upload staging file')
            threading.Thread(target=cleanup_owned_source, daemon=True, name='OwnedSource-cleanup').start()

        if new != TransferState.ACTIVE:
            if new in (TransferState.CANCELLED, TransferState.COMPLETED):
                # Terminal (and not going to be retried) — drop the
                # bookkeeping. FAILED is deliberately excluded: its
                # job stays until retry() re-arms it or the caller
                # gives up on it (nothing here ever discards a FAILED
                # job on its own).
                with self._lock:
                    self._jobs.pop(transfer.id, None)
                    self._paths.pop(transfer.id, None)
                    self._transfer_windows.pop(transfer.id, None)
                    self._receive_transfers.pop(transfer.id, None)
            return

        with self._lock:
            job = self._jobs.get(transfer.id)
            if job is None:
                log.warning(
                    "Transfer %s went ACTIVE with no registered job — "
                    "was it created outside this scheduler?", transfer.id[:8],
                )
                return
            if job.thread is not None and job.thread.is_alive():
                return  # already running — this is a resume, not a fresh start

        # This callback runs synchronously inside Transfer._lock (see
        # Transfer.set_state) — anything slower than starting a thread
        # would hold that lock for the entire transfer. build()/run()
        # happen only inside the new thread.
        def _run_job() -> None:
            # Snapshot: attach_session() may rebind self._router while
            # this thread is still finishing — the unregister at the
            # bottom must hit the router this engine actually used.
            router = self._router
            session = self._session
            try:
                engine = job.build()
            except Exception as e:
                if self._session.is_lost:
                    # Started while the link was down (e.g. a QUEUED
                    # transfer promoted mid-outage): not a failure —
                    # wait for recovery.
                    log.warning("Engine setup for %s hit a dead connection: %s",
                                transfer.id[:8], e)
                    try:
                        self._manager.mark_connection_lost(transfer.id)
                    except TransferNotFound:
                        pass
                    return
                log.exception("Failed to build engine for %s", transfer.id[:8])
                try:
                    self._manager.fail_transfer(transfer.id, f"engine setup failed: {e}")
                except (TransferNotFound, TransferStateError):
                    pass
                return
            if engine is None:
                # build() finished the transfer itself (AlreadyComplete).
                router.unregister(transfer.id)
                return
            with self._lock:
                job.engine = engine
                do_cancel = job.pending_cancel
                do_pause  = job.pending_pause and not do_cancel
                job.pending_cancel = False
                job.pending_pause = False
            # Loss can be declared while build() is still constructing an
            # engine. That engine was absent from the loss listener's snapshot.
            # Publish it first, then check the captured session: either this
            # check or the listener must see and stop it before blocking on I/O.
            if session.is_lost:
                engine.abort_connection_lost()
                try:
                    self._manager.mark_connection_lost(transfer.id)
                except TransferNotFound:
                    pass
                router.unregister(transfer.id)
                return
            # A pause()/cancel() that arrived while build() was still
            # doing its handshake couldn't reach an engine that didn't
            # exist yet — apply it now, before run() starts consuming
            # frames, so it isn't lost. Cancel wins if somehow both
            # arrived (cancel() is always the more final of the two).
            if do_cancel:
                engine.cancel()
            elif do_pause:
                engine.pause()
            try:
                engine.run()
            finally:
                router.unregister(transfer.id)

        def _launch():
            try:
                _run_job()
            finally:
                transfer.io_finished.set()
                with self._lock:
                    self._worker_threads.discard(threading.current_thread())

        transfer.io_finished.clear()
        t = threading.Thread(
            target=_launch, daemon=True, name=f"Engine-{transfer.id[:8]}"
        )
        with self._lock:
            job.thread = t
            self._worker_threads.add(t)
        t.start()

    def update_settings(
        self,
        max_upload_mb: Optional[float] = None,
        blocked_extensions: Optional[str] = None,
    ) -> None:
        """API integration: host-side runtime settings, same semantics as the
        old FileTransferServer.update_settings() — max_upload_mb=0 (or
        omitted-as-0) means unlimited; blocked_extensions is a
        comma-separated string (".exe,.bat")."""
        if max_upload_mb is not None:
            self._max_upload_size = int(max_upload_mb * 1024 * 1024) if max_upload_mb > 0 else 0
        if blocked_extensions is not None:
            self._blocked_extensions = {
                ("." + e.strip().lstrip(".").lower())
                for e in blocked_extensions.split(",") if e.strip()
            }

    # ── recovery: connection loss and recovery ───────────────────────

    def _bind_router_loss(self, router: FrameRouter) -> None:
        router.add_window_listener(lambda frame, r=router: self._on_window_update(frame) if r is self._router else None)
        router.add_cancel_ack_listener(lambda tid, r=router: self._on_cancel_ack(tid) if r is self._router else None)
        router.add_cancel_listener(self._on_remote_cancel)
        # Bound to THIS router: a late notification from a previous,
        # already-replaced router must not tear down the new session's
        # engines (see _on_router_lost).
        router.add_loss_listener(lambda r=router: self._on_router_lost(r))

    def _on_cancel_ack(self, transfer_id):
        try:
            transfer = self._manager.get(transfer_id)
        except TransferNotFound:
            return
        if transfer.state == TransferState.CANCELLED:
            with transfer.lock:
                transfer.cancel_pending = False
                if self._persistence is not None and self._keep_history:
                    self._persistence.save(transfer, part_path=transfer.part_path,
                                           dest_path=transfer.dest_path, retry_count=transfer.attempt)

    def _on_remote_cancel(self, transfer_id):
        try:
            self._manager.get(transfer_id)
        except TransferNotFound:
            return False
        self.cancel(transfer_id, notify_peer=False)
        return True

    def _on_router_lost(self, router: FrameRouter) -> None:
        if router is not self._router:
            return
        self._handle_connection_loss()

    def _handle_connection_loss(self) -> None:
        """Stop every engine (without cancelling anything), move every
        running transfer to CONNECTION_LOST, and prepare jobs for
        recovery. The session stays a session-level concern: no
        transfer is FAILED or CANCELLED by a link outage."""
        log.warning("ConcurrentScheduler: connection lost — suspending transfers")
        self._manager.suspended = True
        with self._lock:
            jobs = list(self._jobs.items())
        for _tid, job in jobs:
            if job.engine is not None:
                job.engine.abort_connection_lost()

        for t in self._manager.all_transfers():
            release_destination(t)
            try:
                self._manager.mark_connection_lost(t.id)
            except TransferNotFound:
                pass

        with self._lock:
            for tid, job in list(self._jobs.items()):
                try:
                    st = self._manager.get(tid).state
                except TransferNotFound:
                    continue
                if st not in (TransferState.CONNECTION_LOST, TransferState.PAUSED):
                    continue        # QUEUED etc.: build() will run on the new session
                if job.initiator:
                    # Rebuildable: recover_transfer() re-runs build().
                    job.engine = None
                    job.thread = None
                    job.pending_pause = False
                else:
                    # Passive: only the peer's re-CREATE can revive it.
                    self._jobs.pop(tid, None)
                    if st == TransferState.PAUSED:
                        self._passive_dormant.add(tid)

    def _job_threads(self) -> list[threading.Thread]:
        with self._lock:
            return [thread for thread in self._worker_threads if thread.is_alive()]

    def attach_session(
        self,
        session: Session,
        router: FrameRouter,
        recover: bool = True,
        keepalive: Optional[tuple[float, float]] = None,
    ) -> list[str]:
        """
        Bind this scheduler (and its TransferManager, with all its
        transfers) to a NEW, already-negotiated session after the old
        one died. `router` must be a fresh FrameRouter on `session`;
        this starts its reader.

        Order matters: the old connection is first declared lost (if it
        hasn't been yet) and its engine threads are joined, so nothing
        from the old incarnation can touch a transfer once it is
        re-queued; only then is the new session bound. Returns the IDs
        of transfers it re-queued (initiator side, when recover=True).
        `keepalive=(interval, timeout)` turns on half-open detection
        for the new router.
        """
        old_router, old_session = self._router, self._session
        was_accepting = self._accept_thread is not None

        if not self.stop_accept_loop():
            raise TransferStateError('old inbound worker has not stopped; reconnect again later')
        if not old_router.connection_lost.is_set():
            old_router.declare_lost("superseded by a new session")
        self._handle_connection_loss()    # idempotent; ensures state is settled
        old_router.stop()
        for th in self._job_threads():
            if th is not threading.current_thread():
                th.join(timeout=5)
            if th.is_alive():
                raise TransferStateError('old transfer worker has not stopped; reconnect again later')
        old_session.close_session("replaced by a new session")

        self._session = session
        self._router = router
        self._transfer_windows.clear()
        self._send_window = GlobalWindow(self._send_window.capacity)
        self._bind_router_loss(router)
        router.start_reader()
        if keepalive is not None:
            router.start_keepalive(*keepalive)
        if was_accepting:
            self.start_accept_loop()
        self._manager.suspended = False
        log.info("ConcurrentScheduler: attached new session %s", session.peer_addr)
        recovered = self.recover_all() if recover else []
        self._manager.schedule()
        return recovered

    def recover_transfer(self, transfer_id: str) -> None:
        """Re-queue one CONNECTION_LOST transfer THIS side initiated.
        Its job re-runs the TRANSFER_CREATE handshake with the same
        transfer ID; the peer answers with the offset it holds, and the
        transfer continues from there."""
        with self._lock:
            job = self._jobs.get(transfer_id)
        if job is None or not job.initiator:
            raise TransferStateError(
                "not recoverable from this side: the peer initiated it and "
                "must re-send TRANSFER_CREATE after reconnecting"
            )
        if self._session.is_lost:
            raise TransferStateError("cannot recover: no live session attached")
        with self._lock:
            job.engine = None
            job.thread = None
        if not self._manager.get(transfer_id).io_finished.wait(5):
            raise TransferStateError('old transfer worker has not stopped')
        self._manager.requeue_transfer(transfer_id)

    def recover_all(self) -> list[str]:
        """Re-queue every CONNECTION_LOST transfer this side initiated."""
        recovered = []
        for t in self._manager.all_transfers():
            with t.lock:
                st = t.state
            if st != TransferState.CONNECTION_LOST:
                continue
            try:
                self.recover_transfer(t.id)
                recovered.append(t.id)
            except TransferStateError:
                pass     # passive (waits for the peer) — expected
        return recovered

    # ── introspection ─────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return self._manager.to_dict()
