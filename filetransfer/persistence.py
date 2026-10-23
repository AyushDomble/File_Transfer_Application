"""
persistence — Persistent transfer metadata, wired into the running app.

Stores per-transfer state in a SQLite database so that transfers can
survive application restarts, disconnections, and crashes.

Every time a transfer makes meaningful progress — or its state changes —
the engine calls PersistenceStore.save(). On startup the application
restores saved queues, paused intent and terminal receipts. Recovery requires
a fresh authenticated, approved connection; PINs are never persisted.

Schema
──────
CREATE TABLE transfers (
    id           TEXT PRIMARY KEY,   -- transfer_id (uuid4 hex)
    direction    TEXT NOT NULL,       -- 'upload' | 'download'
    filename     TEXT NOT NULL,
    total_size   INTEGER NOT NULL,
    offset       INTEGER NOT NULL,    -- last confirmed byte offset
    checksum     TEXT,                -- whole-file SHA-256, NULL until verified
    chunk_sha256 TEXT,                -- last successful chunk SHA-256
    state        TEXT NOT NULL,       -- TransferState value string
    error        TEXT,                -- last error message, if any
    part_path    TEXT,                -- absolute path of the .part file
    dest_path    TEXT,                -- absolute path of the final file
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL,
    retry_count  INTEGER NOT NULL DEFAULT 0,
    metadata     TEXT                -- migrated recovery/job metadata
);

Thread-safety
─────────────
SQLite WAL and synchronous=FULL are enabled. Methods serialize through a
lock and reuse one connection per store, including across worker threads.
Each write still commits before returning. Peer/endpoint scopes have separate journals.

Usage
─────
    store = PersistenceStore("~/.filetransfer/transfers.db")

    # Save a transfer record (call after each state change or progress update)
    store.save(transfer, part_path="/tmp/movie.mp4.part", dest_path="/files/movie.mp4")

    # Load all non-terminal transfers on startup
    rows = store.load_interrupted()
    for row in rows:
        print(row["id"], row["state"], row["offset"])

    # Delete a completed or cancelled transfer record
    store.delete(transfer_id)
"""

from __future__ import annotations

import os
import hashlib
import json
from contextlib import contextmanager
import sqlite3
import threading
import time
import weakref
from typing import Optional

from .logsetup import get_logger
from .transfer import Transfer, TransferDirection, TransferState, TERMINAL_STATES

log = get_logger("filetransfer.persistence")


# States that are persisted as "recoverable" — the user may want to resume these.
RECOVERABLE_STATES: frozenset[TransferState] = frozenset({
    TransferState.QUEUED,
    TransferState.ACTIVE,
    TransferState.PAUSING,
    TransferState.PAUSED,
    TransferState.FAILED,
    TransferState.RETRYING,
    TransferState.CONNECTION_LOST,
})

_SCHEMA = """
CREATE TABLE IF NOT EXISTS transfers (
    id           TEXT    PRIMARY KEY,
    direction    TEXT    NOT NULL,
    filename     TEXT    NOT NULL,
    total_size   INTEGER NOT NULL,
    offset       INTEGER NOT NULL DEFAULT 0,
    checksum     TEXT,
    chunk_sha256 TEXT,
    state        TEXT    NOT NULL,
    error        TEXT,
    part_path    TEXT,
    dest_path    TEXT,
    created_at   REAL    NOT NULL,
    updated_at   REAL    NOT NULL,
    retry_count  INTEGER NOT NULL DEFAULT 0
);
"""


def transfer_metadata(transfer):
    return dict(initiator=getattr(transfer, 'initiator', False),
                source_path=getattr(transfer, 'source_path', None),
                chunk_size=getattr(transfer, 'chunk_size', 1024 * 1024),
                overwrite=transfer.overwrite,
                source_owned=getattr(transfer, 'source_owned', False),
                batch_id=getattr(transfer, 'batch_id', None),
                restart_required=getattr(transfer, '_restart_required', False),
                cancel_pending=getattr(transfer, 'cancel_pending', False),
                attempt=transfer.attempt)


def save_transfer(store, transfer):
    with transfer.lock:
        store.save(transfer, part_path=transfer.part_path, dest_path=transfer.dest_path,
                   retry_count=transfer.attempt)


def default_store(role, directory):
    from . import config
    root = config.STATE_DIR or os.path.join(os.path.abspath(directory), '.ft-state')
    return PersistenceStore(os.path.join(root, role + '.sqlite3'))


class PersistenceStore:
    """
    SQLite-backed store for transfer state.

    One instance per application; shared across all transfer engines via
    dependency injection.
    """

    def __init__(self, db_path: str) -> None:
        """
        db_path — absolute or ~ path to the SQLite file.
        The parent directory is created automatically.
        """
        self._db_path = os.path.abspath(os.path.expanduser(db_path))
        if os.name == 'nt' and not self._db_path.startswith('\\\\?\\'):
            self._db_path = ('\\\\?\\UNC\\' + self._db_path[2:] if self._db_path.startswith('\\\\')
                             else '\\\\?\\' + self._db_path)
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        self._lock = threading.Lock()
        self._connection = None
        try:
            self._init_db()
        except Exception:
            if self._connection is not None:
                self._connection.close()
            raise
        # All access is serialized by _lock. Keep the connection and WAL open;
        # FULL commits are unchanged, only connection/PRAGMA setup is amortized.
        self._anchor = self._connection
        self._anchor_finalizer = weakref.finalize(self, self._anchor.close)

    # ── Internals ─────────────────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        """Get the store connection. Caller must hold _lock."""
        if self._connection is not None:
            return self._connection
        # SQLite's Windows VFS also opens -wal/-shm files. Use the extended
        # absolute path so long install/profile directories remain usable.
        conn = sqlite3.connect(self._db_path, timeout=10, check_same_thread=False)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
        except Exception:
            conn.close()
            raise
        self._connection = conn
        return conn

    @staticmethod
    def _release(conn):
        # Never leak an unsuccessful transaction into the next operation.
        # Successful callers already committed (or used `with conn`).
        if conn.in_transaction:
            conn.rollback()

    def _init_db(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(_SCHEMA)
                conn.execute('BEGIN IMMEDIATE')
                columns = {row['name'] for row in conn.execute('PRAGMA table_info(transfers)')}
                if 'metadata' not in columns:
                    conn.execute('ALTER TABLE transfers ADD COLUMN metadata TEXT')
                conn.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
                conn.execute('CREATE TABLE IF NOT EXISTS scopes (name TEXT PRIMARY KEY)')
                conn.commit()
            finally:
                self._release(conn)
        log.debug("PersistenceStore ready: %s", self._db_path)

    def scoped(self, name: str):
        """Separate journals prevent chosen IDs from crossing peer/role boundaries."""
        with self._lock:
            with self._transaction() as conn:
                conn.execute('INSERT OR IGNORE INTO scopes(name) VALUES (?)', (name,))
        digest = hashlib.sha256(name.encode()).hexdigest()
        return PersistenceStore(os.path.join(self._db_path + '.d', digest + '.sqlite3'))

    def list_scopes(self, prefix=''):
        with self._lock:
            with self._transaction() as conn:
                return [row['name'] for row in conn.execute('SELECT name FROM scopes ORDER BY name')
                        if row['name'].startswith(prefix)]

    def get_setting(self, key, default=None):
        with self._lock:
            with self._transaction() as conn:
                row = conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
                return json.loads(row['value']) if row else default

    def set_setting(self, key, value):
        with self._lock:
            with self._transaction() as conn:
                conn.execute('INSERT INTO settings VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                             (key, json.dumps(value)))

    def get_or_create_setting(self, key, factory):
        with self._lock:
            with self._transaction() as conn:
                conn.execute('BEGIN IMMEDIATE')
                row = conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
                if row:
                    return json.loads(row['value'])
                value = factory()
                conn.execute('INSERT INTO settings VALUES (?,?)', (key, json.dumps(value)))
                return value

    @contextmanager
    def _transaction(self):
        conn = self._connect()
        try:
            with conn:
                yield conn
        finally:
            self._release(conn)

    @staticmethod
    def _record(row):
        if row is None:
            return None
        result = dict(row)
        try:
            metadata = json.loads(result.pop('metadata', None) or '{}')
            if not isinstance(metadata, dict):
                raise ValueError('metadata is not an object')
        except (ValueError, TypeError):
            metadata = {}
            result['state'], result['error'] = 'failed', 'invalid persisted recovery metadata'
        for key in ('initiator', 'source_path', 'chunk_size', 'overwrite', 'source_owned', 'batch_id',
                    'restart_required', 'attempt', 'cancel_pending'):
            if key in metadata:
                result[key] = metadata[key]
        return result

    # ── Public API ────────────────────────────────────────────────────────

    def save(
        self,
        transfer: Transfer,
        part_path: Optional[str] = None,
        dest_path: Optional[str] = None,
        chunk_sha256: Optional[str] = None,
        retry_count: int = 0,
    ) -> None:
        """
        Insert or replace the transfer record.

        Call this:
          - whenever transfer.state changes
          - after each chunk ACK (to update offset)
          - when the whole-file checksum is confirmed

        Thread-safe and synchronous: returns only after the commit.

        Locks `transfer` itself to snapshot its fields consistently.
        Transfer uses an RLock, so state listeners can also checkpoint
        safely. save_snapshot() accepts fields already captured by a caller.
        """
        with transfer.lock:
            tid        = transfer.id
            direction  = transfer.direction.value
            filename   = transfer.filename
            total_size = transfer.total_size
            offset     = transfer.offset
            checksum   = transfer.checksum
            state      = transfer.state.value
            error      = transfer.error
            created_at = transfer.created_at

            # Serialize snapshot AND commit against lifecycle transitions.
            # Otherwise a delayed progress write could overwrite a newer
            # cancellation/completion checkpoint and resurrect it on restart.
            self.save_snapshot(
                tid, direction, filename, total_size, offset, checksum,
                state, error, created_at, part_path=part_path,
                dest_path=dest_path, chunk_sha256=chunk_sha256,
                retry_count=retry_count,
                metadata=transfer_metadata(transfer),
            )

    def save_snapshot(
        self,
        tid: str,
        direction: str,
        filename: str,
        total_size: int,
        offset: int,
        checksum: Optional[str],
        state: str,
        error: Optional[str],
        created_at: float,
        part_path: Optional[str] = None,
        dest_path: Optional[str] = None,
        chunk_sha256: Optional[str] = None,
        retry_count: int = 0,
        metadata: Optional[dict] = None,
    ) -> None:
        """
        Insert or replace the transfer record from already-extracted
        fields — the part of save() that actually touches SQLite, with
        no Transfer object and no transfer.lock involved. Use this
        directly (instead of save()) from a context that already has
        safe access to a transfer's fields without needing to lock it
        again — most notably a manager state-change listener, which
        runs with transfer.lock already held by the caller.
        """
        now = time.time()

        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    INSERT INTO transfers
                        (id, direction, filename, total_size, offset,
                         checksum, chunk_sha256, state, error,
                         part_path, dest_path, created_at, updated_at,
                         retry_count, metadata)
                    VALUES
                        (?, ?, ?, ?, ?,
                         ?, ?, ?, ?,
                         ?, ?, ?, ?,
                         ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        -- A pulled download is created with total_size 0
                        -- (the size only arrives with the peer's ACCEPT),
                        -- so its first row has 0. Refresh it once known,
                        -- but never overwrite a known size with 0.
                        total_size   = CASE WHEN excluded.total_size > 0
                                            THEN excluded.total_size
                                            ELSE transfers.total_size END,
                        offset       = excluded.offset,
                        checksum     = excluded.checksum,
                        chunk_sha256 = excluded.chunk_sha256,
                        state        = excluded.state,
                        error        = excluded.error,
                        part_path    = excluded.part_path,
                        dest_path    = excluded.dest_path,
                        updated_at   = excluded.updated_at,
                        retry_count  = excluded.retry_count,
                        metadata     = COALESCE(excluded.metadata, transfers.metadata)
                    """,
                    (
                        tid, direction, filename, total_size, offset,
                        checksum, chunk_sha256, state, error,
                        part_path, dest_path, created_at, now,
                        retry_count, json.dumps(metadata) if metadata is not None else None,
                    ),
                )
                conn.commit()
                log.debug("Persisted transfer %s state=%s offset=%d",
                          tid[:8], state, offset)
            except sqlite3.Error as e:
                log.error("PersistenceStore.save failed for %s: %s", tid[:8], e)
                raise
            finally:
                self._release(conn)

    def update_offset(self, transfer_id: str, offset: int,
                      chunk_sha256: Optional[str] = None) -> None:
        """
        Lightweight progress update — only touches offset and updated_at.
        Cheaper than a full save(); call after each ACK.
        """
        now = time.time()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    UPDATE transfers
                       SET offset       = ?,
                           chunk_sha256 = COALESCE(?, chunk_sha256),
                           updated_at   = ?
                     WHERE id = ?
                    """,
                    (offset, chunk_sha256, now, transfer_id),
                )
                conn.commit()
            except sqlite3.Error as e:
                log.error("PersistenceStore.update_offset failed for %s: %s",
                          transfer_id[:8], e)
                raise
            finally:
                self._release(conn)

    def update_state(self, transfer_id: str, state: TransferState,
                     error: Optional[str] = None) -> None:
        """Update only the state (and optional error) for a transfer."""
        now = time.time()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    UPDATE transfers
                       SET state      = ?,
                           error      = ?,
                           updated_at = ?
                     WHERE id = ?
                    """,
                    (state.value, error, now, transfer_id),
                )
                conn.commit()
                log.debug("Persisted state change %s → %s",
                          transfer_id[:8], state.value)
            except sqlite3.Error as e:
                log.error("PersistenceStore.update_state failed for %s: %s",
                          transfer_id[:8], e)
            finally:
                self._release(conn)

    def load(self, transfer_id: str) -> Optional[dict]:
        """
        Load one transfer record by ID.
        Returns a plain dict (sqlite3.Row fields) or None if not found.
        """
        with self._lock:
            conn = self._connect()
            try:
                row = conn.execute(
                    "SELECT * FROM transfers WHERE id = ?", (transfer_id,)
                ).fetchone()
                return self._record(row)
            except sqlite3.Error as e:
                log.error("PersistenceStore.load failed for %s: %s",
                          transfer_id[:8], e)
                raise
            finally:
                self._release(conn)

    def load_all(self) -> list[dict]:
        """Return all transfer records, newest first."""
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    "SELECT * FROM transfers ORDER BY updated_at DESC"
                ).fetchall()
                return [self._record(r) for r in rows]
            except sqlite3.Error as e:
                log.error("PersistenceStore.load_all failed: %s", e)
                raise
            finally:
                self._release(conn)

    def load_interrupted(self) -> list[dict]:
        """
        Return transfers in recoverable states (not yet terminal).
        These are the candidates for resumption after a restart.
        Ordered by updated_at descending (most recent first).
        """
        placeholders = ",".join("?" * len(RECOVERABLE_STATES))
        values = [s.value for s in RECOVERABLE_STATES]
        with self._lock:
            conn = self._connect()
            try:
                rows = conn.execute(
                    f"""
                    SELECT * FROM transfers
                     WHERE state IN ({placeholders})
                     ORDER BY updated_at DESC
                    """,
                    values,
                ).fetchall()
                return [self._record(r) for r in rows]
            except sqlite3.Error as e:
                log.error("PersistenceStore.load_interrupted failed: %s", e)
                raise
            finally:
                self._release(conn)

    def delete(self, transfer_id: str) -> None:
        """Remove a transfer record (e.g. after verified completion)."""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    "DELETE FROM transfers WHERE id = ?", (transfer_id,)
                )
                conn.commit()
                log.debug("Deleted persisted transfer %s", transfer_id[:8])
            except sqlite3.Error as e:
                log.error("PersistenceStore.delete failed for %s: %s",
                          transfer_id[:8], e)
            finally:
                self._release(conn)

    def delete_terminal(self) -> int:
        """
        Delete all records in terminal states (COMPLETED, CANCELLED, FAILED).
        Returns the number of rows deleted.
        Useful for periodic housekeeping.
        """
        terminal_values = [s.value for s in TERMINAL_STATES]
        placeholders = ",".join("?" * len(terminal_values))
        with self._lock:
            conn = self._connect()
            try:
                cur = conn.execute(
                    f"DELETE FROM transfers WHERE state IN ({placeholders})",
                    terminal_values,
                )
                conn.commit()
                n = cur.rowcount
                if n:
                    log.info("PersistenceStore: deleted %d terminal transfer(s)", n)
                return n
            except sqlite3.Error as e:
                log.error("PersistenceStore.delete_terminal failed: %s", e)
                return 0
            finally:
                self._release(conn)

    def increment_retry(self, transfer_id: str) -> int:
        """
        Increment and return the retry_count for a transfer.
        Used by the engine to track how many times a transfer has been retried.
        """
        now = time.time()
        with self._lock:
            conn = self._connect()
            try:
                conn.execute(
                    """
                    UPDATE transfers
                       SET retry_count = retry_count + 1,
                           updated_at  = ?
                     WHERE id = ?
                    """,
                    (now, transfer_id),
                )
                conn.commit()
                row = conn.execute(
                    "SELECT retry_count FROM transfers WHERE id = ?",
                    (transfer_id,),
                ).fetchone()
                return row["retry_count"] if row else 0
            except sqlite3.Error as e:
                log.error("PersistenceStore.increment_retry failed for %s: %s",
                          transfer_id[:8], e)
                return 0
            finally:
                self._release(conn)

    def path(self) -> str:
        """Return the absolute path of the database file."""
        return self._db_path

    def __repr__(self) -> str:
        return f"PersistenceStore({self._db_path!r})"


# ── Integration helpers ───────────────────────────────────────────────────────

def make_persistence_hook(store: PersistenceStore,
                          part_path: str,
                          dest_path: str):
    """
    Return a state-change listener compatible with TransferManager.add_listener().

    Wires the store into the manager so every state transition is
    automatically persisted without the engine needing to know about
    the store directly.

        store = PersistenceStore("~/.filetransfer/transfers.db")
        mgr   = TransferManager()
        mgr.add_listener(make_persistence_hook(store, part_path, dest_path))

    The listener is called with (transfer, old_state, new_state) by the
    manager after every set_state() call.
    """
    def _hook(transfer: Transfer, old: TransferState, new: TransferState) -> None:
        store.save(
            transfer,
            part_path=part_path,
            dest_path=dest_path,
        )

    return _hook


def restore_transfer(row: dict, manager, connection_lost: bool = False,
                     include_terminal: bool = False) -> Optional[Transfer]:
    """
    Reconstruct a Transfer object from a persisted row and re-register it
    with the manager.

    By default restore dormant PAUSED state and skip terminal history.
    connection_lost=True restores nonterminal work as CONNECTION_LOST;
    include_terminal=True retains terminal receipts and failed retry jobs.

    Returns the restored Transfer or None if the row should be skipped.
    """
    state = TransferState(row["state"])
    if state in TERMINAL_STATES and not include_terminal:
        log.debug("Skipping terminal transfer %s (%s)",
                  row["id"][:8], state.value)
        return None

    direction = TransferDirection(row["direction"])
    # transfer_id=row["id"] constructs the Transfer with its original ID
    # directly. Atomic identity registration prevents concurrent scheduling
    # from promoting a transfer before its final lookup key is available.
    # start_immediately=False for the same reason — this function
    # drives the transfer through ACTIVE -> PAUSING -> PAUSED itself
    # immediately below; nothing should race ahead of that.
    transfer = manager.create_transfer(
        direction  = direction,
        filename   = row["filename"],
        total_size = row["total_size"],
        checksum   = row.get("checksum"),
        transfer_id = row["id"],
        start_immediately = False,
        ready = False,
    )

    # Restore offset
    transfer.update_progress(row["offset"])
    transfer.part_path = row.get('part_path')
    transfer.dest_path = row.get('dest_path')
    transfer.overwrite = row.get('overwrite', False)
    transfer.initiator = row.get('initiator', False)
    transfer.source_path = row.get('source_path')
    transfer.source_owned = row.get('source_owned', False)
    transfer.batch_id = row.get('batch_id')
    transfer.chunk_size = row.get('chunk_size', 1024 * 1024)
    transfer.attempt = row.get('attempt', row.get('retry_count', 0))
    transfer._restart_required = row.get('restart_required', False)
    transfer.cancel_pending = row.get('cancel_pending', False)
    if include_terminal and state in TERMINAL_STATES:
        # Reconstruct history/tombstones, never run through a fake ACTIVE
        # transition or trigger an engine to rebuild a completed transfer.
        with transfer.lock:
            transfer.state = state
            transfer.error = row.get('error')
            transfer.ready = True
        return transfer

    # Restore a dormant transfer without temporarily consuming an active
    # slot (which may already be occupied by a live transfer).
    try:
        manager.pause_transfer(transfer.id)    # QUEUED → PAUSED
        if connection_lost:
            # recovery: restart-plus-reconnect. The peer link no longer
            # exists, so the transfer waits as CONNECTION_LOST for a
            # new session (initiator: scheduler.reattach_* + recover;
            # passive side: the peer's re-CREATE re-attaches it).
            transfer.set_state(TransferState.CONNECTION_LOST)
            transfer.ready = True
            log.info("Restored transfer %s %r offset=%d → CONNECTION_LOST",
                     transfer.id[:8], transfer.filename, transfer.offset)
            return transfer
        transfer.ready = True
    except Exception as e:
        log.warning("Could not restore transfer %s to PAUSED: %s",
                    row["id"][:8], e)

    log.info(
        "Restored transfer %s %r offset=%d → PAUSED",
        transfer.id[:8], transfer.filename, transfer.offset,
    )
    return transfer


def load_transfer_records(store, manager):
    """Restore metadata first, before a scheduler/listener can overwrite it."""
    restored = []
    for row in reversed(store.load_all()):
        if row['id'] in {item.id for item in manager.all_transfers()}:
            continue
        if row['direction'] == TransferDirection.DOWNLOAD.value and row['state'] not in ('completed', 'cancelled'):
            destination = row.get('dest_path')
            if destination and os.path.isfile(destination) and row.get('checksum') and os.path.getsize(destination) == row['total_size']:
                hasher = hashlib.sha256()
                with open(destination, 'rb') as source:
                    for chunk in iter(lambda: source.read(65536), b''):
                        hasher.update(chunk)
                if hasher.hexdigest() == row['checksum']:
                    row['state'], row['offset'] = 'completed', row['total_size']
            if row['state'] != 'completed' and row.get('part_path'):
                row['offset'] = reconcile_offset(row['part_path'], row['offset'])
        item = restore_transfer(row, manager,
                                connection_lost=row['state'] != TransferState.PAUSED.value,
                                include_terminal=True)
        if item is not None:
            restored.append(item)
            if row['state'] == 'completed':
                save_transfer(store, item)
    return restored


def reconcile_offset(part_path: str, saved_offset: int) -> int:
    """
    Decide the offset a restored download may safely resume from.

    The database offset and the .part file can disagree after a crash:
    the file may be longer (chunk written, row not yet updated) or
    shorter (writes lost). Neither is trusted alone — the smaller of
    the two wins, and the file is truncated to it so whatever follows
    is exactly the bytes the receiver's hasher will re-read. A missing
    or unreadable .part means nothing survives: restart from 0.
    """
    try:
        size = os.path.getsize(part_path)
    except OSError:
        return 0
    offset = max(0, min(saved_offset, size))
    if size != offset:
        try:
            with open(part_path, "r+b") as fh:
                fh.truncate(offset)
        except OSError as e:
            log.warning("Could not truncate %s to %d: %s", part_path, offset, e)
            return 0
    return offset
