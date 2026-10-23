"""Local-only FastAPI backend connecting the desktop interface to TransferHost,
HostLink and the transfer scheduler. The UI service binds to loopback.

Uploads and downloads use TransferManager and ConcurrentScheduler, supporting
pause, resume, retry, concurrency and connection recovery. Canonical resources
live under /api/transfers, with /transfers aliases. Additional HTTP adapters
use the same transfer stack and retain their documented deprecation metadata.
ZIP packaging is implemented by batch_api, not compressed wire frames.
Snippet endpoints return unavailable/empty responses; messaging uses the chat API."""

import asyncio
import anyio
import os
import re
import threading
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing import Literal
from .events import TransferEvents

from filetransfer import config
from filetransfer.chat import ChatError
from filetransfer.discovery import discover_hosts
from filetransfer.host import TransferHost
from filetransfer.hostlink import ConnectionLost, ConnectionRejected, HostLink
from filetransfer.logsetup import add_broadcast_listener, remove_broadcast_listener, get_logger
from filetransfer.manager import TransferNotFound
from filetransfer.session_protocol import SessionError
from filetransfer.persistence import default_store
from filetransfer.netinfo import get_local_ip
from filetransfer.transfer import Transfer, TransferState, TransferStateError

log = get_logger("filetransfer.webapp")

@asynccontextmanager
async def _lifespan(application):
    await on_startup()
    try:
        yield
    finally:
        await asyncio.to_thread(on_shutdown)


app = FastAPI(lifespan=_lifespan)
from .local_security import LocalSecurity
app.add_middleware(LocalSecurity)

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

# -- shared state --
transfer_host = TransferHost(require_batch_approval=True)
client_state: dict[str, Optional[HostLink]] = {"link": None}

_ws_connections: set[WebSocket] = set()           # /api/logs  (unchanged)
_main_loop: Optional[asyncio.AbstractEventLoop] = None
_transfer_events = TransferEvents()
WS_SEND_TIMEOUT = 5


def _on_log(ts: str, level: str, name: str, message: str):
    if _main_loop is None:
        return
    payload = {"type": "log", "ts": ts, "level": level, "logger": name, "message": message}
    _schedule_broadcast(_ws_connections, payload)


def _schedule_broadcast(connections, payload):
    loop = _main_loop
    if loop is None or loop.is_closed():
        return
    coroutine = _broadcast(connections, payload)
    try:
        asyncio.run_coroutine_threadsafe(coroutine, loop)
    except RuntimeError:
        coroutine.close()  # shutdown raced this adapter log notification


async def _broadcast(connections: set, payload: dict):
    dead = []
    for ws in tuple(connections):
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        connections.discard(ws)


async def on_startup():
    global _main_loop
    _main_loop = asyncio.get_event_loop()
    add_broadcast_listener(_on_log)
    transfer_host.add_pending_listener(_on_pending_request)
    transfer_host.add_pin_listener(_on_pin_changed)
    transfer_host.add_transfer_listener(_on_host_transfer_change)
    transfer_host.add_progress_listener(_on_host_progress)
    _restore_client_state()


def _restore_client_state():
    if client_state.get('link') is not None:
        return
    store = default_store('client', config.CLIENT_DIR)
    endpoint = store.get_setting('last_endpoint')
    if endpoint:
        link = HostLink(endpoint['host'], port=endpoint['port'], use_tls=True,
                        dest_dir=config.CLIENT_DIR, persistence=store)
        _bind_client_events(link)
        client_state['link'] = link


def on_shutdown():
    global _main_loop
    _main_loop = None
    remove_broadcast_listener(_on_log)
    _transfer_events.close()
    link = client_state.get('link')
    if link:
        link.close()
    cleanup = globals().get('_cleanup_batches')
    if cleanup:
        cleanup()
    try:
        transfer_host.stop()
    except SessionError:
        # Process shutdown is final; interactive host restart remains guarded.
        log.exception('Host shutdown has unfinished workers')


def _on_pin_changed(new_pin: str):
    if _main_loop is not None:
        _schedule_broadcast(_ws_connections, {"type": "pin", "pin": new_pin})


def _on_pending_request(info: dict):
    try:
        from plyer import notification
        notification.notify(
            title="File Transfer — connection request",
            message=f"{info['ip']} wants to connect. Open the app to accept or reject.",
            timeout=10,
        )
    except Exception:
        pass  # nice-to-have; never let this break the connection flow

    if _main_loop is not None:
        _schedule_broadcast(_ws_connections, {"type": "pending", **info})


# ---------- transfer events: revisioned snapshots and structured events ----------
# State/progress come from managers for both roles, not endpoint callbacks.

_STATE_TO_EVENT = {
    TransferState.PAUSING:         "TRANSFER_PAUSING",
    TransferState.PAUSED:          "TRANSFER_PAUSED",
    TransferState.CANCELLED:       "TRANSFER_CANCELLED",
    TransferState.COMPLETED:       "TRANSFER_COMPLETED",
    TransferState.FAILED:          "TRANSFER_FAILED",
    TransferState.RETRYING:        "TRANSFER_RETRYING",
    TransferState.CONNECTION_LOST: "TRANSFER_CONNECTION_LOST",
}

# States ACTIVE/QUEUED can be reached from, where a plain STATE_TO_EVENT
# lookup would be misleading rather than just incomplete -- ACTIVE means
# something different depending on where it came from, and QUEUED means
# something different the first time than every time after.
_RESUME_SOURCES = {TransferState.PAUSED, TransferState.PAUSING}


def _event_name_for(old, new: TransferState) -> Optional[str]:
    """
    old is None exactly once per transfer: manager.create_transfer()
    calls _notify(transfer, None, transfer.state) right after
    registering it, because a transfer starts life already in QUEUED --
    there is no transition INTO that first state for set_state() to
    notice otherwise, so without this hook TRANSFER_CREATED could never
    fire at all. Every later arrival at QUEUED (recovery recovery
    re-queueing a CONNECTION_LOST transfer) is a real transition with a
    real old state, and is reported as TRANSFER_QUEUED, not a second
    "created" -- the transfer already existed.
    """
    if old is None:
        return "TRANSFER_CREATED"
    if new == TransferState.QUEUED:
        return "TRANSFER_QUEUED"
    if new == TransferState.ACTIVE:
        return "TRANSFER_RESUMED" if old in _RESUME_SOURCES else "TRANSFER_STARTED"
    return _STATE_TO_EVENT.get(new)


def _transfer_summary(t: Transfer) -> dict:
    """Common revisioned snapshot for REST and the event stream."""
    return t.to_dict()


def _snapshot_without_locking(t: Transfer, new: TransferState) -> dict:
    """Compatibility name: the caller holds Transfer's reentrant lock.

    Capture now; asynchronous writers must never re-read mutable state.
    """
    # Transfer now uses an RLock. REST and events share the complete schema.
    return {**t.to_dict(), 'state': new.value}


def _emit_transfer_event(side: str, peer_id, transfer: Transfer,
                         old, new: TransferState):
    event = ('TRANSFER_RESUMING' if old == new == TransferState.PAUSED
             and transfer.resume_requested else _event_name_for(old, new))
    if event is None:
        return
    payload = {
        "type": event, "side": side, "peer_id": peer_id,
        "old_state": old.value if old is not None else None,
        'scope': getattr(transfer, 'namespace', ''),
        **_snapshot_without_locking(transfer, new),
    }
    _transfer_events.publish(payload)


def _on_host_transfer_change(peer_id, transfer, old, new):
    _emit_transfer_event("host", peer_id, transfer, old, new)


def _on_client_transfer_change(transfer, old, new):
    _emit_transfer_event("client", None, transfer, old, new)


def _bind_client_events(link):
    if getattr(link.manager, '_api_events_bound', False):
        return
    link.manager.add_listener(lambda t, old, new:
        _on_client_transfer_change(t, old, new) if client_state.get('link') is link else None)
    link.manager.add_progress_listener(lambda t:
        _on_progress('client', None, t) if client_state.get('link') is link else None)
    link.manager._api_events_bound = True


def _on_host_progress(peer_id, transfer):
    _on_progress('host', peer_id, transfer)


def _on_progress(side, peer_id, transfer):
    if transfer.is_terminal:
        return
    now = time.monotonic()
    previous = getattr(transfer, '_ui_progress_sample', (now, transfer.offset))
    # The journal and ACK remain per chunk. Only redundant UI progress is
    # sampled; lifecycle events and the final byte update are never delayed.
    if hasattr(transfer, '_ui_progress_sample') and now - previous[0] < 0.2 and transfer.offset < transfer.total_size:
        return
    transfer._ui_progress_sample = (now, transfer.offset)
    speed = max(0, transfer.offset - previous[1]) / max(now - previous[0], 0.001)
    snapshot = _snapshot_without_locking(transfer, transfer.state)
    _transfer_events.publish(dict(type='TRANSFER_PROGRESS', side=side, peer_id=peer_id,
        scope=getattr(transfer, 'namespace', ''), **snapshot,
        percent=round(100 * transfer.progress, 1), bytes_per_sec=round(speed)))


def _event_snapshot(side=None, cursor=None):
    # Capture BEFORE reading managers: a newly-created row absent from this
    # snapshot must not erase a newer live event delivered while it was read.
    cursor = _transfer_events.cursor if cursor is None else cursor
    transfers = []
    link = client_state.get('link')
    if side != 'host' and link is not None:
        transfers.extend(dict(**t.to_dict(), side='client', peer_id=None,
                              scope=link.manager.namespace) for t in link.manager.all_transfers())
    if side != 'client':
        for info in transfer_host.list_peers():
            peer = transfer_host.get_peer(info['peer_id'])
            if peer is not None:
                transfers.extend(dict(**t.to_dict(), side='host', peer_id=peer.peer_id,
                                      scope=peer.manager.namespace) for t in peer.manager.all_transfers())
    return dict(type='TRANSFER_SNAPSHOT', transfers=transfers, side=side,
                scope=link.manager.namespace if side == 'client' and link is not None else None,
                stream_id=_transfer_events.stream_id,
                event_id=cursor)


def _make_progress_cb(direction: str, filename: str, side: str = "client"):
    """adapter /api/logs progress only, rate-limited to about twice a second.

    The callback/setter interface stays compatible. Structured progress comes
    directly from managers for both roles; it never depends on this late ID.
    """
    state = {"last_time": time.monotonic(), "last_sent": 0, "samples": [], "id": None}

    def set_transfer_id(transfer_id: str) -> None:
        state["id"] = transfer_id

    def cb(sent: int, total: int):
        if _main_loop is None:
            return
        now = time.monotonic()
        elapsed = now - state["last_time"]
        is_done = total and sent >= total
        if elapsed < 0.5 and not is_done:
            return

        delta = sent - state["last_sent"]
        if elapsed > 0:
            rate = delta / elapsed
            state["samples"].append(rate)
            if len(state["samples"]) > 5:
                state["samples"].pop(0)
        bytes_per_sec = sum(state["samples"]) / len(state["samples"]) if state["samples"] else 0
        state["last_time"] = now
        state["last_sent"] = sent

        log_event = {
            "type": "progress", "direction": direction, "filename": filename,
            "sent": sent, "total": total,
            "percent": round(100 * sent / total, 1) if total else 100.0,
            "bytes_per_sec": round(bytes_per_sec),
        }
        _schedule_broadcast(_ws_connections, log_event)

        # Structured progress comes from manager hooks for BOTH app roles,
        # not this compatibility callback whose ID is assigned after submit.
    return cb, set_transfer_id


@app.websocket("/api/logs")
async def logs_ws(websocket: WebSocket):
    await websocket.accept()
    _ws_connections.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _ws_connections.discard(websocket)


@app.websocket("/api/transfers/events")
async def transfer_events_ws(websocket: WebSocket):
    """Snapshot-first ordered feed. Slow subscribers resync or disconnect."""
    side = websocket.query_params.get('side')
    if side not in (None, 'client', 'host'):
        await websocket.close(code=1008, reason='invalid side')
        return
    await websocket.accept()
    subscriber, cursor = _transfer_events.subscribe(side)
    async def write():
        await asyncio.wait_for(websocket.send_json(_event_snapshot(side, cursor)), WS_SEND_TIMEOUT)
        while True:
            payload = await subscriber.get()
            if payload is None:
                return
            if payload['type'] == 'TRANSFER_RESYNC':
                payload = _event_snapshot(side)
            await asyncio.wait_for(websocket.send_json(payload), WS_SEND_TIMEOUT)
    async def read():
        while True:
            await websocket.receive_text()
    tasks = [asyncio.create_task(write()), asyncio.create_task(read())]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    except (WebSocketDisconnect, asyncio.TimeoutError, RuntimeError, OSError):
        pass
    finally:
        _transfer_events.unsubscribe(subscriber)
        for task in tasks:
            task.cancel()
        # Starlette/AnyIO can cancel the handler again during disconnect.
        # Finish child-task cleanup before leaving that cancellation scope.
        with anyio.CancelScope(shield=True):
            await asyncio.gather(*tasks, return_exceptions=True)
            try:
                await asyncio.wait_for(websocket.close(code=1013), WS_SEND_TIMEOUT)
            except (RuntimeError, WebSocketDisconnect, asyncio.TimeoutError, OSError):
                pass


# ---------- Host endpoints ----------

@app.post("/api/host/start")
def host_start():
    if not config.TLS_ENABLED:
        raise HTTPException(status_code=400, detail='TLS must be enabled to host from the app.')
    try:
        transfer_host.start()
    except (SessionError, OSError, ValueError) as exc:
        return JSONResponse(dict(ok=False, error=str(exc), stopping=True), status_code=409)
    return {"ok": True, **host_info()}


@app.post("/api/host/stop")
def host_stop():
    try:
        transfer_host.stop()
    except SessionError as exc:
        return JSONResponse(dict(ok=False, error=str(exc), stopping=True), status_code=409)
    return {"ok": True}


@app.get("/api/host/info")
def host_info():
    return {
        "running": transfer_host.running,
        "stopping": transfer_host.stopping,
        "ip": get_local_ip(),
        "port": config.PORT,
        "pin": transfer_host.current_pin,
        "certificate_fingerprint": transfer_host.certificate_fingerprint,
    }


@app.get("/api/host/pending")
def host_pending():
    return {"pending": transfer_host.list_pending()}


@app.post("/api/host/accept/{request_id}")
def host_accept(request_id: str):
    return {"ok": transfer_host.accept_pending(request_id)}


@app.post("/api/host/reject/{request_id}")
def host_reject(request_id: str):
    return {"ok": transfer_host.reject_pending(request_id)}


@app.get("/api/host/files")
def host_files():
    os.makedirs(transfer_host.dest_dir, exist_ok=True)
    return {"files": [f for f in os.listdir(transfer_host.dest_dir) if not f.endswith('.part')
                      and not f.startswith('.') and os.path.isfile(os.path.join(transfer_host.dest_dir, f))]}


@app.get("/api/host/peers")
def host_peers():
    """who's currently connected. Each peer has its own
    isolated TransferManager -- see host.py."""
    return {"peers": transfer_host.list_peers()}


@app.get("/api/host/transfers")
def host_transfers(peer_id: Optional[str] = None):
    """the host side of the transfer-resource model.
    Without peer_id, transfers across every connected peer; with it,
    just that peer's."""
    cursor = _transfer_events.cursor
    if peer_id:
        peers = [transfer_host.get_peer(peer_id)]
    else:
        peers = [transfer_host.get_peer(p["peer_id"]) for p in transfer_host.list_peers()]
    out = []
    for peer in peers:
        if peer is None:
            continue
        out.extend({**_transfer_summary(t), "peer_id": peer.peer_id}
                   for t in peer.manager.all_transfers())
    return {"ok": True, "transfers": out, 'stream_id': _transfer_events.stream_id, 'event_id': cursor}


class SettingsRequest(BaseModel):
    allow_remote_delete: Optional[bool] = None
    permission: Optional[str] = None
    extensions: Optional[list[str]] = None
    max_upload_mb: Optional[float] = None       # 0 = unlimited
    blocked_extensions: Optional[str] = None    # comma-separated, e.g. ".exe,.bat"
    allow_compression: Optional[bool] = None    # accepted but ignored -- see module docstring


@app.get("/api/host/settings")
def host_settings_get():
    return {**transfer_host.batches.settings(), 'allow_remote_delete': transfer_host.allow_remote_delete}


@app.post("/api/host/settings")
def host_settings_update(req: SettingsRequest):
    if req.permission is not None:
        try:
            transfer_host.batches.configure(req.permission, req.extensions or [], req.max_upload_mb)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        # Batch guard validates original ZIP members; adapter extension checks
        # must not reject an approved container solely for its .zip suffix.
        transfer_host.update_settings(max_upload_mb=req.max_upload_mb, blocked_extensions='')
        if req.allow_remote_delete is not None:
            transfer_host._persistence.set_setting('allow_remote_delete', req.allow_remote_delete)
            transfer_host.allow_remote_delete = req.allow_remote_delete
        return host_settings_get()
    transfer_host.update_settings(
        max_upload_mb=req.max_upload_mb,
        blocked_extensions=req.blocked_extensions,
    )
    transfer_host.batches.rules = dict(permission='deny', extensions=sorted(transfer_host.blocked_extensions),
                                     max_upload_mb=transfer_host.max_upload_size / 1048576)
    transfer_host._persistence.set_setting('receive_rules', transfer_host.batches.rules)
    if req.allow_remote_delete is not None:
        transfer_host._persistence.set_setting('allow_remote_delete', req.allow_remote_delete)
        transfer_host.allow_remote_delete = req.allow_remote_delete
    return host_settings_get()


# ---------- Local UI chat bridge (wire messages use the approved session) ----------

def _chat_conversations(side):
    if side == 'client':
        link = client_state.get('link')
        if link is None:
            return []
        return [(link.manager.namespace, f'Host · {link.host}', link.chat)]
    result = []
    for info in transfer_host.list_peers():
        peer = transfer_host.get_peer(info['peer_id'])
        if peer is not None:
            result.append((peer.peer_id, f"Client · {peer.addr[0]} · {peer.peer_id[:6]}", peer.chat))
    return result


@app.get('/api/chat')
def chat_snapshot(side: Literal['host', 'client'], peer_id: Optional[str] = None,
                  channel_id: Optional[str] = None, revision: int = -1):
    conversations = _chat_conversations(side)
    peers = [dict(peer_id=pid, label=label, **channel.info()) for pid, label, channel in conversations]
    selected = next(((pid, channel) for pid, _, channel in conversations if pid == peer_id), None)
    if selected is None and peer_id is None and conversations:
        selected = conversations[0][0], conversations[0][2]
    if selected is None:
        return dict(ok=True, peers=peers, conversation=None, messages=None)
    pid, channel = selected
    # Get a coherent revision and message snapshot even when frames arrive concurrently.
    state, messages = channel.view(channel_id, revision)
    info = dict(peer_id=pid, **state)
    return dict(ok=True, peers=peers, conversation=info, messages=messages)


class ChatMessageRequest(BaseModel):
    side: Literal['host', 'client']
    peer_id: str = Field(min_length=1, max_length=512)
    message_id: str = Field(pattern=r'^[0-9a-f]{32}$')
    text: str = Field(min_length=1, max_length=2000)


@app.post('/api/chat/messages')
def send_chat_message(request: ChatMessageRequest):
    channel = next((channel for pid, _, channel in _chat_conversations(request.side)
                    if pid == request.peer_id), None)
    if channel is None:
        return JSONResponse({'ok': False, 'error': 'This conversation is no longer available.'}, status_code=404)
    try:
        return dict(ok=True, message=channel.send(request.text, request.message_id))
    except ChatError as exc:
        return JSONResponse({'ok': False, 'error': str(exc)}, status_code=409)


# ---------- Client endpoints ----------

@app.get("/api/client/discover")
def client_discover():
    hosts = discover_hosts()
    return {"ok": True, "hosts": hosts}


class ConnectRequest(BaseModel):
    host: str
    pin: str
    port: Optional[int] = None
    use_tls: Optional[bool] = None
    fingerprint: Optional[str] = Field(default=None, max_length=128)


@app.post("/api/client/connect")
def client_connect(req: ConnectRequest):
    if req.use_tls is False:
        raise HTTPException(status_code=400, detail='Plaintext connections are disabled in the app.')
    current = client_state.get('link')
    same_host = current is not None and current.host.strip().lower() == req.host.strip().lower()
    port = req.port if req.port is not None else (current.port if same_host else config.PORT)
    use_tls = True
    reuse = same_host and current.port == port and current.use_tls == use_tls
    link = current if reuse else HostLink(req.host, port=port, use_tls=use_tls,
                                         dest_dir=config.CLIENT_DIR)
    _bind_client_events(link)
    link.fingerprint = req.fingerprint
    try:
        recovered = link.connect_with_pin(req.pin)
    except ConnectionRejected as e:
        return {"ok": False, "error": str(e)}
    except OSError as e:
        return {"ok": False, "error": f"could not reach host: {e}"}
    except (TransferStateError, ValueError) as e:
        return {'ok': False, 'error': str(e), 'recoverable': True}

    if not reuse:
        if current is not None:
            current.close()
    client_state["link"] = link
    _transfer_events.publish(dict(type='TRANSFER_RESYNC', side='client'))
    return {"ok": True, 'recovered': recovered or [],
            'transfers': [_transfer_summary(t) for t in link.manager.all_transfers()]}


@app.get('/api/client/status')
def client_status():
    link = client_state.get('link')
    transfers = link.manager.all_transfers() if link is not None else []
    return dict(ok=True, connected=bool(link and link.connected),
                host=link.host if link is not None else None,
                recoverable=sum(not t.is_terminal for t in transfers),
                error=getattr(link, 'last_error', None))


@app.post("/api/client/disconnect")
def client_disconnect():
    link = client_state.get("link")
    if link:
        link.close()
    return {"ok": True}


@app.post("/api/client/cancel_transfer")
def client_cancel_transfer():
    """Compatibility cancel-all alias. Only /disconnect closes the session."""
    link = client_state.get("link")
    if not link:
        return {"ok": False, "error": "not connected"}
    return _bulk_action('cancel_all')


def _link_or_error(offline=False):
    link = client_state.get("link")
    if not link or (not offline and not link.connected):
        return None, {"ok": False, "error": "not connected; reconnect with a fresh PIN to recover saved transfers",
                      'disconnected': True, 'recoverable': bool(link)}
    return link, None


def _handle_lost_connection(e: ConnectionLost) -> dict:
    link = client_state.get("link")
    if link:
        if getattr(link, 'router', None):
            link.router.declare_lost(str(e))
        link.last_error = 'Connection lost. Reconnect with a fresh PIN to resume saved transfers.'
    return {"ok": False, "disconnected": True, 'recoverable': True,
            "error": "Connection lost. Reconnect with a fresh PIN to resume saved transfers."}


@app.get("/api/client/files")
def client_files():
    link, err = _link_or_error()
    if err:
        return err
    try:
        return {"ok": True, "files": link.list_files()}
    except ConnectionLost as e:
        return _handle_lost_connection(e)
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}


# ---------- API integration: the actual transfer-resource REST model ----------
# POST /transfers/upload, POST /transfers/download,
# POST /transfers/{id}/pause|resume|cancel|retry,
# GET /transfers/{id}, GET /transfers -- authoritative transfer resources.
# These operate on the connected client's own ConcurrentScheduler, so
# unlike the adapter endpoints below they return immediately (QUEUED or
# ACTIVE) rather than blocking until the transfer finishes, and expose
# pause/resume/retry/concurrency, none of which the adapter endpoints
# below can do.

class UploadTransferRequest(BaseModel):
    local_path: str
    filename: Optional[str] = None
    overwrite: bool = False


@app.post("/api/client/transfers/upload")
def create_upload_transfer(req: UploadTransferRequest):
    link, err = _link_or_error()
    if err:
        return err
    if not os.path.isfile(req.local_path):
        return {"ok": False, "error": f"local file not found: {req.local_path}"}
    fname = req.filename or os.path.basename(req.local_path)
    cb, set_progress_id = _make_progress_cb("upload", fname)
    try:
        t = link.scheduler.submit_upload(
            req.local_path, filename=fname, overwrite=req.overwrite, progress_cb=cb,
        )
    except FileNotFoundError as e:
        return {"ok": False, "error": str(e)}
    set_progress_id(t.id)
    return {"ok": True, "transfer": _transfer_summary(t)}


class DownloadTransferRequest(BaseModel):
    filename: str
    dest_path: Optional[str] = None
    dest_dir: Optional[str] = None


@app.post("/api/client/transfers/download")
def create_download_transfer(req: DownloadTransferRequest):
    link, err = _link_or_error()
    if err:
        return err
    cb, set_progress_id = _make_progress_cb("download", req.filename)
    t = link.scheduler.submit_download(
        req.filename, dest_dir=req.dest_dir, dest_path=req.dest_path, progress_cb=cb,
    )
    set_progress_id(t.id)
    return {"ok": True, "transfer": _transfer_summary(t)}


@app.get("/api/client/transfers")
def list_transfers():
    link, err = _link_or_error(offline=True)
    if err:
        return {"ok": True, "transfers": []}
    return {"ok": True, "transfers": [_transfer_summary(t) for t in link.manager.all_transfers()]}


@app.get("/api/client/transfers/{transfer_id}")
def get_transfer(transfer_id: str):
    link, err = _link_or_error(offline=True)
    if err:
        return err
    try:
        return {"ok": True, "transfer": _transfer_summary(link.manager.get(transfer_id))}
    except TransferNotFound:
        return {"ok": False, "error": "no such transfer"}


def _transfer_action(transfer_id: str, fn_name: str):
    link, err = _link_or_error(offline=fn_name == 'cancel')
    if err:
        return err
    try:
        link.manager.get(transfer_id)  # existence precedes scheduler eligibility
        if fn_name == 'cancel' and hasattr(link, 'cancel_saved'):
            link.cancel_saved(transfer_id)
        else:
            getattr(link.scheduler, fn_name)(transfer_id)
        return {"ok": True, "transfer": _transfer_summary(link.manager.get(transfer_id))}
    except TransferNotFound:
        return {"ok": False, "error": "no such transfer"}
    except TransferStateError as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/client/transfers/{transfer_id}/pause")
def pause_transfer(transfer_id: str):
    return _transfer_action(transfer_id, "pause")


@app.post("/api/client/transfers/{transfer_id}/resume")
def resume_transfer(transfer_id: str):
    return _transfer_action(transfer_id, "resume")


@app.post("/api/client/transfers/{transfer_id}/cancel")
def cancel_transfer(transfer_id: str):
    return _transfer_action(transfer_id, "cancel")


@app.post("/api/client/transfers/{transfer_id}/retry")
def retry_transfer(transfer_id: str):
    return _transfer_action(transfer_id, "retry")


@app.post('/api/client/transfers/{transfer_id}/recover')
def recover_transfer(transfer_id: str):
    return _transfer_action(transfer_id, 'recover_transfer')


# ---------- bulk actions: global (apply-to-every-transfer) actions ----------
# The one-at-a-time endpoints above, applied in bulk -- a "Pause all" /
# "Cancel all" button shouldn't need the frontend to loop over every
# transfer id itself. Each routes through scheduler.*_all(), never
# TransferManager.cancel_all() directly -- see that method's own
# docstring for exactly why that distinction matters once a real
# connection (which there always is here) is attached.

def _bulk_action(fn_name: str):
    link, err = _link_or_error(offline=fn_name == 'cancel_all')
    if err:
        return err
    if fn_name == 'cancel_all' and not getattr(link, 'connected', True) and hasattr(link, 'cancel_saved'):
        pending = [t for t in link.manager.all_transfers() if not t.is_terminal]
        for transfer in pending:
            link.cancel_saved(transfer.id)
        count = len(pending)
    else:
        count = getattr(link.scheduler, fn_name)()
    if fn_name == 'cancel_all':
        count += _cancel_staged_uploads()
    return {"ok": True, "count": count}


@app.post("/api/client/transfers/cancel_all")
def cancel_all_transfers():
    return _bulk_action("cancel_all")


@app.post("/api/client/transfers/pause_all")
def pause_all_transfers():
    return _bulk_action("pause_all")


@app.post("/api/client/transfers/resume_all")
def resume_all_transfers():
    return _bulk_action("resume_all")


@app.post("/api/client/transfers/retry_all")
def retry_all_transfers():
    return _bulk_action("retry_all")


@app.post('/api/client/transfers/recover_all')
def recover_all_transfers():
    link, error = _link_or_error()
    if error:
        return error
    return {'ok': True, 'recovered': link.scheduler.recover_all()}


# ---------- adapter browser-upload bridge (chunked, blocking) ----------
# Kept at the same path/params the current frontend already calls
# (client.html's uploadOne() slices the file client-side and posts each
# piece separately -- a single giant Blob body is a known WebView2
# crash/hang risk on large files; see the original module's note).
#
# What changed underneath: the old version relayed bytes live into the
# socket with no local staging, because FileTransferClient.upload_stream
# accepted a live generator. SendEngine has no equivalent -- it reads a
# real, complete, seekable file (needed for pause/resume/retry) -- so
# each chunk is now staged to a local temp file as it arrives, and the
# final chunk hands that file to scheduler.submit_upload() and blocks
# (same synchronous contract the frontend already expects) until the
# transfer reaches a terminal state.

_UPLOAD_SCRATCH_DIR = os.path.join(config.CLIENT_DIR, ".upload_scratch")
_upload_sessions: dict = {}  # upload_id -> {"path": str, "written": int}
_upload_sessions_lock = threading.RLock()
_upload_transfers = {}  # upload_id -> (link, transfer)
_cancelled_uploads = {}


class BrowserUploadAborted(ValueError):
    pass


def _remember_cancelled_upload(upload_id):
    with _upload_sessions_lock:
        _cancelled_uploads[upload_id] = True
        if len(_cancelled_uploads) > 1024:
            _cancelled_uploads.pop(next(iter(_cancelled_uploads)))


def _cancel_staged_uploads():
    with _upload_sessions_lock:
        ids = list(_upload_sessions)
        for upload_id in ids:
            _remember_cancelled_upload(upload_id)
            _discard_session(upload_id)
    return len(ids)


def _scratch_path(upload_id: str) -> str:
    if not isinstance(upload_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', upload_id):
        raise ValueError('invalid upload_id')
    os.makedirs(_UPLOAD_SCRATCH_DIR, exist_ok=True)
    return os.path.join(_UPLOAD_SCRATCH_DIR, f"{upload_id}.tmp")


def _discard_session(upload_id: str) -> None:
    with _upload_sessions_lock:
        session = _upload_sessions.pop(upload_id, None)
    if session:
        try:
            os.remove(session["path"])
        except OSError:
            pass


async def _stage_browser_chunk(request, upload_id, filename, size, chunk_index, is_final, overwrite):
    """Validate the staging contract before starting a protocol transfer."""
    path = _scratch_path(upload_id)
    if type(size) is not int or size < 0 or size > (1 << 63) - 1:
        raise ValueError('invalid upload size')
    if type(chunk_index) is not int or chunk_index < 0:
        raise ValueError('invalid chunk_index')
    if not isinstance(filename, str) or not filename:
        raise ValueError('invalid filename')
    metadata = (filename, size, overwrite)
    with _upload_sessions_lock:
        if upload_id in _cancelled_uploads:
            raise ValueError('upload cancelled')
        if upload_id in _upload_transfers:
            raise ValueError('upload was already submitted')
        session = _upload_sessions.get(upload_id)
        if session is None:
            if chunk_index != 0:
                raise ValueError('first chunk_index must be zero')
            if os.path.exists(path):
                raise ValueError('upload_id already owns a staging file; use a new upload_id')
            session = {'path': path, 'written': 0, 'next': 0, 'metadata': metadata}
            _upload_sessions[upload_id] = session
        if session['metadata'] != metadata or session['next'] != chunk_index:
            raise ValueError('upload metadata or chunk order changed')
    try:
        if hasattr(request, 'stream'):
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > 8 * 1024 * 1024:
                    raise ValueError('browser chunk exceeds 8 MiB')
                body.extend(chunk)
        else:  # compatibility for direct-handler test/request adapters
            body = await request.body()
    except ValueError:
        _remember_cancelled_upload(upload_id)
        _discard_session(upload_id)
        raise
    except asyncio.CancelledError:
        _remember_cancelled_upload(upload_id)
        _discard_session(upload_id)
        raise
    except Exception as exc:
        _remember_cancelled_upload(upload_id)
        _discard_session(upload_id)
        raise BrowserUploadAborted('upload cancelled') from exc
    if len(body) > 8 * 1024 * 1024:
        _remember_cancelled_upload(upload_id)
        _discard_session(upload_id)
        raise ValueError('browser chunk exceeds 8 MiB')
    def append():
        with _upload_sessions_lock:
            if upload_id in _cancelled_uploads or _upload_sessions.get(upload_id) is not session:
                raise ValueError('upload cancelled')
            if session['next'] != chunk_index:
                raise ValueError('duplicate concurrent chunk')
            end = session['written'] + len(body)
            if end > size or (is_final and end != size):
                raise ValueError('staged bytes do not match declared size')
            with open(path, 'wb' if chunk_index == 0 else 'ab') as fh:
                fh.write(body)
            session['written'] = end
            session['next'] += 1
    await asyncio.get_event_loop().run_in_executor(None, append)
    return session


@app.post("/api/client/upload_stream")
async def client_upload_stream(
    request: Request, upload_id: str, filename: str, size: int,
    chunk_index: int, is_final: bool, overwrite: bool = False,
):
    link, err = _link_or_error()
    if err:
        return err

    loop = asyncio.get_event_loop()

    try:
        session = await _stage_browser_chunk(request, upload_id, filename, size,
                                             chunk_index, is_final, overwrite)
    except BrowserUploadAborted:
        return {'ok': False, 'cancelled': True, 'error': 'upload cancelled'}
    except (OSError, ValueError) as exc:
        return {"ok": False, "error": str(exc)}

    if not is_final:
        return {"ok": True}

    with _upload_sessions_lock:
        _upload_sessions.pop(upload_id, None)

    cb, set_progress_id = _make_progress_cb("upload", filename)
    try:
        t = link.scheduler.submit_upload(
            session["path"], filename=filename, overwrite=overwrite, progress_cb=cb, source_owned=True,
        )
    except Exception as e:
        try:
            os.remove(session["path"])
        except OSError:
            pass
        return {"ok": False, "error": str(e)}
    set_progress_id(t.id)

    def _wait():
        deadline = time.time() + 3600
        while time.time() < deadline:
            if t.state in (TransferState.COMPLETED, TransferState.FAILED, TransferState.CANCELLED, TransferState.CONNECTION_LOST):
                return
            time.sleep(0.05)
    with _upload_sessions_lock:
        _upload_transfers[upload_id] = (link, t)
        was_cancelled = upload_id in _cancelled_uploads
    if was_cancelled:
        link.scheduler.cancel(t.id)
    await loop.run_in_executor(None, _wait)
    with _upload_sessions_lock:
        _upload_transfers.pop(upload_id, None)
    # source_owned=True gives the scheduler sole cleanup ownership after I/O.

    if t.state == TransferState.COMPLETED:
        return {"ok": True}
    if t.state == TransferState.CONNECTION_LOST:
        return _handle_lost_connection(ConnectionLost('upload connection lost'))
    if t.error and "already exists" in t.error:
        return {"ok": False, "conflict": True, "error": t.error}
    return {"ok": False, "error": t.error or f"upload ended in state {t.state.value}"}


@app.post("/api/client/cancel_upload_stream")
async def client_cancel_upload_stream(upload_id: str):
    """Explicit cancel signal called unconditionally by the Cancel
    button -- see the original module's note on why (closing the gap
    between chunk requests where nothing is in-flight for an abort
    signal to interrupt)."""
    try:
        _scratch_path(upload_id)
    except ValueError as exc:
        return {'ok': False, 'error': str(exc)}
    _remember_cancelled_upload(upload_id)
    with _upload_sessions_lock:
        active = _upload_transfers.get(upload_id)
    if active is not None:
        link, transfer = active
        link.scheduler.cancel(transfer.id)
    _discard_session(upload_id)
    return {"ok": True}


# ---------- frontend queue: non-blocking browser-upload bridge ----------
# Same reason for existing at all, same chunking params, and the same
# scratch-staging mechanics as client_upload_stream above -- reused
# directly, including its _upload_sessions bookkeeping. The one thing
# that's different, and the entire reason this is a separate endpoint
# rather than a parameter on the old one: the old one BLOCKS the HTTP
# response until the transfer finishes, which was fine for a frontend
# that only ever showed a single synchronous progress bar, but makes
# it impossible for the frontend to pause, cancel, or retry a transfer
# while it's still running -- there is no response yet to carry a
# transfer id back. This endpoint instead responds the moment the
# final chunk is staged and the transfer is handed to the scheduler,
# with that transfer's id and state in the body, so the frontend can
# act on it immediately through the ordinary /api/client/transfers/...
# endpoints and watch it live over /api/transfers/events -- exactly
# the model frontend queue's frontend queue is built on.
#
# The scratch file this stages into is on the BACKEND's machine, not
# the browser's, so nothing else will ever clean it up once this
# function returns without blocking for completion -- a one-shot
# manager listener removes it the moment this specific transfer
# reaches a terminal state, then removes itself.

@app.post("/api/client/transfers/upload_stream")
async def create_upload_transfer_stream(
    request: Request, upload_id: str, filename: str, size: int,
    chunk_index: int, is_final: bool, overwrite: bool = False,
):
    link, err = _link_or_error()
    if err:
        return err

    loop = asyncio.get_event_loop()

    try:
        session = await _stage_browser_chunk(request, upload_id, filename, size,
                                             chunk_index, is_final, overwrite)
    except BrowserUploadAborted:
        return {'ok': False, 'cancelled': True, 'error': 'upload cancelled'}
    except (OSError, ValueError) as exc:
        return {"ok": False, "error": str(exc)}

    if not is_final:
        return {"ok": True}

    with _upload_sessions_lock:
        _upload_sessions.pop(upload_id, None)
    scratch_path = session["path"]

    cb, set_progress_id = _make_progress_cb("upload", filename)
    try:
        t = await loop.run_in_executor(None, lambda: link.scheduler.submit_upload(
            scratch_path, filename=filename, overwrite=overwrite, progress_cb=cb, source_owned=True,
        ))
    except Exception as e:
        try:
            os.remove(scratch_path)
        except OSError:
            pass
        return {"ok": False, "error": str(e)}
    set_progress_id(t.id)

    def _cleanup_scratch(transfer, old, new, _tid=t.id, _link=link):
        if transfer.id != _tid:
            return
        # FAILED is deliberately NOT cleaned up here: scheduler.retry()
        # re-reads from this same scratch path (it rebuilds the engine
        # from the same closure submit_upload() originally created, not
        # a fresh one), so deleting it the moment a transfer fails would
        # make the Retry button fail every time it's used. A failed
        # upload's scratch file is left for a later retry, or an app
        # restart's own scratch-directory cleanup, to reclaim.
        if new in (TransferState.COMPLETED, TransferState.CANCELLED):
            # The scheduler owns deletion and waits for actual I/O exit.
            # A second API deletion races Windows delete-pending handles.
            with _upload_sessions_lock:
                _upload_transfers.pop(upload_id, None)
            _link.manager.remove_listener(_cleanup_scratch)
    with _upload_sessions_lock:
        _upload_transfers[upload_id] = (link, t)
        was_cancelled = upload_id in _cancelled_uploads
    link.manager.add_listener(_cleanup_scratch)
    if was_cancelled:
        link.scheduler.cancel(t.id)
    _cleanup_scratch(t, None, t.state)  # completion may beat listener registration

    return {"ok": True, "transfer": _transfer_summary(t)}


@app.get("/api/client/download/{filename}")
def client_download(filename: str):
    link, err = _link_or_error()
    if err:
        return err
    cb, set_progress_id = _make_progress_cb("download", filename)
    t = link.scheduler.submit_download(filename, progress_cb=cb)
    set_progress_id(t.id)
    deadline = time.time() + 3600
    while time.time() < deadline:
        if t.state in (TransferState.COMPLETED, TransferState.FAILED, TransferState.CANCELLED):
            break
        time.sleep(0.05)
    if t.state != TransferState.COMPLETED:
        return {"ok": False, "error": t.error or "download failed"}
    path = os.path.join(config.CLIENT_DIR, filename)
    return FileResponse(path, filename=filename)


class DownloadToRequest(BaseModel):
    filename: str
    dest_path: str


@app.post("/api/client/download_to")
def client_download_to(req: DownloadToRequest):
    link, err = _link_or_error()
    if err:
        return err
    if not req.dest_path or not req.dest_path.strip():
        return {"ok": False, "error": "no destination path was provided"}

    cb, set_progress_id = _make_progress_cb("download", req.filename)
    t = link.scheduler.submit_download(req.filename, dest_path=req.dest_path, progress_cb=cb)
    set_progress_id(t.id)
    deadline = time.time() + 3600
    while time.time() < deadline:
        if t.state in (TransferState.COMPLETED, TransferState.FAILED, TransferState.CANCELLED):
            break
        time.sleep(0.05)
    if t.state != TransferState.COMPLETED:
        return {"ok": False, "error": t.error or "download failed"}
    return {"ok": True}


@app.delete("/api/client/delete/{filename}")
def client_delete(filename: str):
    link, err = _link_or_error()
    if err:
        return err
    try:
        return {"ok": link.delete(filename)}
    except ConnectionLost as e:
        return _handle_lost_connection(e)


# ---------- Snippet sharing (unavailable -- see module docstring) ----------

class SnippetRequest(BaseModel):
    text: str


@app.get("/api/host/snippets")
def host_snippets_get():
    return {"ok": True, "snippets": []}


@app.post("/api/host/snippets")
def host_snippets_add(req: SnippetRequest):
    return {"ok": False, "error": "snippet sharing is not available in this build"}


@app.post("/api/client/send_snippet")
def client_send_snippet(req: SnippetRequest):
    return {"ok": False, "error": "snippet sharing is not available in this build"}


@app.get("/api/client/snippets")
def client_snippets_get():
    link, err = _link_or_error()
    if err:
        return err
    return {"ok": True, "snippets": []}


class CreateTransferRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    direction: Literal['upload', 'download']
    local_path: Optional[str] = None
    filename: Optional[str] = None
    dest_path: Optional[str] = None
    dest_dir: Optional[str] = None
    overwrite: bool = False

    @model_validator(mode='after')
    def validate_direction_fields(self):
        if self.filename is not None and (not self.filename.strip() or
                '/' in self.filename or '\\' in self.filename or '\x00' in self.filename or
                self.filename in ('.', '..')):
            raise ValueError('filename must be a nonempty base name')
        if self.direction == 'upload':
            if not self.local_path or self.dest_path or self.dest_dir:
                raise ValueError('upload requires local_path and no destination fields')
        elif not self.filename or self.local_path or self.overwrite:
            raise ValueError('download requires filename and no upload-only fields')
        if self.dest_path and self.dest_dir:
            raise ValueError('choose dest_path or dest_dir, not both')
        return self


def _resource_response(call, success=200):
    """Canonical HTTP errors; old routes keep their documented JSON contract."""
    try:
        result = call()
    except (TransferNotFound, FileNotFoundError):
        result = dict(ok=False, error='no such transfer or source file')
        return JSONResponse(result, status_code=404)
    except TransferStateError as exc:
        return JSONResponse(dict(ok=False, error=str(exc)), status_code=409)
    except ValueError as exc:
        return JSONResponse(dict(ok=False, error=str(exc)), status_code=422)
    except OSError:
        log.exception('Transfer resource I/O failed')
        return JSONResponse(dict(ok=False, error='transfer storage unavailable'), status_code=503)
    if result.get('ok'):
        headers = {'Location': '/api/transfers/' + result['transfer']['id']} if result.get('transfer') else None
        return JSONResponse(result, status_code=success, headers=headers)
    error = result.get('error', '')
    status = (503 if result.get('disconnected') else 404 if
              error == 'no such transfer' or error.startswith('local file not found:') else 409)
    return JSONResponse(result, status_code=status)


def resource_create(req: CreateTransferRequest):
    if req.direction == 'upload':
        return _resource_response(lambda: create_upload_transfer(UploadTransferRequest(
            local_path=req.local_path, filename=req.filename, overwrite=req.overwrite)), 202)
    return _resource_response(lambda: create_download_transfer(DownloadTransferRequest(
        filename=req.filename, dest_path=req.dest_path, dest_dir=req.dest_dir)), 202)


def resource_list():
    return _resource_response(lambda: dict(ok=True, **{
        key: value for key, value in _event_snapshot('client').items() if key != 'type'}))


def resource_get(transfer_id: str):
    return _resource_response(lambda: get_transfer(transfer_id))


def resource_pause(transfer_id: str):
    return _resource_response(lambda: pause_transfer(transfer_id), 202)


def resource_resume(transfer_id: str):
    return _resource_response(lambda: resume_transfer(transfer_id), 202)


def resource_cancel(transfer_id: str):
    return _resource_response(lambda: cancel_transfer(transfer_id), 202)


def resource_retry(transfer_id: str):
    return _resource_response(lambda: retry_transfer(transfer_id), 202)


def resource_recover(transfer_id: str):
    return _resource_response(lambda: recover_transfer(transfer_id), 202)


def resource_content(transfer_id: str):
    link, error = _link_or_error(offline=True)
    if error:
        return JSONResponse(error, status_code=404)
    try:
        transfer = link.manager.get(transfer_id)
    except TransferNotFound:
        return JSONResponse(dict(ok=False, error='no such transfer'), status_code=404)
    with transfer.lock:
        if transfer.direction.value != 'download' or transfer.state != TransferState.COMPLETED:
            return JSONResponse(dict(ok=False, error='download is not complete'), status_code=409)
        path, filename = transfer.dest_path, transfer.filename
    if not path or not os.path.isfile(path):
        return JSONResponse(dict(ok=False, error='download file no longer exists'), status_code=404)
    return FileResponse(path, filename=filename)


async def resource_upload_chunk(request: Request, upload_id: str, filename: str, size: int,
                                chunk_index: int, is_final: bool, overwrite: bool = False):
    result = await create_upload_transfer_stream(request, upload_id, filename, size,
                                                 chunk_index, is_final, overwrite)
    status = (202 if result.get('transfer') else 200) if result.get('ok') else (
        503 if result.get('disconnected') else 409 if result.get('cancelled') else 422)
    return JSONResponse(result, status_code=status)


async def resource_cancel_upload(upload_id: str):
    result = await client_cancel_upload_stream(upload_id)
    return JSONResponse(result, status_code=200 if result.get('ok') else 422)


def resource_bulk(action: Literal['pause_all', 'resume_all', 'retry_all', 'cancel_all', 'recover_all']):
    return _resource_response(lambda: recover_all_transfers() if action == 'recover_all' else _bulk_action(action), 202)


# Canonical resource routes and concise aliases. adapter adapters are deprecated,
# retained as HTTP aliases using the shared session infrastructure.
app.add_api_route('/api/transfers/actions/{action}', resource_bulk, methods=['POST'], status_code=202,
                  responses={409: {'description': 'Invalid lifecycle state'}, 503: {'description': 'Disconnected'}},
                  tags=['transfers'])
for prefix in ('/api/transfers', '/transfers'):
    for path, method, handler in (
        ('', 'POST', resource_create), ('', 'GET', resource_list),
        ('/{transfer_id}', 'GET', resource_get),
        ('/{transfer_id}/pause', 'POST', resource_pause),
        ('/{transfer_id}/resume', 'POST', resource_resume),
        ('/{transfer_id}/cancel', 'POST', resource_cancel),
        ('/{transfer_id}/retry', 'POST', resource_retry),
        ('/{transfer_id}/recover', 'POST', resource_recover),
        ('/{transfer_id}/content', 'GET', resource_content),
    ):
        app.add_api_route(prefix + path, handler, methods=[method],
                          status_code=202 if method == 'POST' else 200,
                          responses={404: {'description': 'Missing resource or source'},
                                     409: {'description': 'Invalid lifecycle state'},
                                     422: {'description': 'Invalid request'},
                                     503: {'description': 'Disconnected or unavailable storage'}},
                          include_in_schema=prefix.startswith('/api/'), tags=['transfers'])
app.add_api_route('/api/uploads/{upload_id}/chunks', resource_upload_chunk, methods=['POST'],
                  responses={202: {'description': 'Final chunk accepted; transfer submitted'}}, tags=['uploads'])
app.add_api_route('/api/uploads/{upload_id}', resource_cancel_upload, methods=['DELETE'], tags=['uploads'])
for route in app.routes:
    if getattr(route, 'path', '').startswith('/api/client/transfers') or getattr(route, 'path', '') in (
        '/api/client/cancel_transfer', '/api/client/cancel_upload_stream',
        '/api/client/upload_stream', '/api/client/download/{filename}', '/api/client/download_to'):
        route.deprecated = True

from .batch_api import install as install_batches
install_batches(app, globals())

app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
