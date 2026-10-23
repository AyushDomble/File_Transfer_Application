"""Local disk-backed selections. Only approved manifests become wire transfers."""
import os
import shutil
import tempfile
import threading
import time
import uuid
import zipfile
import hashlib
import anyio

from fastapi import HTTPException, Request
from filetransfer.batches import HostBatches


def install(app, env):
    groups, lock = {}, threading.RLock()
    preparation_slots = threading.BoundedSemaphore(2)
    staging_slots = anyio.CapacityLimiter(4)

    def get(batch_id):
        with lock:
            group = groups.get(batch_id)
        if group is None:
            raise HTTPException(404, 'Unknown file selection')
        if env['client_state'].get('link') is not group['link']:
            raise HTTPException(409, 'Reconnect to the original host or select the files again')
        return group

    def clean(group):
        if group.get('preparing'):
            return  # The preparation worker still owns these paths.
        # Paths were generated locally, never supplied by the network.
        for file in group.get('payload', []):
            if file.get('submitted'):
                continue
            if os.path.isfile(file['path']):
                os.remove(file['path'])
        for file in group['files']:
            if not file.get('submitted') and os.path.isfile(file['path']):
                os.remove(file['path'])
        archive = os.path.join(group['directory'], 'archive.zip')
        if os.path.isfile(archive) and not any(f.get('submitted') and f['path'] == archive for f in group.get('payload', [])):
            os.remove(archive)
        try:
            os.rmdir(group['directory'])
        except OSError:
            pass

    def shutdown():
        with lock:
            for group in groups.values():
                group['cancel_event'].set()
                with group['lock']:
                    group['status'] = 'cancelled'
                    clean(group)
            groups.clear()
    env['_cleanup_batches'] = shutdown

    @app.get('/api/host/batches')
    def pending():
        host = env['transfer_host']
        records = host.batches.pending()
        for record in records:
            peer = host.get_peer(record['peer_id'])
            record['peer_label'] = peer.addr[0] if peer else record['peer_id'][:8]
        return {'batches': records}

    @app.post('/api/host/batches/{peer_id}/{batch_id}/{decision}')
    def decide(peer_id: str, batch_id: str, decision: str):
        if decision not in ('accept', 'reject'):
            raise HTTPException(400, 'Unknown decision')
        try:
            return env['transfer_host'].batches.decide(peer_id, batch_id, decision == 'accept')
        except ValueError as exc:
            raise HTTPException(409, str(exc))

    @app.post('/api/client/batches')
    def create(payload: dict):
        link, error = env['_link_or_error']()
        if error:
            raise HTTPException(409, 'Connect to a host first')
        if not link.session.supports('batch_approval'):
            raise HTTPException(409, 'Update the host to support group approval')
        try:
            files = HostBatches._files([dict(item, checksum='0' * 64) for item in payload.get('files', [])])
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, str(exc))
        with lock:
            for key, group in list(groups.items()):
                finished = group['status'] == 'sending' and all(group['link'].manager.get(tid).is_terminal for tid in group['transfers'])
                if not group.get('preparing') and (finished or group['status'] in ('cancelled', 'failed', 'rejected', 'expired') or time.time() - group['created'] > 1800):
                    with group['lock']:
                        if group['status'] != 'sending':
                            clean(group)
                        del groups[key]
            if len(groups) >= 20:
                raise HTTPException(429, 'Too many selections; finish or cancel a group first')
            os.makedirs(env['_UPLOAD_SCRATCH_DIR'], exist_ok=True)
            root = tempfile.mkdtemp(prefix='batch-', dir=env['_UPLOAD_SCRATCH_DIR'])
            if sum(f['size'] for f in files) * (2 if payload.get('zipped') else 1) > shutil.disk_usage(root).free:
                os.rmdir(root)
                raise HTTPException(400, 'Not enough local disk space to prepare this selection')
            bid = uuid.uuid4().hex
            for i, file in enumerate(files):
                file.update(path=os.path.join(root, str(i)), written=0, next=0, ready=False)
            groups[bid] = dict(files=files, directory=root, link=link, zipped=payload.get('zipped') is True,
                               status='staging', created=time.time(), lock=threading.RLock(), transfers=[],
                               cancel_event=threading.Event(), preparing=False, prepared_bytes=0)
        return {'batch_id': bid, 'status': 'staging'}

    @app.get('/api/client/batches')
    def list_groups():
        with lock:
            return {'batches': [dict(batch_id=bid, status=g['status'], count=len(g['files']))
                for bid, g in groups.items() if g['link'] is env['client_state'].get('link')
                and g['status'] in ('preparing', 'pending', 'approved')]}

    @app.put('/api/client/batches/{batch_id}/files/{index}')
    async def stage(batch_id: str, index: int, request: Request, chunk_index: int, final: bool = False):
        group = get(batch_id)
        # Bound buffered uploads as well as offloaded writes. Do not block the
        # ASGI event loop on disk or on the selection's synchronous lock.
        async with staging_slots:
            body = bytearray()
            async for chunk in request.stream():
                if len(body) + len(chunk) > 4 * 1024 * 1024:
                    raise HTTPException(413, 'Chunk exceeds 4 MiB')
                body.extend(chunk)
            await anyio.to_thread.run_sync(write_stage, group, index, chunk_index, final, body)
        return {'ok': True}

    def write_stage(group, index, chunk_index, final, body):
        with group['lock']:
            if group['status'] != 'staging' or not 0 <= index < len(group['files']):
                raise HTTPException(409, 'Selection is no longer accepting files')
            file = group['files'][index]
            end = file['written'] + len(body)
            if file['ready'] or file['next'] != chunk_index or end > file['size'] or (final and end != file['size']):
                raise HTTPException(400, 'File size or chunk order changed')
            with open(file['path'], 'wb' if chunk_index == 0 else 'ab') as stream:
                stream.write(body)
            file.update(written=end, next=chunk_index + 1, ready=final)

    class PreparationCancelled(Exception):
        pass

    def checkpoint(group, amount=0):
        if group['cancel_event'].is_set():
            raise PreparationCancelled()
        with group['lock']:
            group['prepared_bytes'] += amount

    def digest(group, path):
        checksum = hashlib.sha256()
        with open(path, 'rb') as stream:
            while chunk := stream.read(1024 * 1024):
                checkpoint(group, len(chunk))
                checksum.update(chunk)
        checkpoint(group)
        return checksum.hexdigest()

    def prepare(group, batch_id):
        try:
            for file in group['files']:
                file['checksum'] = digest(group, file['path'])
            payload = group['files']
            members = None
            if group['zipped']:
                path = os.path.join(group['directory'], 'archive.zip')
                with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
                    for file in group['files']:
                        checkpoint(group)
                        with open(file['path'], 'rb') as source, archive.open(file['filename'], 'w', force_zip64=True) as target:
                            while chunk := source.read(1024 * 1024):
                                checkpoint(group, len(chunk))
                                target.write(chunk)
                members = group['files']
                payload = [dict(path=path, filename='Selection-' + batch_id[:8] + '.zip',
                    size=os.path.getsize(path), checksum=digest(group, path))]
            checkpoint(group)
            manifest = lambda items: [{k: f[k] for k in ('filename', 'size', 'checksum')} for f in items]
            result = group['link'].batch('request', dict(batch_id=batch_id, files=manifest(payload),
                members=manifest(members) if members else None, overwrite=False))
            with group['lock']:
                if not group['cancel_event'].is_set():
                    group['payload'] = payload
                    group['status'] = result['status']
            if group['cancel_event'].is_set():
                # Cancellation may race the request RPC. Revoke its unused grant.
                try:
                    group['link'].batch('cancel', {'batch_id': batch_id})
                except Exception:
                    pass
        except PreparationCancelled:
            pass
        except Exception as exc:
            with group['lock']:
                if not group['cancel_event'].is_set():
                    group['status'], group['error'] = 'failed', str(exc)
        finally:
            try:
                with group['lock']:
                    group['preparing'] = False
                    if group['cancel_event'].is_set():
                        group['status'] = 'cancelled'
                    if group['status'] in ('failed', 'cancelled', 'rejected', 'expired') or (group['status'] == 'sending' and group['zipped']):
                        clean(group)
            finally:
                preparation_slots.release()

    @app.post('/api/client/batches/{batch_id}/request', status_code=202)
    def request_approval(batch_id: str):
        group = get(batch_id)
        with group['lock']:
            if group['status'] != 'staging':
                return {'status': group['status']}
            if not all(f['ready'] for f in group['files']):
                raise HTTPException(409, 'Finish preparing all files first')
            if not preparation_slots.acquire(blocking=False):
                raise HTTPException(429, 'Two selections are already being prepared; try again shortly')
            group['status'], group['preparing'] = 'preparing', True
            try:
                threading.Thread(target=prepare, args=(group, batch_id), daemon=True,
                    name='batch-prepare-' + batch_id[:8]).start()
            except Exception:
                group['status'], group['preparing'] = 'staging', False
                preparation_slots.release()
                raise
            return {'status': 'preparing'}

    @app.get('/api/client/batches/{batch_id}')
    def status(batch_id: str):
        group = get(batch_id)
        with group['lock']:
            if group['status'] in ('pending', 'approved'):
                try:
                    result = group['link'].batch('status', {'batch_id': batch_id})
                    group['status'] = result['status']
                    if result['status'] == 'approved':
                        for file in group['payload']:
                            if not file.get('submitted'):
                                transfer = group['link'].scheduler.submit_upload(file['path'], filename=file['filename'],
                                    source_owned=True, batch_id=batch_id, _prepared_checksum=file['checksum'])
                                file['submitted'] = True
                                group['transfers'].append(transfer.id)
                        group['status'] = 'sending'
                        if group['zipped']:
                            clean(group)
                    elif result['status'] in ('rejected', 'expired', 'cancelled'):
                        clean(group)
                except Exception as exc:
                    raise HTTPException(409, str(exc))
            return {'status': group['status'], 'transfers': group['transfers'],
                    'prepared_bytes': group['prepared_bytes'], 'error': group.get('error')}

    @app.delete('/api/client/batches/{batch_id}')
    def cancel(batch_id: str):
        group = get(batch_id)
        group['cancel_event'].set()
        with group['lock']:
            if group['status'] in ('pending', 'approved'):
                try:
                    group['link'].batch('cancel', {'batch_id': batch_id})
                except Exception:
                    # No upload is submitted after local cancellation. An offline
                    # host's unused approval expires independently.
                    pass
            for tid in group['transfers']:
                group['link'].scheduler.cancel(tid)
            group['status'] = 'cancelled'
            clean(group)
        return {'ok': True}
