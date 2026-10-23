"""Host-owned batch approvals and receive rules; no file bytes precede approval."""
import hashlib
import json
import math
import os
import re
import shutil
import threading
import time
import zipfile

from .security import safe_filename


def file_digest(path):
    result = hashlib.sha256()
    with open(path, 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


def extensions(values):
    if isinstance(values, str):
        values = values.split(',')
    if not isinstance(values, list) or len(values) > 100:
        raise ValueError('Choose at most 100 extensions.')
    result = set()
    for value in values:
        if not isinstance(value, str):
            raise ValueError('Invalid extension.')
        value = value.strip().lower()
        if not value:
            continue
        value = '.' + value.lstrip('.')
        if not re.fullmatch(r'\.[a-z0-9][a-z0-9._+-]{0,30}', value):
            raise ValueError('Use extensions such as .pdf, .zip, or .custom.')
        result.add(value)
    return sorted(result)


class HostBatches:
    def __init__(self, host):
        self.host = host
        self.lock = threading.RLock()
        self.records = host._persistence.get_setting('receive_batches') or {}
        self.rules = host._persistence.get_setting('receive_rules') or {
            'permission': 'deny', 'extensions': sorted(host.blocked_extensions),
            'max_upload_mb': host.max_upload_size / 1048576,
        }

    def settings(self):
        os.makedirs(self.host.dest_dir, exist_ok=True)
        with self.lock:
            return {**self.rules, 'free_bytes': shutil.disk_usage(self.host.dest_dir).free,
                    'blocked_extensions': ','.join(self.rules['extensions']) if self.rules['permission'] == 'deny' else ''}

    def configure(self, permission, selected, max_upload_mb):
        if permission not in ('allow', 'deny'):
            raise ValueError('Choose Is allowed or Is not allowed.')
        selected = extensions(selected)
        if permission == 'allow' and not selected:
            raise ValueError('Select at least one allowed extension.')
        if not isinstance(max_upload_mb, (int, float)) or not math.isfinite(max_upload_mb) or max_upload_mb < 1:
            raise ValueError('The maximum file size must be at least 1 MB.')
        if max_upload_mb * 1048576 > self.settings()['free_bytes']:
            raise ValueError('The limit exceeds currently available host disk space.')
        with self.lock:
            self.rules = dict(permission=permission, extensions=selected, max_upload_mb=max_upload_mb)
            self.host._persistence.set_setting('receive_rules', self.rules)

    def _save(self):
        self.host._persistence.set_setting('receive_batches', self.records)

    @staticmethod
    def _files(value):
        if not isinstance(value, list) or not 1 <= len(value) <= 100:
            raise ValueError('Select between 1 and 100 files per batch.')
        result, names = [], set()
        for item in value:
            if not isinstance(item, dict):
                raise ValueError('Invalid file manifest.')
            name, size, checksum = item.get('filename'), item.get('size'), item.get('checksum')
            if not isinstance(name, str) or len(name.encode('utf-8')) > 240 or safe_filename(name) != name or name.startswith('.'):
                raise ValueError('Invalid batch filename.')
            if name.casefold() in names:
                raise ValueError('A batch cannot contain duplicate filenames.')
            if type(size) is not int or not 0 <= size < 2**63 or not isinstance(checksum, str) or not re.fullmatch('[0-9a-f]{64}', checksum):
                raise ValueError('Invalid file size or checksum.')
            names.add(name.casefold())
            result.append(dict(filename=name, size=size, checksum=checksum))
        return result

    def _check_rules(self, record):
        rules = self.rules
        limit = int(rules['max_upload_mb'] * 1048576)
        for item in record.get('members') or record['files']:
            matches = any(item['filename'].lower().endswith(ext) for ext in rules['extensions'])
            if (rules['permission'] == 'allow' and not matches) or (rules['permission'] == 'deny' and matches):
                raise ValueError(f"File type is not allowed: {item['filename']}")
            if limit and item['size'] > limit:
                raise ValueError(f"File exceeds the size limit: {item['filename']}")
        if limit and any(item['size'] > limit for item in record['files']):
            raise ValueError('The ZIP or received file exceeds the maximum file size.')

    def request(self, peer_id, payload):
        if not self.host.running:
            raise ValueError('Host is not running.')
        batch_id = payload.get('batch_id')
        if not isinstance(batch_id, str) or not re.fullmatch('[0-9a-f]{32}', batch_id):
            raise ValueError('Invalid batch ID.')
        files = self._files(payload.get('files'))
        members = self._files(payload['members']) if payload.get('members') is not None else None
        if members and (len(files) != 1 or not files[0]['filename'].lower().endswith('.zip')):
            raise ValueError('Compressed batches must contain one ZIP archive.')
        record = dict(batch_id=batch_id, peer_id=peer_id, files=files, members=members,
                      overwrite=payload.get('overwrite') is True)
        if len(json.dumps(record).encode()) > 60000:
            raise ValueError('Batch manifest is too large. Select fewer files.')
        key = peer_id + ':' + batch_id
        with self.lock:
            self._expire()
            old = self.records.get(key)
            if old:
                if any(old.get(k) != v for k, v in record.items()):
                    raise ValueError('Batch ID is already used for a different selection.')
                return self.status(peer_id, batch_id)
            if sum(r['status'] == 'pending' for r in self.records.values()) >= 20:
                raise ValueError('Too many pending groups. Wait for the host to respond.')
            if sum(r['status'] == 'approved' for r in self.records.values()) >= 100:
                raise ValueError('Too many unfinished approved groups. Finish existing transfers first.')
            self._check_rules(record)
            record.update(status='pending', created=time.time(), claims={}, done=[])
            self.records[key] = record
            self._save()
        return self.status(peer_id, batch_id)

    def _expire(self):
        for record in self.records.values():
            if record['status'] == 'pending' and time.time() - record['created'] > 600:
                record['status'] = 'expired'
            if record['status'] == 'approved' and not record['claims'] and time.time() - record.get('decided', 0) > 3600:
                record['status'] = 'expired'
        # Keep active approvals for recovery; only remove old non-active entries.
        removable = [k for k, r in self.records.items() if r['status'] in ('rejected', 'expired', 'cancelled', 'completed')]
        for key in removable[:-100]:
            self.records.pop(key, None)

    def status(self, peer_id, batch_id):
        with self.lock:
            self._expire()
            record = self.records.get(peer_id + ':' + batch_id)
            if not record:
                raise ValueError('Unknown batch. Select the files again.')
            return dict(batch_id=batch_id, status=record['status'], error=record.get('error'))

    def pending(self):
        with self.lock:
            self._expire()
            return [json.loads(json.dumps(r)) for r in self.records.values() if r['status'] == 'pending']

    def decide(self, peer_id, batch_id, accept):
        with self.lock:
            self._expire()
            record = self.records.get(peer_id + ':' + batch_id)
            if not record or record['status'] != 'pending':
                raise ValueError('This batch is no longer waiting for approval.')
            if accept:
                self._check_rules(record)
                reserved = sum(sum(f['size'] for f in r['files'] if f['filename'] not in r['done'])
                               for r in self.records.values() if r['status'] == 'approved')
                if reserved + sum(f['size'] for f in record['files']) > shutil.disk_usage(self.host.dest_dir).free:
                    raise ValueError('Not enough unreserved disk space for this group.')
            record['status'] = 'approved' if accept else 'rejected'
            record['decided'] = time.time()
            self._save()
            return self.status(peer_id, batch_id)

    def cancel(self, peer_id, batch_id):
        with self.lock:
            record = self.records.get(peer_id + ':' + batch_id)
            if record and record['status'] in ('pending', 'approved') and not record['claims']:
                record['status'] = 'cancelled'
                self._save()
            return self.status(peer_id, batch_id)

    def authorize(self, peer_id, frame):
        payload = frame.json()
        if payload.get('direction') != 'upload':
            return None
        batch_id = payload.get('batch_id')
        if not batch_id:
            if self.host.require_batch_approval:
                raise ValueError('Host approval is required for this file group. Use the batch upload control.')
            # Compatibility for protocol embedders, never enabled by the app.
            self._check_rules(dict(files=[dict(filename=payload.get('filename', ''), size=payload.get('size', 0))]))
            return None
        with self.lock:
            record = self.records.get(peer_id + ':' + str(batch_id))
            if not record or record['status'] not in ('approved', 'completed'):
                raise ValueError('This file group has not been approved by the host.')
            self._check_rules(record)
            item = next((f for f in record['files'] if f['filename'] == payload.get('filename')), None)
            if not item or item['size'] != payload.get('size') or item['checksum'] != payload.get('checksum') or record['overwrite'] != payload.get('overwrite', False):
                raise ValueError('File does not match the approved group.')
            claim = record['claims'].get(item['filename'])
            if claim and claim != frame.transfer_id:
                raise ValueError('This approved file already belongs to another transfer.')
            if not claim and time.time() - record.get('decided', 0) > 3600:
                raise ValueError('Unused approval has expired. Select the files again.')
            record['claims'][item['filename']] = frame.transfer_id
            self._save()
            members = record['members']
        if not members:
            return None
        def validate_archive(path):
            with zipfile.ZipFile(path) as archive:
                infos = archive.infolist()
                expected = {m['filename']: m for m in members}
                if len(infos) != len(expected) or {i.filename for i in infos} != set(expected):
                    raise ValueError('ZIP contents differ from the approved selection.')
                for info in infos:
                    item = expected[info.filename]
                    if info.file_size != item['size'] or info.flag_bits & 1 or info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
                        raise ValueError('ZIP member metadata does not match approval.')
                    digest, total = hashlib.sha256(), 0
                    with archive.open(info) as stream:
                        while True:
                            chunk = stream.read(min(1048576, item['size'] - total + 1))
                            if not chunk:
                                break
                            total += len(chunk)
                            if total > item['size']:
                                raise ValueError('ZIP member exceeds approved size.')
                            digest.update(chunk)
                    if total != item['size'] or digest.hexdigest() != item['checksum']:
                        raise ValueError('ZIP member differs from the approved selection.')
        return validate_archive

    def finished(self, peer_id, transfer):
        if transfer.state.value not in ('completed', 'cancelled'):
            return
        with self.lock:
            for record in self.records.values():
                if record['peer_id'] != peer_id:
                    continue
                for name, tid in record['claims'].items():
                    if tid == transfer.id and name not in record['done']:
                        record['done'].append(name)
                        if len(record['done']) == len(record['files']):
                            record['status'] = 'completed'
                        self._save()
