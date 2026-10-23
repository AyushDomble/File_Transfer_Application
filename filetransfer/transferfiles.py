"""Transfer-owned partial files and process-wide destination admission.

All peers share this registry. A paused writer retains its reservation; a
terminal or disconnected writer releases it. Every transfer has a distinct
partial path, so cancellation never unlinks another transfer's bytes.
"""
import os
import hashlib
import threading
import weakref

from .transfer import TransferState
from .transport import validate_transfer_id

_lock = threading.RLock()
_owners = weakref.WeakValueDictionary()
_released = {TransferState.COMPLETED, TransferState.CANCELLED,
             TransferState.FAILED, TransferState.CONNECTION_LOST}


def partial_path(destination, transfer_id, namespace=''):
    validate_transfer_id(transfer_id, allow_null=False)
    key = _key(destination) + ('\0' + namespace if namespace else '')
    digest = hashlib.sha256(key.encode('utf-8')).hexdigest()[:32]
    # Fixed-length component: even a 255-character final filename must not
    # produce an overlong .part component. The destination digest also keeps
    # equal peer-chosen IDs for different destinations from sharing bytes.
    return os.path.join(os.path.dirname(destination), f'.ft-{digest}-{transfer_id}.part')


def destination_for_part(part, transfer_id):
    if os.path.basename(part).startswith('.ft-'):
        raise ValueError('transfer-owned partial files require destination metadata')
    suffix = '.' + transfer_id + '.part'
    return part[:-len(suffix)] if part.endswith(suffix) else part.removesuffix('.part')


def _key(path):
    return os.path.normcase(os.path.realpath(path))


def claim_destination(transfer, destination, part=None, *, overwrite=False):
    destination = os.path.abspath(destination)
    part = os.path.abspath(part) if part is not None else None
    with _lock:
        key = _key(destination)
        owner = _owners.get(key)
        if owner is not None and owner is not transfer and owner.state not in _released:
            raise ValueError('destination is busy with another transfer')
        transfer.dest_path = destination
        transfer.part_path = part or partial_path(destination, transfer.id, getattr(transfer, 'namespace', ''))
        transfer.overwrite = overwrite
        _owners[key] = transfer


def release_destination(transfer):
    if transfer.dest_path:
        with _lock:
            key = _key(transfer.dest_path)
            if _owners.get(key) is transfer:
                _owners.pop(key, None)


def publish(transfer, part, destination, *, overwrite):
    """Called under Transfer.lock: cancellation and publication are serialized."""
    with _lock:
        owner = _owners.get(_key(destination))
        if owner is not None and owner is not transfer and owner.state not in _released:
            raise ValueError('destination is busy with another transfer')
        if overwrite:
            os.replace(part, destination)
        elif os.name == 'nt':
            # Windows rename fails if the destination already exists. It
            # needs no hard-link privilege and keeps no-overwrite atomic.
            os.rename(part, destination)
        else:
            # Unlike exists()+replace(), this fails atomically if an external
            # process created the destination since the CREATE handshake.
            os.link(part, destination)
            os.remove(part)
