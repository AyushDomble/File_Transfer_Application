"""Deterministic adversarial corpus; local test data only, no external targets."""
import json
import random
from pathlib import Path

import pytest
from helpers.native_peers import native_pair
from filetransfer.transport import Frame, FrameError, FrameType
from filetransfer.transfer import TransferState


@pytest.mark.parametrize('payload', [
    b'{"x":NaN}', b'{"x":Infinity}', b'{"x":1e9999}',
    b'{"x":1,"x":2}', b'{"x":"\\ud800"}',
    b'{"x":' + b'['*1500 + b'0' + b']'*1500 + b'}',
    b'{"x":' + b'9'*5000 + b'}',
], ids=['nan', 'infinity', 'overflow', 'duplicate-key', 'surrogate', 'deep-nesting', 'large-integer'])
def test_strict_control_json_rejects_ambiguous_or_unbounded_values(payload):
    with pytest.raises(FrameError):
        Frame(FrameType.HELLO, payload).json()


def test_seeded_json_mutations_never_escape_protocol_error():
    rng = random.Random(250021)
    seeds = [b'{"filename":"normal.txt","size":10}', b'{"pin":"123456"}', b'{}']
    for _ in range(2000):
        data = bytearray(rng.choice(seeds))
        for _ in range(rng.randrange(1, 8)):
            data.insert(rng.randrange(len(data)+1), rng.randrange(256))
        try:
            value = Frame(FrameType.HELLO, bytes(data)).json()
            assert isinstance(value, dict)
        except FrameError:
            pass


@pytest.mark.parametrize('name', ['.private.txt', 'unfinished.part'])
def test_hidden_and_partial_files_cannot_be_downloaded(native_pair, name):
    host, link = native_pair
    (Path(host.dest_dir) / name).write_bytes(b'not shared')
    assert name not in link.list_files()
    transfer = link.scheduler.submit_download(name)
    assert transfer.io_finished.wait(10)
    assert transfer.state == TransferState.FAILED
    assert not (Path(link.dest_dir) / name).exists()
    assert link.connected


@pytest.mark.parametrize('name', ['../secret', '.state', 'open.PART', 'file:stream', 'file.', 'file '])
def test_private_path_aliases_rejected(tmp_path, name):
    from filetransfer.security import shared_file_path
    with pytest.raises(ValueError):
        shared_file_path(str(tmp_path), name)


def test_symlink_admission_guard(tmp_path, monkeypatch):
    from filetransfer.security import shared_file_path
    import os
    path = tmp_path / 'link.txt'
    path.write_text('fixture')
    original = os.path.islink
    monkeypatch.setattr(os.path, 'islink', lambda p: str(p) == str(path) or original(p))
    with pytest.raises(ValueError):
        shared_file_path(str(tmp_path), path.name)


def test_seeded_invalid_headers_rejected_without_body_read():
    from filetransfer.transport import FrameReader, _STRUCT_HEADER, MAGIC, VERSION
    rng = random.Random(250022)
    for _ in range(2000):
        fields = [MAGIC, VERSION, int(FrameType.TRANSFER_DATA), 0, 0, b'a'*32, 0, 1]
        field, invalid = rng.choice([(0, MAGIC ^ rng.randrange(1, 2**32)),
            (1, 255), (2, 255), (3, 128), (4, 1), (5, b'z'*32), (7, 2**32-1)])
        fields[field] = invalid
        header = _STRUCT_HEADER.pack(*fields)
        class HeaderOnly:
            def __init__(self): self.remaining = header
            def recv_into(self, buffer, count=0):
                assert self.remaining, 'Parser requested attacker-declared body'
                amount = min(len(buffer), len(self.remaining))
                buffer[:amount] = self.remaining[:amount]
                self.remaining = self.remaining[amount:]
                return amount
        with pytest.raises(FrameError):
            FrameReader().recv(HeaderOnly())
