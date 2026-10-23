"""Kill a real TLS host process mid-transfer, restart, and resume the same ID."""
import hashlib
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time

from filetransfer.hostlink import HostLink
from filetransfer.session_protocol import Session
from filetransfer.transport import FrameType
from filetransfer.transfer import TransferState

HOST_SCRIPT = '''
import json, logging, sys
logging.disable(logging.CRITICAL)
from filetransfer import config
from filetransfer.host import TransferHost
config.HOST='127.0.0.1'; config.PORT=int(sys.argv[2]); config.STATE_DIR=None
config.TLS_ENABLED=True; config.PIN_EXPIRY_SECONDS=0
host=TransferHost(dest_dir=sys.argv[1])
host._discovery.start=lambda: None
host.add_pending_listener(lambda info: host.accept_pending(info['id']))
host.start()
print('READY '+json.dumps(dict(port=host._sock.getsockname()[1], pin=host.current_pin,
                              fingerprint=host.certificate_fingerprint)), flush=True)
sys.stdin.readline()
host.stop()
'''


def test_real_tls_host_process_kill_restart_resume(tmp_path, monkeypatch):
    children = []
    def start(port):
        process = subprocess.Popen([sys.executable, '-u', '-c', HOST_SCRIPT,
            str(tmp_path / 'host'), str(port)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
            env=dict(os.environ, FT_LOG_FILE=str(tmp_path / 'child.log')))
        children.append(process)
        messages = queue.Queue()
        def read():
            for line in process.stdout:
                if line.startswith('READY '):
                    messages.put(json.loads(line[6:])); return
            messages.put(None)
        threading.Thread(target=read, daemon=True).start()
        info = messages.get(timeout=15)
        assert info is not None, 'Child host failed to start'
        return process, info
    def wait(predicate):
        deadline = time.monotonic()+30
        while not predicate():
            assert time.monotonic() < deadline
            time.sleep(.01)
    original = Session.send
    def paced(session, frame):
        if frame.frame_type == FrameType.TRANSFER_DATA:
            time.sleep(.05)
        return original(session, frame)
    monkeypatch.setattr(Session, 'send', paced)
    link = None
    try:
        child, info = start(0)
        link = HostLink('127.0.0.1', port=info['port'], use_tls=True,
            fingerprint=info['fingerprint'], dest_dir=str(tmp_path / 'client'))
        link.connect_with_pin(info['pin'])
        source = tmp_path / 'resume.bin'
        source.write_bytes(os.urandom(16*1048576))
        transfer = link.scheduler.submit_upload(str(source))
        wait(lambda: 0 < transfer.offset < transfer.total_size)
        tid = transfer.id
        child.kill(); child.communicate(timeout=10)
        wait(lambda: transfer.state == TransferState.CONNECTION_LOST)
        assert not (tmp_path / 'host' / source.name).exists()
        _, restarted = start(info['port'])
        assert restarted['fingerprint'] == info['fingerprint']
        recovered = link.connect_with_pin(restarted['pin'])
        assert tid in recovered
        wait(lambda: transfer.is_terminal and transfer.io_finished.is_set())
        assert transfer.state == TransferState.COMPLETED, transfer.error
        assert link.manager.get(tid) is transfer
        with (tmp_path / 'host' / source.name).open('rb') as stream:
            assert hashlib.file_digest(stream, 'sha256').digest() == hashlib.sha256(source.read_bytes()).digest()
    finally:
        if link:
            link.close()
        for child in children:
            if child.poll() is None:
                child.kill(); child.communicate(timeout=10)
