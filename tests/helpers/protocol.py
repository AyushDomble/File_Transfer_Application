"""Shared protocol test support."""
import hashlib
from filetransfer.engine import FrameRouter, RecvEngine, _encode_data_payload, recv_transfer_create
from filetransfer.manager import TransferManager
from filetransfer.transport import Frame, FrameFlags, FrameType


class RecordingSession:
    is_lost = False
    closed = False
    negotiation = None
    def __init__(self):
        self.frames = []
    def send_json(self, kind, payload, **kwargs):
        self.frames.append(Frame.json_frame(kind, payload, **kwargs))
    def send(self, frame):
        self.frames.append(frame)
    def send_transfer_error(self, tid, error):
        self.send_json(FrameType.TRANSFER_ERROR, {'error': error}, transfer_id=tid)
    def send_transfer_cancel(self, tid, reason=''):
        self.send_json(FrameType.TRANSFER_CANCEL, {'reason': reason}, transfer_id=tid)

def create(session, manager, router, directory, *, tid='a' * 32, **changes):
    metadata = dict(filename='test.bin', size=4, checksum=hashlib.sha256(b'data').hexdigest(),
                    direction='upload', chunk_size=4096)
    metadata.update(changes)
    frame = Frame.json_frame(FrameType.TRANSFER_CREATE, metadata, transfer_id=tid)
    return recv_transfer_create(session, frame, str(directory), manager, router)

def receiver(tmp_path, data, *, declared=None):
    session, manager = RecordingSession(), TransferManager()
    router = FrameRouter(session)
    transfer, offset, part = create(session, manager, router, tmp_path,
        size=len(data) if declared is None else declared, checksum=hashlib.sha256(data).hexdigest())
    engine = RecvEngine(session=session, transfer=transfer, manager=manager,
        frame_queue=router.get_queue(transfer.id), part_path=part, resume_offset=offset)
    return session, manager, transfer, engine

def data_frame(transfer, chunk, offset=0, sequence=0, final=True):
    return Frame(FrameType.TRANSFER_DATA, _encode_data_payload(offset, chunk), transfer.id,
                 sequence, FrameFlags.FINAL_CHUNK if final else 0)

def inline(coro):
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    raise AssertionError('unexpected asynchronous suspension in direct handler test')
