"""Deterministic byte-stream test double; NOT TCP/TLS or OS socket coverage."""
import socket
import threading
import time


class MemorySocket:
    def __init__(self):
        self._cv = threading.Condition()
        self._buffer = bytearray()
        self._closed = False
        self._peer_closed = False
        self._timeout = None
        self.peer = None

    def settimeout(self, value):
        self._timeout = value

    def gettimeout(self):
        return self._timeout

    def sendall(self, data):
        if self._closed:
            raise OSError('closed memory stream')
        with self.peer._cv:
            if self.peer._closed:
                raise BrokenPipeError('peer closed memory stream')
            self.peer._buffer.extend(data)
            self.peer._cv.notify_all()

    def recv_into(self, target, length=0):
        length = length or len(target)
        deadline = None if self._timeout is None else time.monotonic() + self._timeout
        with self._cv:
            while not self._buffer:
                if self._closed or self._peer_closed:
                    return 0
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise socket.timeout('memory stream timeout')
                self._cv.wait(remaining)
            count = min(length, len(self._buffer))
            target[:count] = self._buffer[:count]
            del self._buffer[:count]
            return count

    def recv(self, length):
        result = bytearray(length)
        return bytes(result[:self.recv_into(result)])

    def shutdown(self, how=socket.SHUT_RDWR):
        self.close()

    def close(self):
        with self._cv:
            self._closed = True
            self._cv.notify_all()
        with self.peer._cv:
            self.peer._peer_closed = True
            self.peer._cv.notify_all()


def socketpair(*args, **kwargs):
    first, second = MemorySocket(), MemorySocket()
    first.peer, second.peer = second, first
    return first, second
