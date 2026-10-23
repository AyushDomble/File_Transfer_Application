"""Serialize nonblocking SSL operations without serializing network waits.

One SSL object belongs to a session. Read/write threads may wait concurrently,
but must not enter that SSL object concurrently. A read polling timeout must
also never become the timeout for an unrelated frame write.
"""
import select
import ssl
import threading
import time


class TLSIO:
    def __init__(self, sock, *, write_timeout):
        self.socket = sock
        self.read_timeout = sock.gettimeout()
        self.write_timeout = write_timeout
        self._lock = threading.Lock()
        sock.setblocking(False)

    @staticmethod
    def _deadline(timeout):
        return None if timeout is None else time.monotonic() + timeout

    def _wait(self, writing, deadline):
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            raise TimeoutError('TLS I/O deadline exceeded')
        # Retry periodically: another SSL operation can change pending state
        # without producing a new OS readiness notification.
        timeout = .05 if remaining is None else min(.05, remaining)
        select.select([] if writing else [self.socket], [self.socket] if writing else [], [], timeout)

    def recv_into(self, buffer, length):
        deadline = self._deadline(self.read_timeout)
        while True:
            try:
                with self._lock:
                    return self.socket.recv_into(buffer, length)
            except ssl.SSLWantReadError:
                self._wait(False, deadline)
            except ssl.SSLWantWriteError:
                self._wait(True, deadline)

    def sendall(self, data):
        deadline = self._deadline(self.write_timeout)
        view = memoryview(data)
        offset = 0
        while offset < len(view):
            # Keep the exact same buffer/length on WANT_READ/WANT_WRITE retries.
            chunk = view[offset:]
            while True:
                try:
                    with self._lock:
                        count = self.socket.send(chunk)
                    if count <= 0:
                        raise ConnectionError('TLS socket stopped accepting bytes')
                    offset += count
                    break
                except ssl.SSLWantReadError:
                    self._wait(False, deadline)
                except ssl.SSLWantWriteError:
                    self._wait(True, deadline)
            if deadline is not None and time.monotonic() >= deadline and offset < len(view):
                raise TimeoutError('TLS frame write deadline exceeded')
