"""Bounded, thread-safe fixed-window admission limits (monotonic clock)."""
from collections import OrderedDict, deque
import threading
import time


class AttemptLimiter:
    def __init__(self, per_source=5, global_limit=60, window=60, capacity=1024):
        self.per_source, self.global_limit = per_source, global_limit
        self.window, self.capacity = window, capacity
        self.sources, self.global_attempts = OrderedDict(), deque()
        self.lock = threading.Lock()

    def allow(self, source):
        now = time.monotonic()
        with self.lock:
            while self.global_attempts and self.global_attempts[0] <= now - self.window:
                self.global_attempts.popleft()
            times = self.sources.get(source, deque())
            while times and times[0] <= now - self.window:
                times.popleft()
            if len(times) >= self.per_source or len(self.global_attempts) >= self.global_limit:
                return False
            times.append(now)
            self.global_attempts.append(now)
            self.sources[source] = times
            self.sources.move_to_end(source)
            while len(self.sources) > self.capacity:
                self.sources.popitem(last=False)
            return True
