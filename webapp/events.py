"""Ordered, bounded, nonblocking transfer fan-out; one writer per subscriber.

No per-chunk coroutine backlog. A slow subscriber either coalesces progress,
resynchronizes from a fresh authoritative snapshot, or times out independently.
"""
import asyncio
from collections import deque
import threading
import uuid


class Subscription:
    def __init__(self, loop, side, capacity):
        self.loop, self.side, self.capacity = loop, side, capacity
        self.pending = deque()
        self.ready = asyncio.Event()
        self.lock = threading.Lock()
        self.closed = False
        self.scheduled = False

    def put(self, payload):
        if self.side and payload.get('side') not in (self.side, None):
            return
        with self.lock:
            if self.closed:
                return
            if payload['type'] == 'TRANSFER_PROGRESS':
                key = (payload['side'], payload.get('peer_id'), payload.get('scope'), payload['id'])
                # Only replace the trailing progress record for this transfer;
                # never move progress across a lifecycle barrier.
                if self.pending and self.pending[-1]['type'] == 'TRANSFER_PROGRESS':
                    last = self.pending[-1]
                    if (last['side'], last.get('peer_id'), last.get('scope'), last['id']) == key:
                        self.pending.pop()
            if len(self.pending) >= self.capacity:
                self.pending.clear()
                self.pending.append({'type': 'TRANSFER_RESYNC'})
            self.pending.append(payload)
            if not self.scheduled:
                self.scheduled = True
                try:
                    self.loop.call_soon_threadsafe(self._wake)
                except RuntimeError:
                    self.closed = True
                    self.pending.clear()

    def _wake(self):
        with self.lock:
            self.scheduled = False
            self.ready.set()

    async def get(self):
        while True:
            with self.lock:
                if self.closed:
                    return None
                if self.pending:
                    return self.pending.popleft()
                self.ready.clear()
            await self.ready.wait()

    def close(self):
        with self.lock:
            self.closed = True
            self.pending.clear()
        try:
            self.loop.call_soon_threadsafe(self.ready.set)
        except RuntimeError:
            pass


class TransferEvents:
    def __init__(self, capacity=128):
        if capacity < 2:
            raise ValueError('event capacity must be at least two')
        self.capacity = capacity
        self.stream_id = uuid.uuid4().hex
        self.cursor = 0
        self.lock = threading.Lock()
        self.subscribers = set()

    def publish(self, payload):
        with self.lock:
            self.cursor += 1
            payload = {**payload, 'stream_id': self.stream_id, 'event_id': self.cursor}
            for subscriber in tuple(self.subscribers):
                subscriber.put(payload)
        return payload

    def subscribe(self, side=None):
        if side not in (None, 'client', 'host'):
            raise ValueError('side must be client or host')
        subscriber = Subscription(asyncio.get_running_loop(), side, self.capacity)
        with self.lock:
            self.subscribers.add(subscriber)
            cursor = self.cursor
        return subscriber, cursor

    def unsubscribe(self, subscriber):
        with self.lock:
            self.subscribers.discard(subscriber)
        subscriber.close()

    def close(self):
        with self.lock:
            subscribers = list(self.subscribers)
            self.subscribers.clear()
        for subscriber in subscribers:
            subscriber.close()
