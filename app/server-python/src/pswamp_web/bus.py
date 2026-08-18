# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The seam between p-SWAMP's threads and this server's event loop.

p-SWAMP runs each monitoring application as a plain daemon thread looping on a
blocking read, and that is kept exactly as it is: the applications are upstream
code, and rewriting their execution model to suit a web server would be the
tail wagging the dog.

That leaves one problem worth being careful about -- results are produced on a
thread that must never touch the event loop, and consumed by WebSocket handlers
that live on it. This module is the only crossing point, and it crosses via
``loop.call_soon_threadsafe``, which is the one asyncio primitive documented as
safe to call from another thread.

There is exactly one other place application state crosses a thread boundary:
reading a ``TimeWindow`` through its own lock (``snapshot`` / ``get_safe``). Those
two are the whole thread-safety story of this server. Adding a third is how that
stops being reviewable, so don't.
"""

import asyncio
import contextlib
import logging
from collections.abc import Iterator

logger = logging.getLogger("pswamp_web.bus")


class Subscription:
    """One consumer's view of one or more topics.

    Created by :meth:`Bus.subscribe`; lives entirely on the event loop.
    """

    def __init__(
        self, bus: "Bus", topics: tuple[str, ...], latest_only: bool, maxsize: int
    ) -> None:
        self.bus = bus
        self.topics = topics
        self.latest_only = latest_only
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self._dropped = 0

    def offer(self, topic: str, payload: object) -> None:
        """Deliver one message. Runs on the loop, and never blocks it.

        A slow consumer is handled differently depending on what it is reading.
        For state -- a status table, the current islands -- only the newest value
        has any meaning, so the stale one is discarded (``latest_only``). For
        events, such as alarms, every message matters and dropping one silently
        would be a lie, so the queue is bounded and overflow is logged.
        """
        if self.latest_only:
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
        try:
            self._queue.put_nowait((topic, payload))
        except asyncio.QueueFull:
            self._dropped += 1
            if self._dropped % 50 == 1:
                logger.warning(
                    "subscriber on %s is not keeping up; dropped %s messages",
                    ",".join(self.topics),
                    self._dropped,
                )

    async def get(self) -> tuple[str, object]:
        return await self._queue.get()

    def get_nowait(self) -> tuple[str, object] | None:
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None


class Bus:
    """Publish/subscribe fan-out from application threads to WebSocket handlers.

    Topic names follow what the applications already emit -- "status", "alarms" --
    plus a per-application "<key>.result", because several applications publish
    results and the payloads themselves do not all say which one they came from.
    """

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._subscriptions: dict[str, set[Subscription]] = {}
        self._listeners: dict[str, list] = {}

    def add_listener(self, topic: str, callback):
        """Register a synchronous consumer, called on the loop as each message
        arrives. Returns a function that removes it again.

        For consumers that maintain state rather than serve a client: the alarm
        and status stores want every message, in order, with no queue of their
        own to drain. A Subscription would mean a reader task per store existing
        only to move messages from one place to another.
        """
        self._listeners.setdefault(topic, []).append(callback)

        def remove() -> None:
            listeners = self._listeners.get(topic)
            if listeners and callback in listeners:
                listeners.remove(callback)
                if not listeners:
                    del self._listeners[topic]

        return remove

    def bind(self, loop: asyncio.AbstractEventLoop | None) -> None:
        self._loop = loop

    def publish_threadsafe(self, topic: str, payload: object) -> None:
        """Publish from an application thread.

        Silently does nothing before startup or after shutdown. That is
        deliberate: application threads are stopped after the loop has already
        begun tearing down, and a late result is not worth an exception in a
        thread nobody is watching.
        """
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(self._deliver, topic, payload)
        except RuntimeError:
            # Loop closed between the check above and the call.
            pass

    def _deliver(self, topic: str, payload: object) -> None:
        # Runs on the loop. _subscriptions is only ever mutated by subscribe and
        # unsubscribe, which also run on the loop, so no lock is needed here --
        # and adding one would be the third thread-crossing this module exists to
        # prevent.
        for callback in list(self._listeners.get(topic, ())):
            try:
                callback(payload)
            except Exception:
                logger.exception("listener on %s failed", topic)
        for subscription in self._subscriptions.get(topic, ()):
            subscription.offer(topic, payload)

    @contextlib.contextmanager
    def subscribe(
        self, *topics: str, latest_only: bool = False, maxsize: int = 256
    ) -> Iterator[Subscription]:
        subscription = Subscription(self, topics, latest_only, maxsize)
        for topic in topics:
            self._subscriptions.setdefault(topic, set()).add(subscription)
        try:
            yield subscription
        finally:
            for topic in topics:
                listeners = self._subscriptions.get(topic)
                if listeners is not None:
                    listeners.discard(subscription)
                    if not listeners:
                        del self._subscriptions[topic]
