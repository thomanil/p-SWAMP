# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""In-process publish/subscribe, typed on message classes.

The bus is what decouples a producer from its consumers inside one process: the
player publishes frames, a module publishes results, the player publishes its
status; a page endpoint, a module or a store subscribes to the *classes* it
cares about and never learns who wrote them. A subscription to a base class
receives every subclass -- subscribe to ``ResultEnvelope`` and every module's
result arrives.

Adapted from ``app/server-python/src/pswamp_web/bus.py``, the web proof of
concept's bus, with three changes: topics are model classes rather than strings
(so the topic catalogue is the set of ``DataModel`` subclasses, and nothing has
to be spelled twice); a ``Subscription`` is an async iterator; and the overflow
policy is explicit and per subscription (port doc §13 -- live data and a
completeness-first replay want different answers to "the consumer is slow").

**The thread seam.** ``publish`` is for callers already on the event loop.
``publish_threadsafe`` is the one way a *thread* may put something on the bus:
it hops onto the loop with ``call_soon_threadsafe``, the one asyncio primitive
documented as safe from another thread. Nothing in the slice publishes from a
thread yet; the seam exists so that the desktop package's application threads
can, without a second mechanism appearing when they do.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from enum import Enum
from typing import TYPE_CHECKING, Protocol, TypeVar

from ..log import get_logger

if TYPE_CHECKING:
    from ..messages.data_model import DataModel

__all__ = ["Bus", "InProcessBus", "Latest", "Overflow", "Subscription"]

logger = get_logger("pswamp_core.bus")

M = TypeVar("M", bound="DataModel")

# Sentinel a closed subscription puts on its own queue so a pending reader wakes.
_CLOSED = object()


class Overflow(str, Enum):
    """What a subscription does when its consumer is not keeping up.

    ``DROP_OLDEST`` -- a bounded queue; the oldest unread message makes room for
    the newest. Right for a live stream: a stale frame delivered late is worse
    than one never delivered. ``LATEST_ONLY`` -- only the newest message is ever
    held; right for *state* (a status table, the current islands), where only
    the latest value means anything. ``GROW`` -- unbounded; right for a
    completeness-first replay or for commands, where every message matters and
    the producer is known to be finite.
    """

    DROP_OLDEST = "drop_oldest"
    LATEST_ONLY = "latest_only"
    GROW = "grow"


class Subscription:
    """One consumer's queue over one or more message classes.

    Created by :meth:`InProcessBus.subscribe`; lives entirely on the event loop.
    Iterate it (``async for message in subscription``), or poll with
    :meth:`get_nowait`. Closing it detaches it from the bus and ends the
    iteration; it is also a context manager for exactly that.
    """

    def __init__(
        self,
        bus: InProcessBus,
        models: tuple[type[DataModel], ...],
        overflow: Overflow,
        maxsize: int,
    ) -> None:
        self._bus = bus
        self.models = models
        self.overflow = overflow
        self._queue: asyncio.Queue = asyncio.Queue(
            maxsize=0 if overflow is Overflow.GROW else maxsize
        )
        self._dropped = 0
        self._closed = False

    @property
    def dropped(self) -> int:
        """Messages discarded because this consumer fell behind."""
        return self._dropped

    def matches(self, message: DataModel) -> bool:
        return isinstance(message, self.models)

    def offer(self, message: DataModel) -> None:
        """Deliver one message. Runs on the loop, and never blocks it."""
        if self._closed:
            return
        if self.overflow is Overflow.LATEST_ONLY:
            self._drain()
        elif self.overflow is Overflow.DROP_OLDEST and self._queue.full():
            self._drop_one()
        self._queue.put_nowait(message)

    def _drain(self) -> None:
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    def _drop_one(self) -> None:
        try:
            self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        self._dropped += 1
        if self._dropped % 50 == 1:
            logger.warning(
                "subscriber on %s is not keeping up; dropped %s messages",
                ",".join(model.__name__ for model in self.models),
                self._dropped,
            )

    async def get(self) -> DataModel:
        """The next message; raises ``StopAsyncIteration`` once closed."""
        item = await self._queue.get()
        if item is _CLOSED:
            self._queue.put_nowait(_CLOSED)
            raise StopAsyncIteration
        return item

    def get_nowait(self) -> DataModel | None:
        """The next message without waiting, or ``None`` if there is none."""
        try:
            item = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None
        if item is _CLOSED:
            self._queue.put_nowait(_CLOSED)
            return None
        return item

    def __aiter__(self) -> Subscription:
        return self

    async def __anext__(self) -> DataModel:
        return await self.get()

    def close(self) -> None:
        """Detach from the bus and wake any pending reader with the end."""
        if self._closed:
            return
        self._closed = True
        self._bus._detach(self)
        try:
            self._queue.put_nowait(_CLOSED)
        except asyncio.QueueFull:
            self._drop_one()
            self._queue.put_nowait(_CLOSED)

    def __enter__(self) -> Subscription:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class Bus(Protocol):
    """What a publisher or subscriber needs from a bus. ``InProcessBus`` is the
    one implementation today; a broker-backed one would present the same face."""

    def publish(self, message: DataModel) -> None: ...

    def publish_threadsafe(self, message: DataModel) -> None: ...

    def subscribe(
        self,
        *models: type[DataModel],
        overflow: Overflow = Overflow.DROP_OLDEST,
        maxsize: int = 256,
    ) -> Subscription: ...

    def add_listener(
        self, model: type[DataModel], callback: Callable[[DataModel], None]
    ) -> Callable[[], None]: ...


class InProcessBus:
    """Publish/subscribe fan-out within one event loop.

    Everything here runs on the loop, so the subscription and listener lists
    need no lock -- and adding one would be the second thread-crossing this
    module exists to prevent. ``publish_threadsafe`` is the only entry from a
    thread, and it does nothing but schedule ``_deliver`` on the loop.
    """

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._subscriptions: list[Subscription] = []
        self._listeners: list[tuple[type[DataModel], Callable[[DataModel], None]]] = []

    def bind(self, loop: asyncio.AbstractEventLoop | None) -> None:
        """Name the loop ``publish_threadsafe`` hops onto. ``None`` at shutdown."""
        self._loop = loop

    @property
    def subscriber_count(self) -> int:
        return len(self._subscriptions)

    def add_listener(
        self, model: type[DataModel], callback: Callable[[DataModel], None]
    ) -> Callable[[], None]:
        """Register a synchronous consumer, called on the loop as each message
        of ``model`` (or a subclass) arrives. Returns a function that removes it.

        For consumers that maintain state rather than serve a client -- a store,
        or :class:`Latest` -- which want every message in order and no queue of
        their own to drain.
        """
        entry = (model, callback)
        self._listeners.append(entry)

        def remove() -> None:
            if entry in self._listeners:
                self._listeners.remove(entry)

        return remove

    def subscribe(
        self,
        *models: type[DataModel],
        overflow: Overflow = Overflow.DROP_OLDEST,
        maxsize: int = 256,
    ) -> Subscription:
        """A queue fed with every message that is an instance of any of ``models``."""
        if not models:
            raise ValueError("subscribe needs at least one model class")
        subscription = Subscription(self, models, overflow, maxsize)
        self._subscriptions.append(subscription)
        return subscription

    def publish(self, message: DataModel) -> None:
        """Deliver ``message`` to every listener and subscription. Loop only."""
        self._deliver(message)

    def publish_threadsafe(self, message: DataModel) -> None:
        """Publish from a thread.

        Silently does nothing before ``bind`` or after shutdown. That is
        deliberate: a thread is stopped after the loop has already begun tearing
        down, and a late message is not worth an exception in a thread nobody is
        watching.
        """
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(self._deliver, message)
        except RuntimeError:
            # Loop closed between the check above and the call.
            pass

    def _deliver(self, message: DataModel) -> None:
        # Listeners first, so a store or Latest is current by the time a
        # subscription's reader wakes and looks at it.
        for model, callback in list(self._listeners):
            if isinstance(message, model):
                try:
                    callback(message)
                except Exception:
                    logger.exception("listener on %s failed", model.__name__)
        for subscription in list(self._subscriptions):
            if subscription.matches(message):
                subscription.offer(message)

    def _detach(self, subscription: Subscription) -> None:
        if subscription in self._subscriptions:
            self._subscriptions.remove(subscription)


class Latest:
    """The most recent message of each class seen on a bus.

    What a freshly connected socket renders from before the next message
    arrives, and what a command handler reads to answer "where are we". Attach
    to a bus; detach when the bus is done.
    """

    def __init__(self, bus: InProcessBus, *models: type[DataModel]) -> None:
        from ..messages.data_model import DataModel as _DataModel

        self._by_type: dict[type[DataModel], DataModel] = {}
        watched = models or (_DataModel,)
        self._detach = [bus.add_listener(model, self._remember) for model in watched]

    def _remember(self, message: DataModel) -> None:
        # Re-insert so dict order is recency, which get() relies on.
        self._by_type.pop(type(message), None)
        self._by_type[type(message)] = message

    def get(self, model: type[M]) -> M | None:
        """The newest message that is an instance of ``model``, or ``None``."""
        found: M | None = None
        for kind, message in self._by_type.items():
            if issubclass(kind, model):
                found = message  # type: ignore[assignment]
        return found

    def detach(self) -> None:
        for remove in self._detach:
            remove()
        self._detach = []
