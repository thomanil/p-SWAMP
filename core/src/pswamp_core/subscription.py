# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A consumer's queue, and what it does when the consumer is slow.

Every subscription in the core -- to a transport's topics, to a pipeline's
local view -- is one of these: an async-iterable queue on the event loop with
an explicit :class:`Overflow` policy, because a live stream and a
completeness-first consumer want different answers to "the consumer is slow".
Whoever routes messages into it (its *owner*) calls :meth:`Subscription.offer`
and is told on :meth:`Subscription.close`.
"""

from __future__ import annotations

import asyncio
from enum import Enum
from typing import TYPE_CHECKING, Protocol

from .log import get_logger

if TYPE_CHECKING:
    from .messages.data_model import DataModel

__all__ = ["Overflow", "Sink", "Subscription"]

logger = get_logger("pswamp_core.subscription")


class _Owner(Protocol):
    def _detach(self, subscription: Subscription) -> None: ...


class Sink(Protocol):
    """Anything a message can be handed to, synchronously, on the loop: what a
    player, a module or a monitor publishes into."""

    def publish(self, message: DataModel) -> None: ...


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

    Created by whoever routes messages to it (its *owner*: a transport, a
    pipeline's local view), which offers it each message it wants and is told
    when it closes. Lives entirely on the event loop. Iterate it (``async for
    message in subscription``), or poll with :meth:`get_nowait`. Closing it
    detaches it from its owner and ends the iteration; it is also a context
    manager for exactly that.
    """

    def __init__(
        self,
        owner: _Owner,
        models: tuple[type[DataModel], ...],
        overflow: Overflow,
        maxsize: int,
    ) -> None:
        self._owner = owner
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
        """Detach from the owner and wake any pending reader with the end."""
        if self._closed:
            return
        self._closed = True
        self._owner._detach(self)
        try:
            self._queue.put_nowait(_CLOSED)
        except asyncio.QueueFull:
            self._drop_one()
            self._queue.put_nowait(_CLOSED)

    def __enter__(self) -> Subscription:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
