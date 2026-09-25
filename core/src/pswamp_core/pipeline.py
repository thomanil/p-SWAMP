# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""One stream's player, bus and modules, and the registry that keeps them per key.

A ``Pipeline`` is the unit of isolation decided in STEP 3 §4.6: one *stream* --
a gateway and a cursor -- with its player, its bus and its module instances.
What the key is decides what is shared: a replay is keyed per client, because a
visitor exploring recorded data wants their own clock; a live stream would be
keyed per stream, so every operator sees the same instant. Same class, different
key.

``PipelineRegistry`` is ``HubRegistry`` from the web proof of concept
(``app/server-python/src/pswamp_web/hub.py``) moved down and generalised: it
builds a pipeline on first ``acquire`` through a factory it is given, keeps it
alive across reconnects, evicts it after an idle grace period or -- at the cap --
the least-recently-used one nobody is watching, and refuses when none can be
reclaimed. Nothing here knows what a WebSocket is; the edge maps
``CapacityError`` to a close code.

**A pipeline is where commands are routed.** Its receivers are the player and
every module that declares ``commands``; ``start`` opens one inbox per receiver
before anything runs, and ``dispatch`` is the one way in -- route by class,
check, publish (:mod:`pswamp_core.command_routing`). The edge calls
``pipeline.dispatch(command)`` and maps ``CommandRefused`` to a 409.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Generic, TypeVar

from .bus import Latest
from .command_routing import CommandInbox, CommandReceiver, dispatch
from .log import get_logger

if TYPE_CHECKING:
    from .bus import InProcessBus
    from .messages.commands import Command
    from .datagateway.data_gateway import DataGateway
    from .datagateway.player import Player
    from .modules import Module

__all__ = [
    "DEFAULT_IDLE_EVICT_SECONDS",
    "DEFAULT_MAX_PIPELINES",
    "CapacityError",
    "Pipeline",
    "PipelineRegistry",
]

logger = get_logger("pswamp_core.pipeline")

DEFAULT_MAX_PIPELINES = 8
DEFAULT_IDLE_EVICT_SECONDS = 300.0


class CapacityError(RuntimeError):
    """Every slot is held by a pipeline someone is still watching."""


class Pipeline:
    """One stream: a gateway, a bus, a player and the modules reading the bus.

    Args:
        key: What this pipeline is isolated by -- a client id for a replay.
        gateway: The providers.
        bus: The in-process bus everything on this pipeline publishes to.
        player: Paces the gateway stream onto the bus.
        modules: Started as one task each, before the player.

    Raises ``ValueError`` when two receivers answer the same command class
    under the same name, since no ``target`` could then tell them apart.
    """

    def __init__(
        self,
        key: str,
        gateway: DataGateway,
        bus: InProcessBus,
        player: Player,
        modules: Sequence[Module] = (),
    ) -> None:
        self.key = key
        self.gateway = gateway
        self.bus = bus
        self.player = player
        self.modules = list(modules)
        #: Everything here that takes commands: the player, then the modules
        #: that declare any.
        self.receivers: list[CommandReceiver] = [player] + [m for m in self.modules if m.commands]
        names = [r.name for r in self.receivers]
        if len(names) != len(set(names)):
            raise ValueError(f"two command receivers share a name in pipeline {key}: {names}")
        #: The newest message of each class on this bus; attached at start().
        self.latest: Latest | None = None
        self._tasks: list[asyncio.Task] = []
        self._inboxes: list[CommandInbox] = []
        self._started = False

    def dispatch(self, command: Command) -> None:
        """Route ``command`` to its receiver, check it, and publish it.

        Synchronous, so the caller learns before anything happens: raises
        ``CommandRefused`` when the receiver refuses it now and ``RoutingError``
        when nothing here takes it; publishes nothing in either case.
        """
        dispatch(self.bus, self.receivers, command)

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self.bus.bind(asyncio.get_running_loop())
        self.latest = Latest(self.bus)
        # Inboxes first: each subscribes as it is built, so a command published
        # the moment start() returns reaches its receiver.
        self._inboxes = [
            CommandInbox(self.bus, receiver) if receiver is self.player else receiver.command_inbox(self.bus)
            for receiver in self.receivers
        ]
        await self.gateway.open()
        for module in self.modules:
            await module.setup(self.gateway, self.bus)
            self._tasks.append(
                asyncio.create_task(module.run(self.bus), name=f"{self.key}.{module.name}")
            )
        await self.player.start()
        for inbox in self._inboxes:
            inbox.start()

    async def stop(self) -> None:
        if not self._started:
            return
        for inbox in self._inboxes:
            await inbox.stop()
        self._inboxes = []
        await self.player.stop()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks = []
        if self.latest is not None:
            self.latest.detach()
        await self.gateway.close()
        self.bus.bind(None)
        self._started = False


P = TypeVar("P", bound=Pipeline)

#: Builds a pipeline for a key. May be a coroutine function or a plain one.
PipelineFactory = Callable[[str], "Awaitable[P] | P"]


class _Entry(Generic[P]):
    def __init__(self, pipeline: P) -> None:
        self.pipeline = pipeline
        self.sockets = 0
        self.last_used = time.monotonic()
        self.evict_task: asyncio.Task | None = None


class PipelineRegistry(Generic[P]):
    """Builds, caps and evicts pipelines per key.

    * **One pipeline per key, however many sockets.** A per-key lock brackets
      the build, so five simultaneous first-connects from one client make one
      pipeline rather than racing to build five.
    * **A pipeline outlives its sockets, briefly.** Closing the last one starts
      an idle timer rather than tearing down, so a reload rejoins the same
      stream. Reconnecting cancels the timer.
    * **Never more than the cap, even mid-build.** At the cap a new key reclaims
      the least-recently-used pipeline nobody is watching; if every one is in
      use, ``CapacityError``. Pipelines still under construction count against
      the cap too (``_pending``), so a burst of distinct keys cannot each see
      room and overshoot it together.

    Everything runs on the event loop, so ``_entries`` needs no lock of its own;
    the per-key locks exist to bracket the *awaits* inside ``acquire``.
    """

    def __init__(
        self,
        factory: PipelineFactory[P],
        *,
        max_pipelines: int = DEFAULT_MAX_PIPELINES,
        idle_seconds: float = DEFAULT_IDLE_EVICT_SECONDS,
    ) -> None:
        self._factory = factory
        self._entries: dict[str, _Entry[P]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        # In-flight acquire() calls per key, incremented *before* the key's lock
        # is taken so a caller merely queued on the lock counts too. A key's lock
        # is reclaimable only while this is zero; see _drop_lock_if_unused.
        self._acquiring: dict[str, int] = {}
        # Pipelines past the capacity check but not yet in _entries.
        self._pending = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self.max_pipelines = max_pipelines
        self.idle_seconds = idle_seconds

    def bind(self, loop: asyncio.AbstractEventLoop | None) -> None:
        self._loop = loop

    @property
    def live(self) -> int:
        return len(self._entries)

    def keys(self) -> list[str]:
        """The keys with a live pipeline."""
        return list(self._entries)

    def watchers(self, key: str) -> int:
        """How many sockets currently hold ``key``'s pipeline."""
        entry = self._entries.get(key)
        return 0 if entry is None else entry.sockets

    def peek(self, key: str) -> P | None:
        """This key's pipeline if it already has one, else ``None``.

        A pure read: no socket count, no idle timer, nothing constructed. It is
        what a *command* uses -- a command must never build a pipeline.
        """
        entry = self._entries.get(key)
        return None if entry is None else entry.pipeline

    @contextlib.asynccontextmanager
    async def session(self, key: str) -> AsyncIterator[P]:
        """Hold a key's pipeline for the life of one connection."""
        pipeline = await self.acquire(key)
        try:
            yield pipeline
        finally:
            self.release(key)

    async def acquire(self, key: str) -> P:
        self._acquiring[key] = self._acquiring.get(key, 0) + 1
        try:
            lock = self._locks.setdefault(key, asyncio.Lock())
            async with lock:
                entry = self._entries.get(key)
                if entry is not None:
                    if entry.evict_task is not None:
                        entry.evict_task.cancel()
                        entry.evict_task = None
                    entry.sockets += 1
                    entry.last_used = time.monotonic()
                    return entry.pipeline

                await self._make_room()

                self._pending += 1
                try:
                    pipeline = await self._build(key)
                    entry = _Entry(pipeline)
                    entry.sockets = 1
                    self._entries[key] = entry
                finally:
                    self._pending -= 1
                logger.info(
                    "pipeline started for %s (%s/%s live)", key, self.live, self.max_pipelines
                )
                return pipeline
        finally:
            self._acquiring[key] -= 1
            self._drop_lock_if_unused(key)

    async def _build(self, key: str) -> P:
        built = self._factory(key)
        pipeline = await built if inspect.isawaitable(built) else built
        try:
            await pipeline.start()
        except BaseException:
            with contextlib.suppress(Exception):
                await pipeline.stop()
            raise
        return pipeline

    def release(self, key: str) -> None:
        entry = self._entries.get(key)
        if entry is None:
            return
        entry.sockets = max(0, entry.sockets - 1)
        entry.last_used = time.monotonic()
        if entry.sockets == 0 and entry.evict_task is None:
            entry.evict_task = asyncio.create_task(self._evict_when_idle(key))

    async def stop_all(self) -> None:
        """Tear down every pipeline, concurrently, on process shutdown."""
        keys = list(self._entries)
        for key in keys:
            entry = self._entries.get(key)
            if entry is not None and entry.evict_task is not None:
                entry.evict_task.cancel()
        entries = [self._entries.pop(key) for key in keys]
        self._locks.clear()
        if not entries:
            return
        await asyncio.gather(
            *(entry.pipeline.stop() for entry in entries), return_exceptions=True
        )
        logger.info("stopped %s pipeline(s)", len(entries))

    # -- internals -------------------------------------------------------------

    async def _make_room(self) -> None:
        """Free a slot if at the cap. Caller holds the new key's lock.

        Evicts *another* key's pipeline while holding ours; deadlock-free because
        it never waits on the victim's lock -- ``Pipeline.stop`` is idempotent, so
        racing with the victim's own idle evictor is harmless.
        """
        while self.live + self._pending >= self.max_pipelines:
            victim = min(
                (key for key, entry in self._entries.items() if entry.sockets == 0),
                key=lambda key: self._entries[key].last_used,
                default=None,
            )
            if victim is None:
                raise CapacityError(f"all {self.max_pipelines} pipelines are in use")
            await self._evict(victim, "capacity")

    async def _evict_when_idle(self, key: str) -> None:
        try:
            await asyncio.sleep(self.idle_seconds)
        except asyncio.CancelledError:
            return
        lock = self._locks.get(key)
        if lock is None:
            return
        async with lock:
            entry = self._entries.get(key)
            if entry is None or entry.sockets > 0:
                return
            await self._evict(key, "idle")

    async def _evict(self, key: str, reason: str) -> None:
        entry = self._entries.pop(key, None)
        self._drop_lock_if_unused(key)
        if entry is None:
            return
        if entry.evict_task is not None:
            entry.evict_task = None
        await entry.pipeline.stop()
        logger.info(
            "pipeline evicted for %s (%s), %s/%s live", key, reason, self.live, self.max_pipelines
        )

    def _drop_lock_if_unused(self, key: str) -> None:
        """Reclaim a key's lock once no acquire is in flight and no pipeline lives."""
        if self._acquiring.get(key, 0) <= 0:
            self._acquiring.pop(key, None)
            if key not in self._entries:
                self._locks.pop(key, None)
