# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""One stream's gateway and player, its modules' topics, and the registry that
keeps one per key.

A ``Pipeline`` is the unit of isolation decided in STEP 3 §4.6: one *stream* --
a gateway and a cursor -- keyed by what should be shared. A replay is keyed per
client, because a visitor exploring recorded data wants their own clock; a live
stream would be keyed per stream, so every operator sees the same instant. Same
class, different key.

**What a pipeline is made of** is declared once per app, as a
:class:`PipelineFamily`: the app's name (the namespace of its topics), how to
build its gateway, and its module classes. The server builds one ``Pipeline``
per key from it; a worker hosts the family's modules
(:mod:`pswamp_core.host`). The two sides meet only on the transport::

    DATA DOWN    gateway ── Player ── publish ──▶ topic <app>.pmu.frame, key ──▶ Module (hosted)
                                                                                    │
                 pipeline.latest ◀── topic <app>.<result>, key ◀────────────────────┘
    COMMANDS UP  edge ── dispatch ── player command: player.validate (the 409)
                                  ──▶ topic <app>.<command>, key ──▶ Player | Module
    STATE        the edge reads pipeline.latest and the player, woken by pipeline.changes()

**The player lives here, with the edge**: its commands are checked before they
are published (a refusal is the POST's 409), and its frames and status are
read straight off it. It still takes its commands off their topic, so a module
can send it one (``SwitchSourceCommand``) exactly as the edge does.

**The local view is not a bus.** It is ``latest`` -- the newest message of each
class the player published or a module answered -- and ``changes()``, a
wake-up that coalesces: an endpoint that wakes builds its message from the
current state, so however much arrived meanwhile it sends one.

``PipelineRegistry`` builds a pipeline on first ``acquire`` through a factory,
keeps it alive across reconnects, evicts it after an idle grace period or -- at
the cap -- the least-recently-used one nobody is watching, and refuses when
none can be reclaimed. Nothing here knows what a WebSocket is; the edge maps
``CapacityError`` to a close code.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from .command_routing import CommandInbox, NoReceiver, concrete_commands
from .datagateway.player import PLAYER_COMMANDS, Player
from .log import get_logger
from .messages.commands import Command
from .messages.control import PipelineClosed
from .messages.errors import ErrorEvent
from .messages.pmu import PmuFrame
from .subscription import Overflow
from .transport import Outbox
from .util.tasks import cancel_and_wait, finish
from .util.time import utcnow

if TYPE_CHECKING:
    from .datagateway.data_gateway import DataGateway
    from .messages.data_model import DataModel
    from .messages.results import ResultEnvelope
    from .modules import Module
    from .transport import Transport, TransportSubscription

__all__ = [
    "DEFAULT_IDLE_EVICT_SECONDS",
    "DEFAULT_MAX_PIPELINES",
    "CapacityError",
    "Latest",
    "Pipeline",
    "PipelineFamily",
    "PipelineRegistry",
]

logger = get_logger("pswamp_core.pipeline")

DEFAULT_MAX_PIPELINES = 8
DEFAULT_IDLE_EVICT_SECONDS = 300.0

#: How long a pipeline waits for its topics' feeds to be consuming at start.
_READY_TIMEOUT_S = 20.0

M = TypeVar("M", bound="DataModel")


class CapacityError(RuntimeError):
    """Every slot is held by a pipeline someone is still watching."""


@dataclass(frozen=True)
class PipelineFamily:
    """What every pipeline of one app is made of, declared once.

    Attributes:
        app: The app's name: the namespace of its topics
            (``<app>.pmu.frame``) and the slug on its error reports.
        gateway: Builds a fresh gateway -- the server's player reads one, and
            a host hands one to each module instance that wants it.
        modules: The module classes, hosted wherever the transport says.

    Raises ``ValueError`` when two receivers -- the player, or a module --
    declare the same command class, or a module declares a base class: a
    command class is an address, and a topic carries one class.
    """

    app: str
    gateway: Callable[[], DataGateway]
    modules: tuple[type[Module], ...] = ()

    def __post_init__(self) -> None:
        seen: dict[type[Command], str] = {command: "the player" for command in PLAYER_COMMANDS}
        for module in self.modules:
            for command in concrete_commands(module.__name__, module.commands):
                if command in seen:
                    raise ValueError(
                        f"{self.app}: {module.__name__} and {seen[command]} both take {command.__name__}"
                    )
                seen[command] = module.__name__

    @property
    def inputs(self) -> frozenset[type[DataModel]]:
        """What the modules read, and so what a pipeline publishes to them."""
        return frozenset(m.input_model for m in self.modules if m.input_model is not None)

    @property
    def results(self) -> tuple[type[ResultEnvelope], ...]:
        """What the modules publish, and so what a pipeline listens for."""
        return tuple(dict.fromkeys(m.output_model for m in self.modules))

    def module_for(self, command: type[Command]) -> type[Module] | None:
        return next((m for m in self.modules if command in m.commands), None)


class Latest:
    """The newest message of each class seen: what an endpoint renders from."""

    def __init__(self) -> None:
        self._by_type: dict[type[DataModel], DataModel] = {}

    def remember(self, message: DataModel) -> None:
        self._by_type[type(message)] = message

    def get(self, model: type[M]) -> M | None:
        """The newest message of exactly ``model``, or ``None``."""
        return self._by_type.get(model)  # type: ignore[return-value]


class Changes:
    """Wake-ups for one reader: each ``async for`` step means "something
    changed since you last looked". Registered from construction, so a reader
    that snapshots right after opening one misses nothing; many changes while
    it was busy are one wake-up."""

    def __init__(self, waiters: set[asyncio.Event]) -> None:
        self._waiters = waiters
        self._event = asyncio.Event()
        waiters.add(self._event)

    def __aiter__(self) -> Changes:
        return self

    async def __anext__(self) -> None:
        await self._event.wait()
        self._event.clear()

    async def wait(self, timeout: float | None = None) -> bool:
        """Wait for a change, at most ``timeout``; whether one came."""
        try:
            await asyncio.wait_for(self._event.wait(), timeout)
        except TimeoutError:
            return False
        self._event.clear()
        return True

    def close(self) -> None:
        self._waiters.discard(self._event)

    def __enter__(self) -> Changes:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class Pipeline:
    """One stream: a gateway, a player, and the topics of its family's modules.

    Args:
        key: What this pipeline is isolated by -- a client id for a replay.
        family: What it is made of.
        transport: The process's transport.
        model: The message class the player streams.
        **player_options: Passed to the ``Player`` (``loop``, ``autoplay``, ...).
    """

    def __init__(
        self,
        key: str,
        family: PipelineFamily,
        transport: Transport,
        *,
        model: type[DataModel] = PmuFrame,
        **player_options: Any,
    ) -> None:
        self.key = key
        self.family = family
        self.transport = transport
        self.gateway = family.gateway()
        self.latest = Latest()
        self.outbox = Outbox(
            transport,
            app=family.app,
            key=key,
            keep_up=_publisher_keep_up(family),
            source=family.app,
            label=f"the server-side publisher for {family.app}",
        )
        self.player = Player(self.gateway, self, model=model, **player_options)
        self._waiters: set[asyncio.Event] = set()
        self._inbox: CommandInbox | None = None
        self._subscriptions: list[TransportSubscription] = []
        self._tasks: list[asyncio.Task] = []
        self._started = False

    # -- the player's sink, and the local view -------------------------------------

    def publish(self, message: DataModel) -> None:
        """What the player publishes: remembered and announced here, and put on
        the transport when a module reads it or it reports an error."""
        self._remember(message)
        if type(message) in self.family.inputs or isinstance(message, ErrorEvent):
            self.outbox.publish(message)

    def changes(self) -> Changes:
        """Wake-ups on every change here; close it (or use ``with``) when done."""
        return Changes(self._waiters)

    def _remember(self, message: DataModel) -> None:
        self.latest.remember(message)
        for event in self._waiters:
            event.set()

    # -- commands ---------------------------------------------------------------------

    def dispatch(self, command: Command) -> None:
        """Route ``command`` by its class and publish it on its topic.

        Synchronous, so the caller learns before anything happens: a player
        command the player refuses now raises ``CommandRefused`` (the 409), and
        a command nothing here takes raises ``NoReceiver``; neither is
        published. A module command is accepted as it is: the module checks it
        where it runs, and a refusal returns as an ``ErrorEvent``.
        """
        if type(command) in PLAYER_COMMANDS:
            self.player.validate(command)
        elif self.family.module_for(type(command)) is None:
            raise NoReceiver(f"{self.family.app} has no receiver for {type(command).__name__}")
        self.outbox.publish(command)

    # -- lifecycle ----------------------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        app, key = self.family.app, self.key
        commands = self.transport.subscribe(*PLAYER_COMMANDS, app=app, key=key, overflow=Overflow.GROW)
        self._subscriptions.append(commands)
        if self.family.results:
            results = self.transport.subscribe(
                *self.family.results, app=app, key=key, overflow=Overflow.DROP_OLDEST, maxsize=64
            )
            self._subscriptions.append(results)
            self._tasks.append(asyncio.create_task(self._receive(results), name=f"{app}@{key}.results"))
        # A command published the moment start() returns must find the feed
        # consuming: a broker's consumer joins a topic at its end.
        for subscription in self._subscriptions:
            await subscription.ready(_READY_TIMEOUT_S)
        self.outbox.start()
        await self.gateway.open()
        await self.player.start()
        self._inbox = CommandInbox((c async for _, c in commands), self.player, self)
        self._inbox.start()

    async def stop(self, reason: str = "stopped") -> None:
        if not self._started:
            return
        self._started = False
        # To its end even if the caller is cancelled meanwhile (the registry's
        # shutdown cancels an idle eviction that may be half way through this).
        await finish(self._teardown(reason))

    async def _teardown(self, reason: str) -> None:
        if self._inbox is not None:
            await self._inbox.stop()
            self._inbox = None
        await self.player.stop()
        await cancel_and_wait(*self._tasks)
        self._tasks = []
        for subscription in self._subscriptions:
            subscription.close()
        self._subscriptions = []
        self.outbox.publish(PipelineClosed(timestamp=utcnow(), reason=reason))
        await self.outbox.close()
        await self.gateway.close()

    async def _receive(self, results: TransportSubscription) -> None:
        async for _key, result in results:
            self._remember(result)


def _publisher_keep_up(family: PipelineFamily):
    """The outbox reports falling behind under the policy of the modules it
    feeds; none when no module wants reports."""
    policies = [m.keep_up for m in family.modules if m.input_model is not None and m.keep_up is not None]
    return policies[0] if policies else None


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
