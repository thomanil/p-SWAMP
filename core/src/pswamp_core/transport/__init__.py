# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Keyed publish/subscribe between processes: the topics a module is reached on.

A module consumes one message class and publishes another
(:mod:`pswamp_core.modules`); a transport carries those messages between the
pipeline and the process the module runs in -- **one topic per message class,
namespaced by the app, and the pipeline key on every record**::

    await transport.publish(frame, app="pmu-test-streamer", key="42")   # topic pmu-test-streamer.pmu.frame
    with transport.subscribe(FrameStatsResult, app="pmu-test-streamer", key="42") as results:
        async for key, result in results: ...                          # only key "42"
    with transport.subscribe(PmuFrame, app="pmu-test-streamer") as frames:
        async for key, frame in frames: ...                            # every key: a worker's view

**The app is part of the topic.** Two apps whose modules read the same class
(``PmuFrame``) under the same browser client id would otherwise hear each
other's frames; ``<app>.<model.topic>`` keeps them apart without any
configuration.

**A topic carries exactly one class.** A subscriber to ``ResultEnvelope`` hears
nothing a subclass publishes: a broker has no notion of subclasses, and the
in-memory transport behaves the same way on purpose.

**The in-memory transport is honest.** Every message it delivers went through
JSON (``model_dump_json`` then ``model_validate_json``), as a broker's does, so
a message that would not survive the wire fails in a hermetic test rather than
in the deployment. Subscribers receive an equal object, never the published
one.

**A transport is not a provider.** A transport carries whatever was published,
in order, and never looks at a timestamp -- a replay whose timestamps jump back
at every loop crosses it unharmed. A broker can *also* be a data source (a
topic's retention as history, its tail as live); that is a ``DataClient``, a
different class for a different job.

**The key is transport metadata, not a message field.** A pipeline keyed per
client publishes under that client id; the worker on the other side runs one
module instance per key it sees.

**A subscriber hears only what is said after it subscribes.** A transport
keeps nothing and replays nothing: a frame carries its own layout
(``PmuFrame.header``), so a worker that starts late is primed by the first
frame it sees.

What a process listens to is fanned out from one feed: ``subscribe`` offers
each message to the subscriber queues that want its topic and key, each with
its own :class:`~pswamp_core.subscription.Overflow` policy, so eight pipelines
tailing their results cost one feed, not eight. (The Kafka transport reads
every topic its process listens to with a single consumer.)

Concrete transports: :class:`InMemoryTransport` here (no port; one instance
shared by two sides *is* the broker -- the single-process deployment, and every
hermetic test), and :class:`pswamp_core.transport.kafka.KafkaTransport` behind
the ``kafka`` extra. **The deployment picks one, and nothing else changes**:
``PSWAMP_TRANSPORT`` names it -- ``name:module.path:ClassName`` plus a
``{NAME}_{SETTING}`` block, as a provider is configured -- and unset it is the
in-memory one, with the module hosts in the server's own process
(:func:`transport_from_env`).

Publishers are synchronous (the player, a module, a monitor publish on the
loop and never wait); a broker acknowledges. :class:`Outbox` is the queue
between the two, one per app and key.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import os
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import TYPE_CHECKING, Any, ClassVar

from ..datagateway.config import EnvSetting, MissingSettingError, format_settings, read_setting
from ..keep_up import KeepUp, KeepUpMonitor
from ..log import get_logger
from ..messages.commands import Command
from ..messages.control import PipelineClosed
from ..messages.data_model import stamp_sent_at
from ..messages.errors import ErrorEvent
from ..subscription import Overflow, Subscription
from ..util.tasks import cancel_and_wait

if TYPE_CHECKING:
    from ..messages.data_model import DataModel

__all__ = [
    "TRANSPORT_VARIABLE",
    "InMemoryTransport",
    "Outbox",
    "Transport",
    "TransportSubscription",
    "parse_transport_spec",
    "transport_from_env",
]

#: The variable naming the deployment's transport; unset is the in-memory one.
TRANSPORT_VARIABLE = "PSWAMP_TRANSPORT"

logger = get_logger("pswamp_core.transport")


class TransportSubscription(Subscription):
    """One subscriber's queue of ``(key, message)`` pairs over one or more topics.

    A :class:`~pswamp_core.subscription.Subscription` -- the same overflow
    policies, iteration and context-manager protocol -- with the transport as
    its owner, over the topics of ``models`` in one app, plus the key filter:
    ``key=None`` receives every key, which is a worker's view; a pipeline names
    its own.
    """

    def __init__(
        self,
        transport: Transport,
        topics: dict[str, type[DataModel]],
        key: str | None,
        overflow: Overflow,
        maxsize: int,
    ) -> None:
        super().__init__(transport, tuple(topics.values()), overflow, maxsize)
        self._transport = transport
        #: Topic name → the class it carries.
        self.topics = topics
        self.key = key

    def wants(self, topic: str, key: str) -> bool:
        return topic in self.topics and (self.key is None or key == self.key)

    async def ready(self, timeout: float | None = None) -> None:
        """Wait until this process is consuming every topic here.

        A subscriber that publishes right after subscribing and expects to hear
        the answer needs this; one that only tails does not.
        """
        events = [self._transport._ready[t] for t in self.topics if t in self._transport._ready]
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in events)), timeout)


class Transport(ABC):
    """The keyed publish/subscribe contract, plus the shared fan-out.

    A subclass implements :meth:`publish`, and :meth:`_watch` if messages reach
    it from elsewhere (a broker): start receiving a topic, and call
    :meth:`_deliver` with each message. It inherits ``subscribe`` and the
    delivery to subscriber queues. ``open`` and ``close`` bracket the broker
    connection; ``open`` must be idempotent, since everything in a process
    shares one transport.

    Class attributes a subclass sets: ``env_settings``, the ``{NAME}_{SETTING}``
    block :meth:`from_env` reads, as for a provider.
    """

    env_settings: ClassVar[tuple[EnvSetting, ...]] = ()
    #: True when both ends of every topic live in this process: the module
    #: hosts must then run here too (the server starts them).
    in_process: ClassVar[bool] = False

    def __init__(self, name: str = "transport") -> None:
        self.name = name
        self._subscriptions: list[TransportSubscription] = []
        #: Set while this process is actually consuming a topic.
        self._ready: dict[str, asyncio.Event] = {}

    # --- configuration ----------------------------------------------------------------

    @classmethod
    def show_config(cls, name: str = "<name>") -> None:
        """Print the environment variables that configure this transport."""
        print(format_settings(cls.__name__, name, cls.env_settings))

    @classmethod
    def from_env(cls, name: str, **overrides: Any):
        """Build a transport from its ``{NAME}_{SETTING}`` environment block."""
        settings: dict[str, Any] = {}
        for setting in cls.env_settings:
            keyword = setting.setting.lower()
            if keyword in overrides:
                continue
            value = read_setting(name, setting)
            if value is not None:
                settings[keyword] = value
        return cls(name=name, **settings, **overrides)

    # --- topics -----------------------------------------------------------------------

    def topic(self, model: type[DataModel], app: str) -> str:
        """The topic carrying ``model`` for ``app``: ``<app>.<model.topic>``."""
        return f"{app}.{model.topic}"

    # --- lifecycle --------------------------------------------------------------------

    async def open(self) -> None:
        """Connect to the broker. Idempotent."""
        return

    async def close(self) -> None:
        """Disconnect. Subscriptions still open end."""
        self._ready.clear()
        for subscription in list(self._subscriptions):
            subscription.close()

    # --- the contract -------------------------------------------------------------------

    @abstractmethod
    async def publish(self, message: DataModel, *, app: str, key: str) -> None:
        """Send ``message`` on its class's topic in ``app``, under ``key``.

        Raises on failure; the caller decides whether to count and carry on (a
        live stream) or to stop.
        """

    def subscribe(
        self,
        *models: type[DataModel],
        app: str,
        key: str | None = None,
        overflow: Overflow = Overflow.DROP_OLDEST,
        maxsize: int = 256,
    ) -> TransportSubscription:
        """A queue of ``(key, message)`` over the topics of ``models`` in ``app``,
        for one key or every key. The first subscriber to a topic starts this
        process receiving it, for as long as the transport is open.
        """
        if not models:
            raise ValueError("subscribe needs at least one model class")
        topics = {self.topic(model, app): model for model in models}
        subscription = TransportSubscription(self, topics, key, overflow, maxsize)
        self._subscriptions.append(subscription)
        for topic, model in topics.items():
            self._watch(topic, model)
        return subscription

    # --- the fan-out ----------------------------------------------------------------

    def _watch(self, topic: str, model: type[DataModel]) -> None:
        """Receive ``topic`` (carrying ``model``) in this process from now on,
        setting ``_ready[topic]`` once it is. Nothing to do when messages only
        ever arrive through :meth:`publish`."""
        return

    def _deliver(self, topic: str, key: str, message: DataModel) -> None:
        for subscription in list(self._subscriptions):
            if subscription.wants(topic, key):
                subscription.offer((key, message))  # type: ignore[arg-type]

    def _detach(self, subscription: TransportSubscription) -> None:
        """Called by a closing subscription."""
        if subscription in self._subscriptions:
            self._subscriptions.remove(subscription)


class InMemoryTransport(Transport):
    """A broker with no port: delivery is a method call on the same loop.

    One instance handed to both sides is the whole broker between them -- what
    every hermetic test uses, and the single-process mode of a deployment.
    Delivery behaves as a broker's does: the message is serialised to JSON and
    parsed back (once per publish, shared by the subscribers, as one consumer's
    fan-out is), it reaches only the exact topic of its class, and it is stamped
    with its send time. Constructible with no settings, so
    ``mem:pswamp_core.transport:InMemoryTransport`` is a legal, portless
    configuration.
    """

    in_process = True

    def __init__(self, name: str = "memory") -> None:
        super().__init__(name)
        self.published = 0

    async def publish(self, message: DataModel, *, app: str, key: str) -> None:
        self.published += 1
        topic = self.topic(type(message), app)
        wanted = [s for s in self._subscriptions if s.wants(topic, key)]
        if not wanted:
            return
        received = type(message).model_validate_json(message.model_dump_json())
        stamp_sent_at(received, time.time())
        for subscription in wanted:
            subscription.offer((key, received))  # type: ignore[arg-type]


# --- the outbox: synchronous publishers, an acknowledging broker ---------------------

#: Never dropped by an outbox that is full: what changes something, or reports it.
_CONTROL = (Command, ErrorEvent, PipelineClosed)

#: Log the first few publish failures, then one in every this-many.
_LOG_FIRST = 3
_LOG_EVERY = 50


class Outbox:
    """A synchronous sink onto the transport, for one app and one key.

    The player, a module and a monitor publish synchronously, on the loop, and
    never wait; ``Transport.publish`` is a coroutine a broker acknowledges.
    The outbox is the one ordered queue between them, drained by its own task.
    When it is full the oldest *data* message is dropped -- counted in
    ``dropped`` and reported under ``keep_up`` -- and never a command, an
    error or a ``PipelineClosed``: a live stream would rather lose a stale
    frame than fall further behind, and nothing else may be lost.

    Args:
        transport: Where the messages go.
        app, key: The topic namespace and the record key of everything here.
        maxsize: How much data may wait before the oldest is dropped.
        keep_up: When dropping is reported as an ``ErrorEvent``; ``None`` never.
        source, label: Who the report comes from and names.
    """

    def __init__(
        self,
        transport: Transport,
        *,
        app: str,
        key: str,
        maxsize: int = 64,
        keep_up: KeepUp | None = None,
        source: str = "outbox",
        label: str | None = None,
    ) -> None:
        self.transport = transport
        self.app = app
        self.key = key
        self.maxsize = maxsize
        self._queue: deque[DataModel] = deque()
        self._data = 0
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        #: Messages the transport took, refused, and data dropped for falling behind.
        self.published = 0
        self.failed = 0
        self.dropped = 0
        self.monitor = KeepUpMonitor(
            source, "cannot publish as fast as the pipeline produces", keep_up, label=label
        )

    def publish(self, message: DataModel) -> None:
        """Queue ``message``; never blocks. Loop only."""
        control = isinstance(message, _CONTROL)
        if not control and self._data >= self.maxsize:
            self._drop_oldest_data()
        self._queue.append(message)
        if not control:
            self._data += 1
        self._wake.set()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name=f"{self.app}@{self.key}.outbox")

    async def close(self, timeout: float = 2.0) -> None:
        """Send what is still queued (a ``PipelineClosed``, say), then stop."""
        task, self._task = self._task, None
        if task is not None:
            await cancel_and_wait(task)
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(timeout):
                while self._queue:
                    await self._send(self._pop())

    def _drop_oldest_data(self) -> None:
        for i, queued in enumerate(self._queue):
            if not isinstance(queued, _CONTROL):
                del self._queue[i]
                self._data -= 1
                self.dropped += 1
                self.monitor.note(self, 1)
                return

    def _pop(self) -> DataModel:
        message = self._queue.popleft()
        if not isinstance(message, _CONTROL):
            self._data -= 1
        return message

    async def _run(self) -> None:
        while True:
            while not self._queue:
                self._wake.clear()
                await self._wake.wait()
            await self._send(self._pop())

    async def _send(self, message: DataModel) -> None:
        try:
            await self.transport.publish(message, app=self.app, key=self.key)
        except Exception as exc:
            self.failed += 1
            if self.failed <= _LOG_FIRST or self.failed % _LOG_EVERY == 0:
                logger.warning(
                    "%s@%s: could not publish %s (%d failed so far): %s",
                    self.app, self.key, type(message).__name__, self.failed, exc,
                )
            return
        self.published += 1


# --- naming a transport from the environment --------------------------------------------


def parse_transport_spec(variable: str, spec: str) -> tuple[str, str, str]:
    """Split a ``name:module.path:ClassName`` value into its three parts."""
    parts = spec.strip().split(":")
    if len(parts) != 3 or not all(parts):
        raise MissingSettingError(f"{variable} value {spec!r} is not name:module.path:ClassName")
    return parts[0], parts[1], parts[2]


def load_transport_class(variable: str, module_path: str, class_name: str) -> type[Transport]:
    """Import ``class_name`` from ``module_path`` and check it is a ``Transport``."""
    try:
        module = importlib.import_module(module_path)
    except ImportError as error:
        raise MissingSettingError(
            f"{variable}: cannot import module {module_path!r}: {error}"
        ) from error
    cls = getattr(module, class_name, None)
    if cls is None or not isinstance(cls, type) or not issubclass(cls, Transport):
        raise MissingSettingError(f"{variable}: {module_path}.{class_name} is not a Transport")
    return cls


def transport_from_env(variable: str = TRANSPORT_VARIABLE) -> Transport:
    """The transport ``variable`` names, built from its own environment block;
    an :class:`InMemoryTransport` when it is unset.

    This is the whole choice between "the modules run in this process" and
    "the modules run in a worker": the server and every worker read the same
    variable, so the two sides cannot be configured apart::

        PSWAMP_TRANSPORT=kafka:pswamp_core.transport.kafka:KafkaTransport
        KAFKA_BOOTSTRAP_SERVERS=kafka:9092

    Raises:
        MissingSettingError: A malformed spec, an unimportable class, or a
            transport whose required settings are missing.
    """
    spec = os.environ.get(variable, "").strip()
    if not spec:
        return InMemoryTransport()
    name, module_path, class_name = parse_transport_spec(variable, spec)
    return load_transport_class(variable, module_path, class_name).from_env(name)
