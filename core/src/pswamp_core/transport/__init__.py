# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Keyed publish/subscribe between processes: what carries a module's topics
when the module runs as its own service.

A module is connected to its pipeline by a bus: it consumes one message class
and publishes another (:mod:`pswamp_core.modules`). Running it in another
process keeps that connection and moves the *hop* onto a broker -- one topic per
message class, the pipeline key on every record -- and this module is the
contract for that hop::

    await transport.publish(frame, key="42")                  # topic pmu.frame, record key "42"
    with transport.subscribe(FrameStatsResult, key="42") as results:
        async for key, result in results: ...                 # only key "42"
    with transport.subscribe(PmuFrame) as frames:             # every key: a worker's view
        async for key, frame in frames: ...

**A transport is not a provider.** STEP 3 §4.4 sketched the broker as a bus by
way of ``gateway.produce`` / ``gateway.consume``; that cannot carry the PMU
test streamer, whose replay frames are stamped in January and whose timestamps
go *backwards* at every loop -- a time-addressed ``DataStream`` with a watermark
would drop them. A transport carries whatever the bus said, in the order it
said it, and never looks at a timestamp. A broker can *also* be a provider (a
topic's retention as history, its tail as live); that is a ``DataClient``, a
different class for a different job, and not this one.

**The key is transport metadata, not a message field** (STEP 3 ADR-005: no
per-message namespace). A pipeline keyed per client publishes under that client
id; a pipeline keyed per stream publishes under the stream name; the worker on
the other side runs one module instance per key it sees. Nothing in a
``DataModel`` changes for the move.

**A subscriber hears only what is said after it subscribes.** A transport
keeps nothing and replays nothing: a frame carries its own layout
(``PmuFrame.header``), so a worker that starts late is primed by the first
frame it sees.

One broker consumer per model per process (STEP 3 §4.4): ``subscribe`` fans a
shared feed out to subscriber queues with the bus's own ``Overflow`` policy,
so eight pipelines tailing their results cost one consumer, not eight.

Concrete transports: :class:`InMemoryTransport` here (no port; one instance
shared by two sides *is* the broker, which is what every hermetic test uses),
and :class:`pswamp_core.transport.kafka.KafkaTransport` behind the ``kafka``
extra. A transport is named and configured from the environment exactly as a
provider is -- ``name:module.path:ClassName`` plus a ``{NAME}_{SETTING}`` block
-- through :func:`transport_from_env`.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import os
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, ClassVar

from ..bus import Overflow, Subscription
from ..datagateway.config import EnvSetting, MissingSettingError, format_settings, read_setting
from ..log import get_logger
from ..messages.data_model import stamp_sent_at

if TYPE_CHECKING:
    from ..messages.data_model import DataModel

__all__ = [
    "InMemoryTransport",
    "Transport",
    "TransportSubscription",
    "parse_transport_spec",
    "transport_from_env",
]

logger = get_logger("pswamp_core.transport")

#: Backoff for a feed that died (broker restart, network): first wait, and the cap.
_RECONNECT_DELAY = 1.0
_MAX_RECONNECT_DELAY = 30.0


class TransportSubscription(Subscription):
    """One subscriber's queue of ``(key, message)`` pairs over one model.

    The bus's :class:`~pswamp_core.bus.Subscription` -- the same overflow
    policies, the same iteration and context-manager protocol -- with the
    transport playing the bus's part (it detaches the subscription on close),
    plus the key filter: ``key=None`` receives every key, which is a worker's
    view; a pipeline names its own.
    """

    def __init__(
        self,
        transport: Transport,
        model: type[DataModel],
        key: str | None,
        overflow: Overflow,
        maxsize: int,
    ) -> None:
        super().__init__(transport, (model,), overflow, maxsize)  # type: ignore[arg-type]
        self._transport = transport
        self.model = model
        self.key = key

    def wants(self, key: str, message: DataModel) -> bool:
        return isinstance(message, self.models) and (self.key is None or key == self.key)

    async def ready(self, timeout: float | None = None) -> None:
        """Wait until the broker-side feed behind this subscription is consuming.

        A subscriber that publishes right after subscribing and expects to hear
        the echo needs this; a pipeline tailing results does not.
        """
        event = self._transport._ready.get(self.model)
        if event is not None:
            await asyncio.wait_for(event.wait(), timeout)


class Transport(ABC):
    """The keyed publish/subscribe contract, plus the shared fan-out.

    A subclass implements :meth:`publish` and :meth:`_feed` -- one broker
    consumer's worth of ``(key, message)`` pairs for one model -- and inherits
    ``subscribe``, the per-model feed task, its reconnect loop and the delivery
    to subscriber queues. ``open`` and ``close`` bracket the broker connection;
    ``open`` must be idempotent, since every ``RemoteModule`` in a process calls
    it on the one shared transport.

    Class attributes a subclass sets: ``env_settings``, the ``{NAME}_{SETTING}``
    block :meth:`from_env` reads, as for a provider.
    """

    env_settings: ClassVar[tuple[EnvSetting, ...]] = ()

    def __init__(self, name: str = "transport") -> None:
        self.name = name
        self._subscriptions: list[TransportSubscription] = []
        self._feeds: dict[type[DataModel], asyncio.Task] = {}
        #: Set while the feed for a model is actually consuming.
        self._ready: dict[type[DataModel], asyncio.Event] = {}

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

    # --- lifecycle --------------------------------------------------------------------

    async def open(self) -> None:
        """Connect to the broker. Idempotent."""
        return

    async def close(self) -> None:
        """Stop every feed and disconnect. Subscriptions still open end."""
        feeds = list(self._feeds.values())
        self._feeds.clear()
        self._ready.clear()
        for task in feeds:
            task.cancel()
        for task in feeds:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        for subscription in list(self._subscriptions):
            subscription.close()

    # --- the contract -------------------------------------------------------------------

    @abstractmethod
    async def publish(self, message: DataModel, key: str) -> None:
        """Send ``message`` on its class's topic under ``key``.

        Raises on failure; the caller decides whether to count and carry on (a
        live stream) or to stop.
        """

    def subscribe(
        self,
        model: type[DataModel],
        key: str | None = None,
        *,
        overflow: Overflow = Overflow.DROP_OLDEST,
        maxsize: int = 256,
    ) -> TransportSubscription:
        """A queue of ``(key, message)`` for ``model``, one key or every key.

        The first subscriber to a model starts its feed; the last one to close
        stops it.
        """
        subscription = TransportSubscription(self, model, key, overflow, maxsize)
        self._subscriptions.append(subscription)
        self._ensure_feed(model)
        return subscription

    # --- the shared feed ----------------------------------------------------------------

    @abstractmethod
    def _feed(
        self, model: type[DataModel], ready: asyncio.Event
    ) -> AsyncIterator[tuple[str, DataModel]]:
        """One broker consumer over ``model``'s topic, yielding ``(key, message)``
        until cancelled. Sets ``ready`` once it is actually consuming. Raising
        ends one attempt; the base class reopens it with backoff."""

    def _ensure_feed(self, model: type[DataModel]) -> None:
        if model not in self._feeds:
            self._ready[model] = asyncio.Event()
            self._feeds[model] = asyncio.create_task(
                self._run_feed(model), name=f"{self.name}.feed.{model.topic}"
            )

    async def _run_feed(self, model: type[DataModel]) -> None:
        ready = self._ready[model]
        delay = _RECONNECT_DELAY
        while True:
            delivered = 0
            try:
                async for key, message in self._feed(model, ready):
                    delivered += 1
                    self._deliver(key, message)
                logger.warning("%s: feed of %s ended; reopening", self.name, model.topic)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("%s: feed of %s failed: %s; reopening", self.name, model.topic, exc)
            finally:
                ready.clear()
            await asyncio.sleep(delay)
            delay = _RECONNECT_DELAY if delivered else min(delay * 2, _MAX_RECONNECT_DELAY)

    def _deliver(self, key: str, message: DataModel) -> None:
        for subscription in list(self._subscriptions):
            if subscription.wants(key, message):
                subscription.offer((key, message))  # type: ignore[arg-type]

    def _detach(self, subscription: TransportSubscription) -> None:
        """Called by a closing subscription; stops its feed if it was the last."""
        if subscription in self._subscriptions:
            self._subscriptions.remove(subscription)
        model = subscription.model
        if any(s.model is model for s in self._subscriptions):
            return
        feed = self._feeds.pop(model, None)
        self._ready.pop(model, None)
        if feed is not None:
            feed.cancel()


class InMemoryTransport(Transport):
    """A broker with no port: delivery is a method call on the same loop.

    One instance handed to both sides -- the pipeline's ``RemoteModule`` and the
    worker's ``ModuleHost`` -- is the whole broker between them, which is what
    every hermetic test uses. Constructible with no settings, so
    ``from_env("mem")`` needs no variables and
    ``mem:pswamp_core.transport:InMemoryTransport`` is a legal, portless
    configuration.
    """

    def __init__(self, name: str = "memory") -> None:
        super().__init__(name)
        self.published = 0

    async def publish(self, message: DataModel, key: str) -> None:
        self.published += 1
        stamp_sent_at(message, time.time())
        self._deliver(key, message)

    def _ensure_feed(self, model: type[DataModel]) -> None:
        return  # publish delivers directly; there is no consumer to run

    async def _feed(self, model, ready):  # pragma: no cover - never started
        ready.set()
        return
        yield


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


def transport_from_env(variable: str, default: str | None = None) -> Transport | None:
    """The transport ``variable`` names, built from its own environment block;
    ``None`` when the variable is unset and there is no ``default``.

    This is the whole switch between "the module runs here" and "the module
    runs as its own service": an app reads one variable of its own
    (``PMU_TEST_STREAMER_MODULE_TRANSPORT``), and both the server and the worker
    read the same one, so the two sides cannot be configured apart::

        PMU_TEST_STREAMER_MODULE_TRANSPORT=kafka:pswamp_core.transport.kafka:KafkaTransport
        KAFKA_BOOTSTRAP_SERVERS=kafka:9092

    Raises:
        MissingSettingError: A malformed spec, an unimportable class, or a
            transport whose required settings are missing.
    """
    spec = os.environ.get(variable, "").strip() or default
    if not spec:
        return None
    name, module_path, class_name = parse_transport_spec(variable, spec)
    return load_transport_class(variable, module_path, class_name).from_env(name)
