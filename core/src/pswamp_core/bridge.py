# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A pipeline's bus, bridged over topics on a broker: ``TopicBridge``.

STEP 3 §4.4 designed a broker as a bus -- publish by ``gateway.produce``,
subscribe by **one** ``gateway.consume(Model, now, None)`` per model per
process, fanned out on the local bus -- and STEP 4 §8.1 #3 said the ``Bus``
protocol should not be frozen until one such adapter had been written. This is
that adapter, kept *outside* the protocol: not a ``Bus`` implementation but a
Module-shaped component in the pipeline's module list, so a pipeline's own bus
stays the in-process one, and what crosses the process boundary is exactly the
classes the bridge is told to carry::

    web process                                  worker process
    bus ─▶ TopicBridge(outbound=[PmuFrame]) ─ produce ─▶ broker ─ consume ─▶ TopicBridge(inbound=[PmuFrame]) ─▶ bus
    bus ◀─ TopicBridge(inbound=[Result])  ◀─ consume ─ broker ◀─ produce ─ TopicBridge(outbound=[Result]) ◀─ bus

The broker is a ``DataClient`` (``KafkaClient``, or ``InMemoryBroker`` in a
test) behind a ``DataGateway`` of the bridge's **own** -- never the pipeline's
data gateway, whose planner would otherwise offer the broker as one more live
source of the very frames the player publishes. A class is carried in one
direction per side, so nothing echoes.

Three things the shape decides:

* **Publishing is fire-and-forget.** ``Bus.publish`` is synchronous and
  ``DataGateway.produce`` is a coroutine; the subscription the bridge holds on
  the local bus *is* the outbox between them, with the bus's own overflow
  policy (``DROP_OLDEST``: a live stream drops time rather than growing).
* **Tails reconnect here.** A ``DataStream`` ends when its live segment does
  and never re-plans, so a tail that ends -- broker restart, network -- is
  reopened from *now* with exponential backoff. Catching up a backlog is the
  wrong thing for live data.
* **Priming is re-stamped.** A record the consumer side needs before its
  first message (the stream's ``PmuHeader``) is read from the pipeline's
  gateway in ``setup`` and produced with ``timestamp`` set to now, then again
  every ``prime_interval``: a broker client addresses by payload time, so a
  header stamped with the recording's start would never pass a live tail nor a
  bounded read, and re-priming keeps it inside the broker's retention for a
  consumer that starts late.

It follows that only live-stamped messages cross a bridge. A *replay* over a
broker would need a client addressing by arrival time, which none does.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Sequence
from datetime import timedelta
from typing import ClassVar

from .bus import Bus, Overflow
from .datagateway import Capability, DataGateway
from .log import get_logger
from .messages import AppStatus, DataModel, ResultEnvelope
from .modules import Module
from .util.time import utcnow

__all__ = ["TopicBridge", "newest"]

logger = get_logger("pswamp_core.bridge")

#: A rolling window's start moves between the coverage read and the plan; ask
#: from a little after it, so the planner never sees (and logs) a gap there.
_ROLLING_SLACK = timedelta(seconds=1)

#: Log every produce failure up to this many, then one in every ``_LOG_EVERY``.
_LOG_FIRST = 3
_LOG_EVERY = 50


class TopicBridge(Module):
    """Carry ``outbound`` classes from the local bus to a broker gateway and
    ``inbound`` classes back, for as long as the pipeline runs.

    Args:
        gateway: The broker, behind a gateway of this bridge's own. Opened in
            ``setup``, closed when ``run`` ends.
        outbound: Classes subscribed on the local bus and produced.
        inbound: Classes tailed from the broker and published locally, one
            open-ended consume per class.
        prime: Classes read from the *pipeline's* gateway in ``setup`` and
            produced, re-stamped, at once and every ``prime_interval``.
        prime_interval: Seconds between re-primes; ``None`` primes once.
        overflow, maxsize: The outbox's policy, as for any module.
        reconnect_delay, max_reconnect_delay: The backoff for a tail that ended.
        name: How this bridge identifies itself in logs.
    """

    input_model: ClassVar[type[DataModel]] = DataModel  # unused: run() is overridden
    output_model: ClassVar[type[ResultEnvelope]] = ResultEnvelope

    def __init__(
        self,
        gateway: DataGateway,
        *,
        outbound: Sequence[type[DataModel]] = (),
        inbound: Sequence[type[DataModel]] = (),
        prime: Sequence[type[DataModel]] = (),
        prime_interval: float | None = 30.0,
        overflow: Overflow = Overflow.DROP_OLDEST,
        maxsize: int = 256,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 30.0,
        name: str = "topic-bridge",
    ) -> None:
        self.name = name
        super().__init__()
        overlap = set(outbound).union(prime).intersection(inbound)
        if overlap:
            names = ", ".join(sorted(cls.__name__ for cls in overlap))
            raise ValueError(f"{name}: {names} cannot be both sent and received by one side")
        self._gateway = gateway
        self._outbound = tuple(outbound)
        self._inbound = tuple(inbound)
        self._prime = tuple(prime)
        self._prime_interval = prime_interval
        self.overflow = overflow
        self.maxsize = maxsize
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        self._primed: list[DataModel] = []
        self._tailing: dict[type[DataModel], bool] = {cls: False for cls in self._inbound}
        self.produced = 0
        self.dropped = 0
        self.received = 0
        self.reconnects = 0
        self.parameters = {
            "outbound": [cls.topic for cls in self._outbound],
            "inbound": [cls.topic for cls in self._inbound],
            "prime": [cls.topic for cls in self._prime],
        }

    @property
    def connected(self) -> bool:
        """Whether every inbound tail is currently open."""
        return all(self._tailing.values())

    async def setup(self, gateway: DataGateway, bus: Bus) -> None:
        """Open the broker gateway; read and produce what the other side needs first."""
        await self._gateway.open()
        for cls in self._prime:
            async for record in gateway.consume(cls):
                self._primed.append(record)
        await self._produce_primed()
        logger.info(
            "%s: bridging out %s, in %s (%d primed)",
            self.name, list(self.parameters["outbound"]), list(self.parameters["inbound"]),
            len(self._primed),
        )

    async def process(self, message: DataModel) -> None:
        raise NotImplementedError("a TopicBridge forwards; nothing here processes")

    async def run(self, bus: Bus) -> None:
        tasks: list[asyncio.Task] = []
        if self._outbound:
            tasks.append(asyncio.create_task(self._drain(bus), name=f"{self.name}.drain"))
        for cls in self._inbound:
            tasks.append(asyncio.create_task(self._tail(bus, cls), name=f"{self.name}.tail.{cls.topic}"))
        if self._primed and self._prime_interval:
            tasks.append(asyncio.create_task(self._reprime(), name=f"{self.name}.prime"))
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            with contextlib.suppress(Exception):
                await self._gateway.close()

    # --- outbound -----------------------------------------------------------------

    async def _drain(self, bus: Bus) -> None:
        with bus.subscribe(*self._outbound, overflow=self.overflow, maxsize=self.maxsize) as outbox:
            async for message in outbox:
                await self._produce(message)

    async def _produce(self, message: DataModel) -> None:
        try:
            await self._gateway.produce(message)
        except Exception as exc:
            self.dropped += 1
            if self.dropped <= _LOG_FIRST or self.dropped % _LOG_EVERY == 0:
                logger.warning(
                    "%s: could not produce %s (%d dropped so far): %s",
                    self.name, type(message).__name__, self.dropped, exc,
                )
            return
        self.produced += 1

    async def _produce_primed(self) -> None:
        for record in self._primed:
            await self._produce(record.model_copy(update={"timestamp": utcnow()}))

    async def _reprime(self) -> None:
        assert self._prime_interval is not None
        while True:
            await asyncio.sleep(self._prime_interval)
            await self._produce_primed()

    # --- inbound ------------------------------------------------------------------

    async def _tail(self, bus: Bus, cls: type[DataModel]) -> None:
        delay = self._reconnect_delay
        first = True
        while True:
            delivered = 0
            try:
                self._tailing[cls] = True
                if not first:
                    self.reconnects += 1
                async for message in self._gateway.consume(cls, utcnow(), None):
                    delivered += 1
                    self.received += 1
                    self.status = AppStatus.OK
                    bus.publish(message)
                logger.warning("%s: tail of %s ended", self.name, cls.topic)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("%s: tail of %s failed: %s", self.name, cls.topic, exc)
            finally:
                self._tailing[cls] = False
            first = False
            self.status = AppStatus.UNDEFINED
            await asyncio.sleep(delay)
            delay = self._reconnect_delay if delivered else min(delay * 2, self._max_reconnect_delay)


async def newest(gateway: DataGateway, model: type[DataModel]) -> DataModel | None:
    """The newest ``model`` a gateway's history clients hold, or ``None``.

    A *bounded* read up to now over the history coverage, so a client with a
    rolling window returns instead of tailing. What a consumer side uses to
    find a primed record before its first message.
    """
    coverage = await gateway.coverage(model, capability=Capability.HISTORY_CONSUME)
    if coverage is None or coverage.range.start is None:
        return None
    start = coverage.range.start + (_ROLLING_SLACK if coverage.live else timedelta(0))
    end = utcnow()
    if coverage.range.end is not None and coverage.range.end < end:
        end = coverage.range.end
    if end <= start:
        return None
    latest: DataModel | None = None
    async for record in gateway.consume(model, start, end):
        latest = record
    return latest
