# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.
# Lifted from Louis Pauchet's data-gateway draft (SINTEF, 2026), see STEP3 §13.
# Adapted: multi-subscriber fan-out (one queue per open live stream), a bounded
# store, and a per-model overflow policy — which makes it the in-process bus.

"""List-backed data client — and, with ``LIVE_CONSUME``, the in-process bus.

Contract 3 of STEP3: the bus is not a second abstraction beside the provider
contract, it is one :class:`~..gateway.client.DataClient`. A module's output is
``gateway.produce(result)``; this client, registered with ``PRODUCE``, receives
it and fans it out to every stream currently tailing it. Nobody subscribes to a
module; they ``consume(ResultModel)``, and it makes no difference whether the
result came from a thread in this process or, one day, from a broker client
registered beside this one.

Useful on its own for tests and fixtures, and the reference shape for real
backends: a Kafka client sets ``live=True`` on a narrow now-relative coverage, a
TimescaleDB client reports a wide coverage ending slightly in the past, and a
file reports a fixed window with ``live=False``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Iterable, Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Self

from ..gateway.client import (
    Capability,
    DataClient,
    ModelSelector,
    MRIDFilter,
    normalise_models,
    normalise_mrid_filter,
)
from ..gateway.config import EnvSetting, env_capabilities, env_int
from ..gateway.time_range import Coverage, TimeRange
from ..time import utcnow

if TYPE_CHECKING:
    from ..models.base import DataModel

__all__ = ["InMemoryClient"]

logger = logging.getLogger("pswamp.data.clients.in_memory")

#: Padding added past the newest record so a half-open coverage includes it.
_END_PADDING = timedelta(microseconds=1)

#: How far behind "now" an empty live client's coverage starts, so a cursor the
#: planner stamped a moment earlier still falls inside it.
_LIVE_SLACK = timedelta(seconds=1)

#: Sort position for payloads that have no timestamp yet.
_UNSET_TIMESTAMP = datetime.min.replace(tzinfo=UTC)

DEFAULT_CAPABILITIES = Capability.HISTORY_CONSUME | Capability.PRODUCE
BUS_CAPABILITIES = Capability.LIVE_CONSUME | Capability.HISTORY_CONSUME | Capability.PRODUCE


class InMemoryClient(DataClient):
    """Data client backed by a Python list, fanning live payloads out to subscribers.

    Args:
        name: Unique client name.
        supported_models: Model classes this client serves.
        records: Initial stored payloads. Sorted by timestamp on ingest.
        priority: Preference against other clients covering the same instant.
        capabilities: Operations to expose. Add ``LIVE_CONSUME`` to let open-ended
            ``consume`` calls tail what is produced after they started.
        coverage_fn: Overrides the coverage derived from the stored records,
            letting tests model now-relative or moving windows.
        max_records: Keep only the newest this many records (``None`` = all).
            A bus is not an archive; bound it in a long-running process.
        queue_size: Per-subscriber queue bound (``0`` = unbounded).
        drop_oldest: Model classes whose payloads may push older ones out of a
            full queue — the right policy for samples, where only recent values
            matter. Everything else is dropped *newest* with a warning, so a
            result or an alarm is never silently replaced.
    """

    env_settings = (
        EnvSetting("PRIORITY", "Preference against other clients", default="0"),
        EnvSetting(
            "CAPABILITIES",
            "Comma-separated capability names",
            default="LIVE_CONSUME,HISTORY_CONSUME,PRODUCE",
        ),
        EnvSetting("MAX_RECORDS", "How many records to retain", default="1000"),
    )

    def __init__(
        self,
        name: str,
        supported_models: ModelSelector,
        records: Sequence[DataModel] = (),
        *,
        priority: int = 0,
        capabilities: Capability = DEFAULT_CAPABILITIES,
        coverage_fn: Callable[[], Coverage | None] | None = None,
        max_records: int | None = None,
        queue_size: int = 0,
        drop_oldest: Iterable[type[DataModel]] = (),
    ):
        self.name = name
        self.supported_models = normalise_models(supported_models)
        self.priority = priority
        self.capabilities = capabilities

        self.records: list[DataModel] = sorted(records, key=_timestamp_key)
        self._coverage_fn = coverage_fn
        self._max_records = max_records
        self._queue_size = queue_size
        self._drop_oldest = tuple(drop_oldest)
        self._tails: list[asyncio.Queue[DataModel]] = []

    @classmethod
    def from_env(cls, name: str, models: ModelSelector) -> Self:
        return cls(
            name,
            models,
            priority=env_int(name, "PRIORITY", 0),
            capabilities=env_capabilities(name, "CAPABILITIES", BUS_CAPABILITIES),
            max_records=env_int(name, "MAX_RECORDS", 1000),
        )

    @property
    def subscriber_count(self) -> int:
        """How many open streams are currently tailing this client."""
        return len(self._tails)

    async def coverage(
        self,
        model: type[DataModel],
        mRID: MRIDFilter = None,
    ) -> Coverage | None:
        """Window currently held, derived from the stored records by default."""
        if not self.supports(model):
            return None

        if self._coverage_fn is not None:
            return self._coverage_fn()

        matching = self._matching(model, mRID)
        live = Capability.LIVE_CONSUME in self.capabilities

        if not live:
            if not matching:
                return None
            return Coverage(
                range=TimeRange(matching[0].timestamp, matching[-1].timestamp + _END_PADDING),
            )

        # A live-capable client covers *now* whether or not it has stored
        # anything yet -- that is what lets the planner hand a subscriber to it
        # before the first payload exists. The window starts a little behind
        # now, because the planner computed its cursor before this call.
        now = utcnow()
        start = matching[0].timestamp if matching else now - _LIVE_SLACK
        end = max(now, matching[-1].timestamp if matching else now) + _END_PADDING
        return Coverage(range=TimeRange(start, end), live=True)

    async def consume(
        self,
        model: type[DataModel],
        time_range: TimeRange,
        mRID: MRIDFilter = None,
    ) -> AsyncIterator[DataModel]:
        """Replay stored records in order, then tail what is produced next if allowed.

        The tail queue is registered *before* the stored records are read, and
        nothing awaits between the two, so a payload produced while this
        consumer catches up lands in exactly one of them.
        """
        queue: asyncio.Queue[DataModel] | None = None
        if self._should_tail(time_range):
            queue = asyncio.Queue(maxsize=self._queue_size)
            self._tails.append(queue)

        try:
            for record in self._matching(model, mRID):
                if not time_range.contains(record.timestamp):
                    if time_range.end is not None and record.timestamp >= time_range.end:
                        break
                    continue

                yield record

            if queue is None:
                return

            wanted = normalise_mrid_filter(mRID)

            while True:
                record = await self._next_live(queue, time_range)

                if record is None:
                    return

                if not isinstance(record, model):
                    continue

                if wanted is not None and record.mRID not in wanted:
                    continue

                if time_range.end is not None and record.timestamp >= time_range.end:
                    return

                if not time_range.contains(record.timestamp):
                    continue

                yield record
        finally:
            if queue is not None:
                self._tails.remove(queue)

    async def _next_live(
        self, queue: asyncio.Queue[DataModel], time_range: TimeRange
    ) -> DataModel | None:
        """Await the next published payload, giving up once the window closes."""
        if time_range.end is None:
            return await queue.get()

        remaining = (time_range.end - utcnow()).total_seconds()

        if remaining <= 0:
            return None

        try:
            return await asyncio.wait_for(queue.get(), timeout=remaining)
        except TimeoutError:
            return None

    async def produce(self, data: DataModel) -> None:
        """Store the payload and hand it to every subscriber tailing this client."""
        self._store(data)
        self.publish(data)

    def publish(self, data: DataModel) -> None:
        """Fan a payload out to current subscribers without storing it."""
        for queue in list(self._tails):
            self._offer(queue, data)

    def _offer(self, queue: asyncio.Queue[DataModel], data: DataModel) -> None:
        try:
            queue.put_nowait(data)
        except asyncio.QueueFull:
            if isinstance(data, self._drop_oldest):
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                queue.put_nowait(data)
            else:
                logger.warning(
                    "subscriber on %s is not keeping up; dropped a %s",
                    self.name,
                    type(data).__name__,
                )

    def _store(self, data: DataModel) -> None:
        self.records.append(data)
        self.records.sort(key=_timestamp_key)
        if self._max_records is not None and len(self.records) > self._max_records:
            del self.records[: len(self.records) - self._max_records]

    def _matching(
        self,
        model: type[DataModel],
        mRID: MRIDFilter,
    ) -> list[DataModel]:
        wanted = normalise_mrid_filter(mRID)

        return [
            record
            for record in self.records
            if isinstance(record, model)
            and record.timestamp is not None
            and (wanted is None or record.mRID in wanted)
        ]

    def _should_tail(self, time_range: TimeRange) -> bool:
        if Capability.LIVE_CONSUME not in self.capabilities:
            return False

        return time_range.end is None or time_range.end > utcnow()


def _timestamp_key(record: DataModel) -> datetime:
    return record.timestamp or _UNSET_TIMESTAMP
