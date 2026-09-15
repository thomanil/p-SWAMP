# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""
List-backed data client.

Lifted from the test_pswamp draft (``core/datagateway/clients/in_memory.py``).
Useful on its own for tests and fixtures, and intended as the reference shape
for real backends: a Kafka client sets ``live=True`` on a narrow now-relative
coverage, a TimescaleDB client reports a wide coverage ending slightly in the
past, and a recording reports a fixed window with ``live=False``.

Adapted from the draft: live delivery fans out to **every** consumer currently
tailing (the draft had one ``asyncio.Queue`` that N consumers would have split
between them, STEP 2 A2). A payload published while nobody tails is held for
the next consumer that does, which is the draft's behaviour and what its tests
rely on.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from ...util.time import UTC, utcnow
from ..data_client_model import (
    Capability,
    DataClient,
    ModelSelector,
    MRIDFilter,
    normalise_models,
    normalise_mrid_filter,
)
from ..time_range import Coverage, TimeRange

if TYPE_CHECKING:
    from ...messages.data_model import DataModel

__all__ = ["InMemoryClient"]

#: Padding added past the newest record so a half-open coverage includes it.
_END_PADDING = timedelta(microseconds=1)

#: Sort position for payloads that have no timestamp yet.
_UNSET_TIMESTAMP = datetime.min.replace(tzinfo=UTC)


class InMemoryClient(DataClient):
    """
    Data client backed by a Python list, with optional live fan-out.

    Args:
        name: Unique client name.
        supported_models: Model classes this client serves.
        records: Initial stored payloads. Sorted by timestamp on ingest.
        priority: Preference against other clients covering the same instant.
        capabilities: Operations to expose. Add
            :attr:`~pswamp_core.datagateway.data_client_model.Capability.LIVE_CONSUME`
            to make the client tail published payloads.
        coverage_fn: Overrides the coverage derived from the stored records,
            letting tests model now-relative or moving windows.
    """

    def __init__(
        self,
        name: str,
        supported_models: ModelSelector,
        records: Sequence[DataModel] = (),
        *,
        priority: int = 0,
        capabilities: Capability = Capability.HISTORY_CONSUME | Capability.PRODUCE,
        coverage_fn: Callable[[], Coverage | None] | None = None,
    ):
        self.name = name
        self.supported_models = normalise_models(supported_models)
        self.priority = priority
        self.capabilities = capabilities

        self.records: list[DataModel] = sorted(records, key=_timestamp_key)
        # One queue per consumer currently tailing, plus the backlog for the
        # next one when nobody is.
        self._tails: list[asyncio.Queue[DataModel]] = []
        self._backlog: list[DataModel] = []

        self._coverage_fn = coverage_fn

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

        if not matching:
            return None

        return Coverage(
            range=TimeRange(
                matching[0].timestamp,
                matching[-1].timestamp + _END_PADDING,
            ),
            live=Capability.LIVE_CONSUME in self.capabilities,
        )

    async def consume(
        self,
        model: type[DataModel],
        time_range: TimeRange,
        mRID: MRIDFilter = None,
    ) -> AsyncIterator[DataModel]:
        """Replay stored records in order, then tail live payloads if allowed."""
        for record in self._matching(model, mRID):
            if not time_range.contains(record.timestamp):
                if time_range.end is not None and record.timestamp >= time_range.end:
                    break
                continue

            yield record

        if not self._should_tail(time_range):
            return

        queue: asyncio.Queue[DataModel] = asyncio.Queue()
        for held in self._backlog:
            queue.put_nowait(held)
        self._backlog.clear()
        self._tails.append(queue)

        try:
            while True:
                record = await self._next_live(queue, time_range)

                if record is None:
                    return

                if not isinstance(record, model):
                    continue

                if time_range.end is not None and record.timestamp >= time_range.end:
                    return

                if not time_range.contains(record.timestamp):
                    continue

                yield record
        finally:
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
        """Append to the stored records, keeping them ordered by timestamp."""
        self.records.append(data)
        self.records.sort(key=_timestamp_key)

    def publish(self, data: DataModel) -> None:
        """Push a payload to every consumer currently tailing this client, or
        hold it for the next one when nobody is."""
        if not self._tails:
            self._backlog.append(data)
            return
        for queue in self._tails:
            queue.put_nowait(data)

    def _matching(
        self,
        model: type[DataModel],
        mRID: MRIDFilter,
    ) -> list[DataModel]:
        """Stored records of ``model`` matching the identifier filter."""
        wanted = normalise_mrid_filter(mRID)

        return [
            record
            for record in self.records
            if isinstance(record, model)
            and record.timestamp is not None
            and (wanted is None or record.mRID in wanted)
        ]

    def _should_tail(self, time_range: TimeRange) -> bool:
        """Whether the request asks this client to follow live data."""
        if Capability.LIVE_CONSUME not in self.capabilities:
            return False

        return time_range.end is None or time_range.end > utcnow()


def _timestamp_key(record: DataModel) -> datetime:
    """Sort key placing not-yet-stamped records first."""
    return record.timestamp or _UNSET_TIMESTAMP


#: How long the broker stand-in pretends to retain what was produced.
_DEFAULT_BROKER_RETENTION = timedelta(minutes=20)


class InMemoryBroker(InMemoryClient):
    """A broker with no port, for tests and portless configurations.

    What a message broker looks like through the provider contract, on top of
    the list-backed client: ``produce`` keeps the payload as history *and*
    hands it to every consumer currently tailing (a broker does not hold
    messages for "the next consumer", so there is no backlog); an open-ended
    ``consume`` is a pure tail from now, as a seek-to-end on a topic is; a
    bounded one replays what is retained; ``coverage`` is a rolling window
    ending in the open, so the planner treats it as live. Records older than
    ``retention`` are dropped on the next ``produce``.

    One instance shared by two gateways stands in for a broker between two
    sides of a :class:`~pswamp_core.bridge.TopicBridge`. ``drop_tails`` ends
    every open tail, which is what a broker restart looks like to a consumer.
    Constructible with no settings, so ``from_env("bus")`` needs no variables.
    """

    def __init__(
        self,
        name: str = "broker",
        supported_models: ModelSelector | None = None,
        *,
        retention: timedelta = _DEFAULT_BROKER_RETENTION,
        priority: int = 0,
        capabilities: Capability = (
            Capability.LIVE_CONSUME | Capability.HISTORY_CONSUME | Capability.PRODUCE
        ),
    ) -> None:
        from ...messages.data_model import DataModel  # a bus carries any message

        super().__init__(
            name,
            DataModel if supported_models is None else supported_models,
            priority=priority,
            capabilities=capabilities,
        )
        self.retention = retention

    async def coverage(
        self,
        model: type[DataModel],
        mRID: MRIDFilter = None,
    ) -> Coverage | None:
        """A rolling retention window; open-ended when this client tails."""
        if not self.supports(model):
            return None
        now = utcnow()
        live = Capability.LIVE_CONSUME in self.capabilities
        return Coverage(range=TimeRange(now - self.retention, None if live else now), live=live)

    async def consume(
        self,
        model: type[DataModel],
        time_range: TimeRange,
        mRID: MRIDFilter = None,
    ) -> AsyncIterator[DataModel]:
        """A bounded range replays what is retained; an open one tails from now."""
        if time_range.end is not None:
            async for record in super().consume(model, time_range, mRID):
                yield record
            return

        wanted = normalise_mrid_filter(mRID)
        queue: asyncio.Queue[DataModel | None] = asyncio.Queue()
        self._tails.append(queue)
        try:
            while True:
                record = await queue.get()
                if record is None:
                    return
                if not isinstance(record, model) or not time_range.contains(record.timestamp):
                    continue
                if wanted is not None and record.mRID not in wanted:
                    continue
                yield record
        finally:
            self._tails.remove(queue)

    async def produce(self, data: DataModel) -> None:
        """Retain the payload and deliver it to every open tail."""
        await super().produce(data)
        cutoff = utcnow() - self.retention
        self.records = [r for r in self.records if r.timestamp is not None and r.timestamp >= cutoff]
        self.publish(data)

    def publish(self, data: DataModel) -> None:
        """Deliver to every open tail; nobody tailing means nobody hears it."""
        for queue in self._tails:
            queue.put_nowait(data)

    def drop_tails(self) -> None:
        """End every open tail, as a broker going away does to its consumers."""
        for queue in list(self._tails):
            queue.put_nowait(None)
