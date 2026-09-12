# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.
# Lifted from Louis Pauchet's data-gateway draft (test_pswamp/tests/test_data_gateway.py);
# the live hand-off test publishes after the subscriber exists, since the bus now
# fans out rather than buffering in one queue.

"""Behaviour of the gateway's time-range routing across data clients."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
from conftest import Measurement, at, collect, measurement

from pswamp.data import (
    Capability,
    Coverage,
    DataGapError,
    DataGateway,
    InMemoryClient,
    TimeRange,
    utcnow,
)

HISTORY = Capability.HISTORY_CONSUME | Capability.PRODUCE
LIVE = Capability.LIVE_CONSUME | Capability.HISTORY_CONSUME


class TrackingClient(InMemoryClient):
    """In-memory client that records how many times it was streamed from."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.consume_calls = 0
        self.closed = False

    def consume(self, model, time_range, mRID=None) -> AsyncIterator[Measurement]:
        self.consume_calls += 1
        return self._tracked(model, time_range, mRID)

    async def _tracked(self, model, time_range, mRID) -> AsyncIterator[Measurement]:
        try:
            async for payload in super().consume(model, time_range, mRID):
                yield payload
        finally:
            self.closed = True


class ReplayingClient(InMemoryClient):
    """Client that always replays its whole window, ignoring the requested start."""

    async def consume(self, model, time_range, mRID=None) -> AsyncIterator[Measurement]:
        for record in self.records:
            if time_range.end is not None and record.timestamp >= time_range.end:
                return

            yield record


async def test_single_client_history():
    client = InMemoryClient(
        "csv",
        [Measurement],
        [measurement(index, at(index)) for index in range(3)],
        capabilities=HISTORY,
    )
    gateway = DataGateway([client])

    assert await collect(gateway.consume(Measurement)) == ["m0", "m1", "m2"]


async def test_request_window_is_honoured():
    client = InMemoryClient(
        "csv",
        [Measurement],
        [measurement(index, at(index)) for index in range(5)],
        capabilities=HISTORY,
    )
    gateway = DataGateway([client])

    stream = gateway.consume(Measurement, start=at(1), end=at(3))

    assert await collect(stream) == ["m1", "m2"]


async def test_stitches_across_two_clients():
    cold = InMemoryClient(
        "cold",
        [Measurement],
        [measurement(index, at(index)) for index in range(3)],
        priority=10,
        capabilities=HISTORY,
    )
    warm = InMemoryClient(
        "warm",
        [Measurement],
        [measurement(index, at(index)) for index in range(1, 5)],
        priority=5,
        capabilities=HISTORY,
    )
    gateway = DataGateway([cold, warm])

    stream = gateway.consume(Measurement)
    result = await collect(stream)

    assert result == ["m0", "m1", "m2", "m3", "m4"]
    assert [segment.client.name for segment in stream.segments] == ["cold", "warm"]


async def test_overlapping_records_are_emitted_once():
    cold = InMemoryClient(
        "cold",
        [Measurement],
        [measurement(0, at(0)), measurement(1, at(1))],
        priority=10,
        capabilities=HISTORY,
        coverage_fn=lambda: Coverage(TimeRange(at(0), at(2))),
    )
    bus = ReplayingClient(
        "bus",
        [Measurement],
        [measurement(index, at(index)) for index in range(4)],
        priority=5,
        capabilities=HISTORY,
        coverage_fn=lambda: Coverage(TimeRange(at(0), at(4))),
    )
    gateway = DataGateway([cold, bus])

    assert await collect(gateway.consume(Measurement)) == ["m0", "m1", "m2", "m3"]


async def test_two_models_at_one_instant_both_survive_dedup():
    class Other(Measurement):
        pass

    client = InMemoryClient(
        "csv",
        [Measurement],
        [measurement(0, at(0)), Other(mRID="m0", timestamp=at(0))],
        capabilities=HISTORY,
    )
    gateway = DataGateway([client])

    payloads = [payload async for payload in gateway.consume(Measurement)]

    assert [type(payload).__name__ for payload in payloads] == ["Measurement", "Other"]


async def test_gap_is_skipped_by_default():
    early = InMemoryClient("early", [Measurement], [measurement(0, at(0))], capabilities=HISTORY)
    late = InMemoryClient("late", [Measurement], [measurement(9, at(90))], capabilities=HISTORY)
    gateway = DataGateway([early, late])

    assert await collect(gateway.consume(Measurement)) == ["m0", "m9"]


async def test_gap_raises_under_strict_policy():
    early = InMemoryClient("early", [Measurement], [measurement(0, at(0))], capabilities=HISTORY)
    late = InMemoryClient("late", [Measurement], [measurement(9, at(90))], capabilities=HISTORY)
    gateway = DataGateway([early, late], on_gap="raise")

    with pytest.raises(DataGapError):
        await collect(gateway.consume(Measurement))


async def test_live_source_is_opened_only_once_replay_catches_up():
    now = utcnow()
    history = [
        measurement(index, now - timedelta(seconds=30 - index * 10))
        for index in range(3)
    ]

    # The temporal database keeps ingesting while we replay, so its end advances.
    advancing_ends = iter(
        [now - timedelta(seconds=20), now - timedelta(seconds=10), now]
    )
    last_end = [now]

    def db_coverage() -> Coverage:
        last_end[0] = next(advancing_ends, last_end[0])
        return Coverage(TimeRange(now - timedelta(seconds=30), last_end[0]))

    db = TrackingClient(
        "timescale",
        [Measurement],
        history,
        priority=10,
        capabilities=HISTORY,
        coverage_fn=db_coverage,
    )
    bus = TrackingClient(
        "kafka",
        [Measurement],
        [],
        priority=5,
        capabilities=LIVE,
        coverage_fn=lambda: Coverage(
            TimeRange(utcnow() - timedelta(seconds=20), utcnow()), live=True
        ),
    )

    gateway = DataGateway([db, bus])
    stream = gateway.consume(Measurement, end=now + timedelta(seconds=5))

    collected = asyncio.create_task(collect(stream))
    # The bus fans out to subscribers that exist; wait until the stream has
    # replayed the database and is tailing the bus before publishing.
    for _ in range(100):
        if bus.subscriber_count:
            break
        await asyncio.sleep(0.01)
    assert bus.subscriber_count == 1

    bus.publish(measurement(3, now + timedelta(seconds=1)))
    bus.publish(measurement(4, now + timedelta(seconds=10)))

    result = await asyncio.wait_for(collected, timeout=5)

    assert result == ["m0", "m1", "m2", "m3"]
    assert [segment.client.name for segment in stream.segments] == [
        "timescale",
        "timescale",
        "timescale",
        "kafka",
    ]
    assert stream.segments[-1].live is True
    assert bus.consume_calls == 1
    assert bus.subscriber_count == 0


async def test_closing_a_stream_releases_the_client_iterator():
    client = TrackingClient(
        "csv",
        [Measurement],
        [measurement(index, at(index)) for index in range(5)],
        capabilities=HISTORY,
    )
    gateway = DataGateway([client])

    stream = gateway.consume(Measurement)

    async for _ in stream:
        break

    await stream.aclose()

    assert client.closed is True


async def test_produce_stamps_missing_timestamp():
    client = InMemoryClient("csv", [Measurement], capabilities=HISTORY)
    gateway = DataGateway([client])

    payload = Measurement(value=3.0, mRID="m0")
    await gateway.produce(payload)

    assert payload.timestamp is not None
    assert client.records == [payload]


async def test_disabled_gateway_rejects_calls():
    gateway = DataGateway(None)

    assert gateway.enabled is False

    with pytest.raises(RuntimeError):
        gateway.consume(Measurement)


async def test_duplicate_client_names_are_rejected():
    with pytest.raises(ValueError, match="Duplicate"):
        DataGateway(
            [
                InMemoryClient("csv", [Measurement], capabilities=HISTORY),
                InMemoryClient("csv", [Measurement], capabilities=HISTORY),
            ]
        )
