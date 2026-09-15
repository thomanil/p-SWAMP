# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""Behaviour of the gateway's time-range routing across data clients.

Lifted from the test_pswamp draft (``tests/test_data_gateway.py``); the cases
under "added in p-SWAMP" (coverage union, produce failures surfaced, live
fan-out, and the capability rules) are new.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
from support import HISTORY, LIVE, Measurement, at, collect, measurement

from pswamp_core.datagateway import (
    Capability,
    Coverage,
    DataGapError,
    DataGateway,
    ProduceError,
    TimeRange,
)
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.util.time import utcnow


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

    bus.publish(measurement(3, now + timedelta(seconds=1)))
    bus.publish(measurement(4, now + timedelta(seconds=10)))

    result = await asyncio.wait_for(collect(stream), timeout=5)

    assert result == ["m0", "m1", "m2", "m3"]
    assert [segment.client.name for segment in stream.segments] == [
        "timescale",
        "timescale",
        "timescale",
        "kafka",
    ]
    assert stream.segments[-1].live is True
    assert bus.consume_calls == 1


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


# --- added in p-SWAMP -------------------------------------------------------


async def test_coverage_is_the_union_over_clients():
    cold = InMemoryClient(
        "cold", [Measurement], [measurement(i, at(i)) for i in range(3)], capabilities=HISTORY
    )
    warm = InMemoryClient(
        "warm", [Measurement], [measurement(i, at(i)) for i in range(2, 6)], capabilities=LIVE
    )
    gateway = DataGateway([cold, warm])

    coverage = await gateway.coverage(Measurement)

    assert coverage is not None
    assert coverage.range.start == at(0)
    assert coverage.range.contains(at(5))
    assert coverage.live is True
    assert await gateway.coverage(Measurement, mRID="nope") is None


async def test_produce_failures_are_surfaced():
    class Broken(InMemoryClient):
        async def produce(self, data):
            raise OSError("disk full")

    good = InMemoryClient("good", [Measurement], capabilities=HISTORY)
    bad = Broken("bad", [Measurement], capabilities=HISTORY)
    gateway = DataGateway([good, bad])

    with pytest.raises(ProduceError) as excinfo:
        await gateway.produce(Measurement(value=1.0, mRID="m0"))

    assert set(excinfo.value.failures) == {"bad"}
    assert len(good.records) == 1  # the healthy client still wrote


async def test_in_memory_live_delivery_fans_out_to_every_consumer():
    client = InMemoryClient("bus", [Measurement], capabilities=LIVE)
    open_range = TimeRange(utcnow() - timedelta(seconds=1), None)

    first = client.consume(Measurement, open_range)
    second = client.consume(Measurement, open_range)
    # Prime both generators so they are tailing before anything is published.
    first_task = asyncio.create_task(first.__anext__())
    second_task = asyncio.create_task(second.__anext__())
    await asyncio.sleep(0)

    client.publish(measurement(1, utcnow()))

    got_first, got_second = await asyncio.wait_for(
        asyncio.gather(first_task, second_task), timeout=2
    )
    assert got_first.mRID == got_second.mRID == "m1"
    await first.aclose()
    await second.aclose()


# --- declared capabilities are honoured by routing ----------------------------


def _live_only(name: str, since: timedelta = timedelta(seconds=60), **kwargs) -> TrackingClient:
    """A client that can tail but holds no history: its coverage is now-relative
    and live, like a broker subscription with no retention."""
    return TrackingClient(
        name,
        [Measurement],
        [],
        capabilities=Capability.LIVE_CONSUME,
        coverage_fn=lambda: Coverage(TimeRange(utcnow() - since, None), live=True),
        **kwargs,
    )


async def test_produce_only_client_contributes_no_coverage_and_no_segment():
    sink = TrackingClient(
        "sink", [Measurement], [measurement(i, at(i)) for i in range(3)],
        capabilities=Capability.PRODUCE,
    )
    gateway = DataGateway([sink])

    assert await gateway.coverage(Measurement) is None
    assert await collect(gateway.consume(Measurement)) == []
    assert sink.consume_calls == 0


async def test_history_segments_never_go_to_a_live_only_client():
    history = InMemoryClient(
        "history", [Measurement], [measurement(i, at(i)) for i in range(3)], capabilities=HISTORY
    )
    live = _live_only("live", priority=10)
    gateway = DataGateway([history, live])

    stream = gateway.consume(Measurement, at(0), at(3))
    result = await asyncio.wait_for(collect(stream), timeout=2)

    assert result == ["m0", "m1", "m2"]
    assert live.consume_calls == 0
    assert all(not segment.live for segment in stream.segments)


async def test_live_handoff_requires_live_consume():
    """A history-only client whose coverage *claims* live is never tailed."""
    now = utcnow()
    liar = TrackingClient(
        "liar", [Measurement], [measurement(0, now - timedelta(seconds=1))],
        capabilities=HISTORY,
        coverage_fn=lambda: Coverage(TimeRange(now - timedelta(seconds=30), None), live=True),
    )
    gateway = DataGateway([liar])

    stream = gateway.consume(Measurement, now - timedelta(seconds=2), now + timedelta(seconds=0.2))
    result = await asyncio.wait_for(collect(stream), timeout=2)

    assert result == ["m0"]
    assert all(not segment.live for segment in stream.segments)


async def test_live_only_client_is_offered_only_from_the_handoff_margin():
    """Asking for the last 30 s of a client that can only tail skips straight to
    the hand-off margin: it is never handed a stretch of the past."""
    now = utcnow()
    margin = timedelta(seconds=1)
    live = _live_only("live")
    gateway = DataGateway([live], live_handoff_margin=margin)

    stream = gateway.consume(Measurement, now - timedelta(seconds=30), now + timedelta(seconds=0.2))
    live.publish(measurement(7, now + timedelta(seconds=0.05)))
    result = await asyncio.wait_for(collect(stream), timeout=2)

    assert result == ["m7"]
    assert len(stream.segments) == 1
    assert stream.segments[0].live is True
    assert stream.segments[0].range.start >= now - margin - timedelta(seconds=0.5)
    assert live.consume_calls == 1


async def test_coverage_filters_by_capability():
    history = InMemoryClient(
        "history", [Measurement], [measurement(i, at(i)) for i in range(3)], capabilities=HISTORY
    )
    live = _live_only("live")
    gateway = DataGateway([history, live])

    seekable = await gateway.coverage(Measurement, capability=Capability.HISTORY_CONSUME)
    tailable = await gateway.coverage(Measurement, capability=Capability.LIVE_CONSUME)
    everything = await gateway.coverage(Measurement)

    assert seekable is not None and seekable.live is False
    assert seekable.range.start == at(0) and seekable.range.end is not None
    assert tailable is not None and tailable.live is True and tailable.range.end is None
    assert everything is not None and everything.live is True and everything.range.start == at(0)
    assert gateway.supports(Measurement, Capability.LIVE_CONSUME)
    assert gateway.supports(Measurement, Capability.PRODUCE)  # HISTORY includes PRODUCE
