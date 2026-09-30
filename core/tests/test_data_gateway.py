# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""Behaviour of the gateway: named sources, one of them active, and the writers.

Lifted from the test_pswamp draft (``tests/test_data_gateway.py``), without its
routing across clients; the cases under "added in p-SWAMP" (the active source
and the switch, produce failures surfaced, live fan-out) are new.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
from support import HISTORY, LIVE, Measurement, at, collect, measurement

from pswamp_core.datagateway import Capability, Coverage, DataGateway, ProduceError, TimeRange
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

async def test_duplicate_client_names_are_rejected():
    with pytest.raises(ValueError, match="Duplicate"):
        DataGateway(
            [
                InMemoryClient("csv", [Measurement], capabilities=HISTORY),
                InMemoryClient("csv", [Measurement], capabilities=HISTORY),
            ]
        )

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


# --- added in p-SWAMP: one active source ------------------------------------


def _history(name: str, first: int, last: int) -> TrackingClient:
    return TrackingClient(
        name, [Measurement], [measurement(i, at(i)) for i in range(first, last)], capabilities=HISTORY
    )


def _live(name: str) -> TrackingClient:
    return TrackingClient(
        name,
        [Measurement],
        [],
        capabilities=LIVE,
        coverage_fn=lambda: Coverage(TimeRange(utcnow() - timedelta(seconds=1), None), live=True),
    )


async def test_the_first_source_listed_is_active_and_only_it_is_read():
    cold, warm = _history("cold", 0, 3), _history("warm", 3, 6)
    gateway = DataGateway([cold, warm])

    assert gateway.sources == ["cold", "warm"]
    assert gateway.source == "cold" and gateway.active is cold and gateway.live is False
    assert await collect(gateway.consume(Measurement)) == ["m0", "m1", "m2"]
    coverage = await gateway.coverage(Measurement)
    assert coverage is not None and coverage.range.start == at(0)
    assert warm.consume_calls == 0


async def test_switch_makes_another_source_active():
    cold, warm = _history("cold", 0, 3), _history("warm", 3, 6)
    gateway = DataGateway([cold, warm])

    assert gateway.switch("warm") is warm
    assert gateway.source == "warm"
    assert await collect(gateway.consume(Measurement)) == ["m3", "m4", "m5"]
    assert cold.consume_calls == 0


async def test_active_can_be_named_and_live_is_the_active_sources_kind():
    gateway = DataGateway([_history("cold", 0, 3), _live("feed")], active="feed")

    assert gateway.source == "feed" and gateway.live is True
    coverage = await gateway.coverage(Measurement)
    assert coverage is not None and coverage.live is True
    gateway.switch("cold")
    assert gateway.live is False


async def test_an_unknown_source_is_refused():
    gateway = DataGateway([_history("cold", 0, 3)])

    with pytest.raises(ValueError, match="no source named 'nope'"):
        gateway.switch("nope")
    with pytest.raises(ValueError, match="no source named 'nope'"):
        DataGateway([_history("cold", 0, 3)], active="nope")


async def test_a_client_declaring_history_and_live_is_refused():
    hybrid = InMemoryClient(
        "both", [Measurement], capabilities=Capability.HISTORY_CONSUME | Capability.LIVE_CONSUME
    )
    with pytest.raises(ValueError, match="one or the other"):
        DataGateway([hybrid])


async def test_a_produce_only_client_is_no_source():
    sink = TrackingClient("sink", [Measurement], capabilities=Capability.PRODUCE)
    gateway = DataGateway([sink, _history("cold", 0, 3)])

    assert gateway.sources == ["cold"]
    await gateway.produce(Measurement(value=1.0, mRID="m9"))
    assert len(sink.records) == 1
    with pytest.raises(ValueError):
        gateway.switch("sink")


async def test_a_gateway_without_a_source_refuses_to_consume():
    gateway = DataGateway(None)

    assert gateway.sources == [] and gateway.source is None
    assert await gateway.coverage(Measurement) is None
    with pytest.raises(RuntimeError, match="no source"):
        gateway.consume(Measurement)


async def test_a_failing_coverage_is_recorded_and_cleared():
    class Flaky(InMemoryClient):
        down = True

        async def coverage(self, model, mRID=None):
            if self.down:
                raise ConnectionError("cannot reach the store")
            return await super().coverage(model, mRID)

    flaky = Flaky("store", [Measurement], [measurement(0, at(0))], capabilities=HISTORY)
    gateway = DataGateway([flaky])

    assert await gateway.coverage(Measurement) is None
    assert gateway.coverage_failure == "ConnectionError: cannot reach the store"
    flaky.down = False
    assert await gateway.coverage(Measurement) is not None
    assert gateway.coverage_failure is None


async def test_the_stream_drops_untimestamped_payloads_and_stops_at_the_end():
    class Sloppy(InMemoryClient):
        async def consume(self, model, time_range, mRID=None) -> AsyncIterator[Measurement]:
            yield Measurement(mRID="none", value=0.0)
            for i in range(5):
                yield measurement(i, at(i))  # ignores the requested end

    gateway = DataGateway([Sloppy("sloppy", [Measurement], capabilities=HISTORY)])

    assert await collect(gateway.consume(Measurement, at(0), at(2))) == ["m0", "m1"]
