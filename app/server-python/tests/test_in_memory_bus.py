# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The in-memory client as the bus: fan-out, retention, overflow policy.

What the draft's single-queue client did not do and STEP2 §A2 asked for: N
consumers of one topic in one process, each receiving every payload.
"""

from __future__ import annotations

import asyncio

from conftest import Measurement, at, measurement

from pswamp.data import BUS_CAPABILITIES, DataGateway, InMemoryClient, utcnow


async def _tail(gateway: DataGateway, count: int) -> list[str]:
    """Take ``count`` payloads off an open-ended stream, then close it."""
    stream = gateway.consume(Measurement, start=utcnow())
    got: list[str] = []
    async for payload in stream:
        got.append(payload.mRID)
        if len(got) == count:
            break
    await stream.aclose()
    return got


async def _settle(bus: InMemoryClient, subscribers: int) -> None:
    for _ in range(100):
        if bus.subscriber_count == subscribers:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"expected {subscribers} subscribers, have {bus.subscriber_count}")


async def test_every_subscriber_receives_every_payload():
    bus = InMemoryClient("bus", [Measurement], capabilities=BUS_CAPABILITIES)
    gateway = DataGateway([bus])

    first = asyncio.create_task(_tail(gateway, 2))
    second = asyncio.create_task(_tail(gateway, 2))
    await _settle(bus, 2)

    await gateway.produce(Measurement(mRID="a"))
    await gateway.produce(Measurement(mRID="b"))

    assert await first == ["a", "b"]
    assert await second == ["a", "b"]
    await _settle(bus, 0)


async def test_produce_stores_and_a_late_consumer_replays_history():
    bus = InMemoryClient("bus", [Measurement], capabilities=BUS_CAPABILITIES)
    gateway = DataGateway([bus])

    await gateway.produce(measurement(0, at(0)))
    await gateway.produce(measurement(1, at(1)))

    got = [payload.mRID async for payload in gateway.consume(Measurement, end=at(5))]

    assert got == ["m0", "m1"]


async def test_publish_without_subscribers_is_lost_and_not_stored():
    bus = InMemoryClient("bus", [Measurement], capabilities=BUS_CAPABILITIES)

    bus.publish(measurement(0, at(0)))

    assert bus.records == []
    # An empty live client still covers "now" -- that is what lets the planner
    # route a subscriber to it before anything has been produced.
    coverage = await bus.coverage(Measurement)
    assert coverage is not None and coverage.live


async def test_retention_keeps_the_newest_records():
    bus = InMemoryClient("bus", [Measurement], capabilities=BUS_CAPABILITIES, max_records=2)

    for index in range(5):
        await bus.produce(measurement(index, at(index)))

    assert [record.mRID for record in bus.records] == ["m3", "m4"]


async def test_overflow_drops_oldest_only_for_declared_models():
    class Sampleish(Measurement):
        pass

    bus = InMemoryClient(
        "bus",
        [Measurement],
        capabilities=BUS_CAPABILITIES,
        queue_size=1,
        drop_oldest=[Sampleish],
    )
    gateway = DataGateway([bus])

    slow = asyncio.create_task(_tail(gateway, 1))
    await _settle(bus, 1)

    # Samples: the newer one replaces the older in the full queue.
    bus.publish(Sampleish(mRID="s1", timestamp=utcnow()))
    bus.publish(Sampleish(mRID="s2", timestamp=utcnow()))
    assert await slow == ["s2"]

    slow = asyncio.create_task(_tail(gateway, 1))
    await _settle(bus, 1)

    # Results: the queue keeps the first, the second is dropped with a warning.
    bus.publish(Measurement(mRID="r1", timestamp=utcnow()))
    bus.publish(Measurement(mRID="r2", timestamp=utcnow()))
    assert await slow == ["r1"]
