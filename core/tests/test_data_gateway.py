# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""The gateway: one provider per role, history read by ``consume``, live by
``tail``, and coverage from the history provider."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest
from support import HISTORY, Measurement, at, collect, measurement

from pswamp_core.datagateway import Capability, DataGateway
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.util.time import utcnow


class TrackingClient(InMemoryClient):
    """In-memory client that records how many times it was read."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.consume_calls = 0

    def consume(self, model, time_range, mRID=None) -> AsyncIterator[Measurement]:
        self.consume_calls += 1
        return super().consume(model, time_range, mRID)


def history(name: str = "history", n: int = 3) -> InMemoryClient:
    return InMemoryClient(name, [Measurement], [measurement(i, at(i)) for i in range(n)], capabilities=HISTORY)


async def test_single_client_history():
    gateway = DataGateway([history()])

    assert await collect(gateway.consume(Measurement)) == ["m0", "m1", "m2"]
    assert await collect(gateway.consume(Measurement, at(1))) == ["m1", "m2"]  # open end: to what it holds


async def test_a_second_provider_for_the_same_role_is_refused():
    with pytest.raises(ValueError, match="cold and warm both provide HISTORY_CONSUME for Measurement"):
        DataGateway([history("cold"), history("warm")])
    # one per role is fine
    DataGateway([history(), TrackingClient("live", [Measurement], capabilities=Capability.LIVE_CONSUME)])


async def test_consume_reads_history_and_tail_reads_live():
    live = TrackingClient("live", [Measurement], capabilities=Capability.LIVE_CONSUME)
    gateway = DataGateway([history(), live])

    assert await collect(gateway.consume(Measurement)) == ["m0", "m1", "m2"]
    assert live.consume_calls == 0  # a history read never opens the live provider

    stream = gateway.tail(Measurement)
    assert stream.client is live and stream.request.end is None
    live.publish(measurement(7, utcnow()))
    frame = await asyncio.wait_for(stream.__anext__(), timeout=2)
    await stream.aclose()
    assert frame.mRID == "m7"


async def test_coverage_is_the_history_provider_s_and_a_failure_is_kept():
    class Flaky(InMemoryClient):
        down = True

        async def coverage(self, model, mRID=None):
            if self.down:
                raise ConnectionError("the store is down")
            return await super().coverage(model, mRID)

    store = Flaky("store", [Measurement], [measurement(i, at(i)) for i in range(3)], capabilities=HISTORY)
    live = InMemoryClient("live", [Measurement], capabilities=Capability.LIVE_CONSUME)
    gateway = DataGateway([store, live])

    assert await gateway.coverage(Measurement) is None
    assert gateway.coverage_failures == {"store": "ConnectionError: the store is down"}

    store.down = False
    coverage = await gateway.coverage(Measurement)
    assert coverage is not None and coverage.range.start == at(0) and coverage.range.contains(at(2))
    assert gateway.coverage_failures == {}
