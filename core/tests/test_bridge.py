# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The topic bridge over the broker stand-in: no port anywhere."""

from __future__ import annotations

import asyncio
import contextlib

import pytest
from support import Measurement, Number, NumberResult, at, measurement, take

from pswamp_core.bridge import TopicBridge, newest
from pswamp_core.bus import InProcessBus, Latest, Overflow
from pswamp_core.datagateway import Capability, DataGateway
from pswamp_core.datagateway.clients import InMemoryBroker, InMemoryClient
from pswamp_core.messages import AppStatus
from pswamp_core.modules import Module
from pswamp_core.util.time import utcnow


class Doubler(Module):
    name = "doubler"
    input_model = Measurement
    output_model = NumberResult

    async def process(self, message: Measurement) -> Number | None:
        return Number(value=message.value * 2)


def now_measurement(index: int) -> Measurement:
    return Measurement(mRID=f"m{index}", value=float(index), timestamp=utcnow())


@contextlib.asynccontextmanager
async def running(bridge: TopicBridge, bus: InProcessBus, pipeline_gateway: DataGateway | None = None):
    """setup + run as a Pipeline would, torn down however the block ends."""
    bus.bind(asyncio.get_running_loop())
    await bridge.setup(pipeline_gateway or DataGateway([InMemoryClient("none", Measurement)]), bus)
    task = asyncio.create_task(bridge.run(bus))
    await asyncio.sleep(0.01)  # let the tails open
    try:
        yield task
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        bus.bind(None)


async def test_outbound_messages_reach_a_tail_on_the_broker():
    broker = InMemoryBroker()
    bus = InProcessBus()
    bridge = TopicBridge(DataGateway([broker]), outbound=[Measurement])
    other_side = DataGateway([broker])
    async with running(bridge, bus):
        stream = other_side.consume(Measurement, utcnow(), None)
        got = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(0.01)
        bus.publish(now_measurement(1))
        received = await asyncio.wait_for(got, 2)
        await stream.aclose()
    assert received.mRID == "m1"
    assert bridge.produced == 1 and bridge.dropped == 0


async def test_inbound_messages_land_on_the_local_bus():
    broker = InMemoryBroker()
    bus = InProcessBus()
    latest = Latest(bus)
    bridge = TopicBridge(DataGateway([broker]), inbound=[Measurement])
    async with running(bridge, bus):
        with bus.subscribe(Measurement, overflow=Overflow.GROW) as sub:
            await DataGateway([broker]).produce(now_measurement(2))
            (got,) = await take(sub, 1)
    assert got.mRID == "m2"
    assert latest.get(Measurement) is got
    assert bridge.received == 1 and bridge.status is AppStatus.OK


async def test_an_inbound_class_is_never_echoed_back():
    broker = InMemoryBroker()
    bus = InProcessBus()
    bridge = TopicBridge(DataGateway([broker]), inbound=[Measurement])
    async with running(bridge, bus):
        await DataGateway([broker]).produce(now_measurement(3))
        await asyncio.sleep(0.02)
    assert len(broker.records) == 1 and bridge.produced == 0
    with pytest.raises(ValueError):
        TopicBridge(DataGateway([broker]), outbound=[Measurement], inbound=[Measurement])
    with pytest.raises(ValueError):
        TopicBridge(DataGateway([broker]), prime=[Measurement], inbound=[Measurement])


async def test_prime_is_produced_restamped_and_repeated():
    broker = InMemoryBroker()
    bus = InProcessBus()
    header = measurement(9, at(0))  # stamped in 2026, as a recording's header is
    pipeline_gateway = DataGateway([InMemoryClient("recording", Measurement, [header])])
    bridge = TopicBridge(DataGateway([broker]), prime=[Measurement], prime_interval=0.05)
    async with running(bridge, bus, pipeline_gateway):
        await asyncio.sleep(0.12)
    assert bridge.produced >= 2
    assert all(r.mRID == "m9" and r.timestamp > at(0) for r in broker.records)
    assert (await newest(DataGateway([broker]), Measurement)).mRID == "m9"


async def test_a_tail_that_ends_is_reopened():
    broker = InMemoryBroker()
    bus = InProcessBus()
    bridge = TopicBridge(DataGateway([broker]), inbound=[Measurement], reconnect_delay=0.02)
    async with running(bridge, bus):
        assert bridge.connected
        broker.drop_tails()
        await asyncio.sleep(0.01)
        assert bridge.status is AppStatus.UNDEFINED
        await asyncio.sleep(0.05)
        assert bridge.connected and bridge.reconnects == 1
        with bus.subscribe(Measurement, overflow=Overflow.GROW) as sub:
            await DataGateway([broker]).produce(now_measurement(4))
            (got,) = await take(sub, 1)
    assert got.mRID == "m4"


async def test_two_sides_run_a_module_across_the_broker():
    broker = InMemoryBroker()
    web_bus, worker_bus = InProcessBus(), InProcessBus()
    web = TopicBridge(DataGateway([broker]), outbound=[Measurement], inbound=[NumberResult], name="web")
    worker = TopicBridge(DataGateway([broker]), outbound=[NumberResult], inbound=[Measurement], name="worker")
    module = Doubler()
    async with running(worker, worker_bus), running(web, web_bus):
        module_task = asyncio.create_task(module.run(worker_bus))
        await asyncio.sleep(0.01)
        try:
            with web_bus.subscribe(NumberResult, overflow=Overflow.GROW) as results:
                web_bus.publish(now_measurement(5))
                (result,) = await take(results, 1)
        finally:
            module_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await module_task
    assert result.result.value == 10.0 and result.app.name == "doubler"


async def test_newest_reads_history_and_never_tails():
    broker = InMemoryBroker()
    gateway = DataGateway([broker])
    assert await asyncio.wait_for(newest(gateway, Measurement), 1) is None
    await gateway.produce(now_measurement(1))
    await gateway.produce(now_measurement(2))
    got = await asyncio.wait_for(newest(gateway, Measurement), 1)
    assert got is not None and got.mRID == "m2"
    assert broker.supports(Measurement, Capability.HISTORY_CONSUME)
