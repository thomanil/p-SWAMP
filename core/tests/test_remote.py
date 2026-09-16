# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A module in another process, over the in-memory transport: no port anywhere.

Both sides -- ``RemoteModule`` standing in for the module in a pipeline's
module list, ``ModuleHost`` running the real module per key -- share one
``InMemoryTransport``, which is the whole broker between them.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime
from typing import Literal

import pytest
from support import HISTORY, Measurement, Number, NumberResult, at, measurement, take

from pswamp_core.bus import Bus, InProcessBus, Overflow
from pswamp_core.datagateway import DataGateway, MissingSettingError
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.messages import DataModel
from pswamp_core.modules import Module
from pswamp_core.remote import ModuleHost, RemoteModule, main
from pswamp_core.transport import InMemoryTransport, transport_from_env
from pswamp_core.util.time import utcnow


class Doubler(Module):
    name = "doubler"
    input_model = Measurement
    output_model = NumberResult

    async def process(self, message: Measurement) -> Number | None:
        return Number(value=message.value * 2)


class Scale(DataModel):
    """What ``Scaler.setup`` reads: the factor to multiply by."""

    version: Literal["v1"] = "v1"
    timestamp: datetime
    factor: float


class Scaler(Module):
    """A module that needs a setup record before it can process, and re-primes
    from the bus when one arrives later -- the host-independent shape."""

    name = "scaler"
    input_model = Measurement
    output_model = NumberResult
    setup_models = (Scale,)

    def __init__(self) -> None:
        super().__init__()
        self.factor: float | None = None

    async def setup(self, gateway: DataGateway, bus: Bus) -> None:
        async for scale in gateway.consume(Scale):
            self.factor = scale.factor
        bus.add_listener(Scale, lambda scale: setattr(self, "factor", scale.factor))

    async def process(self, message: Measurement) -> Number | None:
        if self.factor is None:
            return None
        return Number(value=message.value * self.factor)


def scale(factor: float) -> Scale:
    return Scale(mRID="scale", timestamp=at(0), factor=factor)


@contextlib.asynccontextmanager
async def hosting(module_cls, transport, **kwargs):
    host = ModuleHost(module_cls, transport, **kwargs)
    task = asyncio.create_task(host.serve())
    await asyncio.sleep(0.01)
    try:
        yield host
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@contextlib.asynccontextmanager
async def pipeline_side(remote: RemoteModule, gateway: DataGateway | None = None):
    """setup + run as a Pipeline would, on a bus of its own."""
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    await remote.setup(gateway or DataGateway([InMemoryClient("none", Measurement)]), bus)
    task = asyncio.create_task(remote.run(bus))
    await asyncio.sleep(0.01)
    try:
        yield bus
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        bus.bind(None)


async def test_remote_module_forwards_input_and_returns_the_result():
    broker = InMemoryTransport()
    remote = RemoteModule(Doubler, broker, "k1")
    assert (remote.name, remote.input_model, remote.output_model) == ("doubler", Measurement, NumberResult)
    async with hosting(Doubler, broker) as host, pipeline_side(remote) as bus:
        with bus.subscribe(NumberResult, overflow=Overflow.GROW) as results:
            bus.publish(measurement(3, at(3)))
            (result,) = await take(results, 1)
        assert host.keys() == ["k1"]
    assert result.result.value == 6.0 and result.app.name == "doubler"
    assert result.timestamp == at(3)
    assert remote.published == 1 and remote.received == 1 and host.forwarded == 1
    assert host.keys() == []  # shut down with the host


async def test_each_key_gets_its_own_module_and_its_own_results_back():
    broker = InMemoryTransport()
    a, b = RemoteModule(Doubler, broker, "a"), RemoteModule(Doubler, broker, "b")
    async with hosting(Doubler, broker) as host, pipeline_side(a) as bus_a, pipeline_side(b) as bus_b:
        with bus_a.subscribe(NumberResult, overflow=Overflow.GROW) as ra, bus_b.subscribe(
            NumberResult, overflow=Overflow.GROW
        ) as rb:
            bus_a.publish(measurement(1, at(1)))
            bus_b.publish(measurement(10, at(2)))
            (got_a,) = await take(ra, 1)
            (got_b,) = await take(rb, 1)
            await asyncio.sleep(0.02)
            assert ra.get_nowait() is None and rb.get_nowait() is None  # nothing crossed over
        assert sorted(host.keys()) == ["a", "b"]
    assert got_a.result.value == 2.0 and got_b.result.value == 20.0
    assert got_a.app.uuid != got_b.app.uuid  # two instances


async def test_setup_records_are_sent_ahead_and_a_late_host_still_gets_them():
    broker = InMemoryTransport()
    gateway = DataGateway([InMemoryClient("cfg", Scale, [scale(10.0)], capabilities=HISTORY)])
    remote = RemoteModule(Scaler, broker, "k")
    async with pipeline_side(remote, gateway) as bus:
        assert broker.published == 1  # the Scale, retained, before any input
        bus.publish(measurement(2, at(2)))  # nobody hosting yet: dropped, not queued
        async with hosting(Scaler, broker) as host:
            with bus.subscribe(NumberResult, overflow=Overflow.GROW) as results:
                bus.publish(measurement(3, at(3)))
                (result,) = await take(results, 1)
            assert host._slots["k"].module.factor == 10.0
    assert result.result.value == 30.0


async def test_a_setup_record_arriving_after_the_first_input_reprimes_the_module():
    broker = InMemoryTransport()
    remote = RemoteModule(Scaler, broker, "k")  # its gateway holds no Scale
    async with hosting(Scaler, broker) as host, pipeline_side(remote) as bus:
        with bus.subscribe(NumberResult, overflow=Overflow.GROW) as results:
            bus.publish(measurement(1, at(1)))
            await asyncio.sleep(0.02)
            assert results.get_nowait() is None and host._slots["k"].module.factor is None
            await broker.publish(scale(3.0), "k", retained=True)  # as a re-sent header would be
            await asyncio.sleep(0.01)
            bus.publish(measurement(2, at(2)))
            (result,) = await take(results, 1)
    assert result.result.value == 6.0


async def test_an_idle_key_is_evicted_and_rebuilt_on_the_next_message():
    broker = InMemoryTransport()
    remote = RemoteModule(Doubler, broker, "k")
    async with hosting(Doubler, broker, idle_seconds=0.05) as host, pipeline_side(remote) as bus:
        with bus.subscribe(NumberResult, overflow=Overflow.GROW) as results:
            bus.publish(measurement(1, at(1)))
            (first,) = await take(results, 1)
            await asyncio.sleep(0.15)
            assert host.keys() == []
            bus.publish(measurement(2, at(2)))
            (second,) = await take(results, 1)
            assert host.keys() == ["k"]
    assert first.app.uuid != second.app.uuid  # a fresh instance after the eviction


async def test_the_transport_carries_backwards_timestamps():
    """A replay loops: its timestamps go back to the start. A transport never
    looks at them, which is why the hop is not a time-addressed provider."""
    broker = InMemoryTransport()
    remote = RemoteModule(Doubler, broker, "k")
    async with hosting(Doubler, broker), pipeline_side(remote) as bus:
        with bus.subscribe(NumberResult, overflow=Overflow.GROW) as results:
            for moment in (at(5), at(6), at(0), at(1)):
                bus.publish(Measurement(mRID="m", value=1.0, timestamp=moment))
            got = await take(results, 4)
    assert [r.timestamp for r in got] == [at(5), at(6), at(0), at(1)]


async def test_a_retained_subscription_sees_the_newest_per_key_first():
    broker = InMemoryTransport()
    await broker.publish(scale(1.0), "a", retained=True)
    await broker.publish(scale(2.0), "a", retained=True)
    await broker.publish(scale(9.0), "b", retained=True)
    await broker.publish(Measurement(mRID="m", timestamp=utcnow()), "a")  # streamed: not kept
    with broker.subscribe(Scale, "a", retained=True) as only_a:
        assert [(k, m.factor) for k, m in [await only_a.get()]] == [("a", 2.0)]
        assert only_a.get_nowait() is None
    with broker.subscribe(Scale, retained=True) as every:
        got = sorted((k, m.factor) for k, m in await take(every, 2))
    assert got == [("a", 2.0), ("b", 9.0)]
    with broker.subscribe(Measurement, "a") as stream:
        assert stream.get_nowait() is None


def test_transport_from_env(monkeypatch):
    monkeypatch.delenv("X_MODULE_TRANSPORT", raising=False)
    assert transport_from_env("X_MODULE_TRANSPORT") is None
    monkeypatch.setenv("X_MODULE_TRANSPORT", "mem:pswamp_core.transport:InMemoryTransport")
    transport = transport_from_env("X_MODULE_TRANSPORT")
    assert isinstance(transport, InMemoryTransport) and transport.name == "mem"
    monkeypatch.setenv("X_MODULE_TRANSPORT", "not-a-spec")
    with pytest.raises(MissingSettingError):
        transport_from_env("X_MODULE_TRANSPORT")
    monkeypatch.setenv("X_MODULE_TRANSPORT", "m:pswamp_core.modules:Module")
    with pytest.raises(MissingSettingError):
        transport_from_env("X_MODULE_TRANSPORT")


def test_worker_main_exits_2_without_a_transport(monkeypatch, capsys):
    monkeypatch.delenv("X_MODULE_TRANSPORT", raising=False)
    assert main(Doubler, "X_MODULE_TRANSPORT") == 2
    assert "X_MODULE_TRANSPORT is unset" in capsys.readouterr().err
