# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The frequency-peek app: the module on its own, and the pipeline end to end."""

from __future__ import annotations

import asyncio

import pytest

from frequency_peek import api, worker
from frequency_peek.frequency_module import FrequencyModule, FrequencyResult
from pswamp_core.bridge import TopicBridge
from pswamp_core.bus import InProcessBus, Overflow
from pswamp_core.datagateway import Capability, DataGateway, Player
from pswamp_core.datagateway.clients import InMemoryBroker
from pswamp_core.messages import PmuFrame, PmuHeader


async def _header(pipeline) -> PmuHeader:
    header = None
    async for message in pipeline.gateway.consume(PmuHeader):
        header = message
    assert header is not None
    return header


@pytest.mark.asyncio
async def test_module_keeps_only_the_frequency_per_station(monkeypatch):
    monkeypatch.delenv(api.DATA_CLIENTS_VARIABLE, raising=False)
    monkeypatch.delenv(api.BUS_CLIENTS_VARIABLE, raising=False)
    pipeline = await api.build_pipeline("live")
    header = await _header(pipeline)
    module = FrequencyModule()
    module.use_header(header)

    coverage = await pipeline.gateway.coverage(PmuFrame, capability=Capability.HISTORY_CONSUME)
    assert coverage is not None
    frame = None
    async for frame in pipeline.gateway.consume(PmuFrame, coverage.range.start, None):
        break
    assert frame is not None
    result = await module.process(frame)

    assert result is not None
    assert list(result.frequency_hz) == header.stations
    assert all(v is None or 40 < v < 70 for v in result.frequency_hz.values())
    assert await module.process(frame.model_copy(update={"header_id": "other"})) is None


@pytest.mark.asyncio
async def test_pipeline_goes_live_and_streams_frequency_results(monkeypatch):
    monkeypatch.delenv(api.DATA_CLIENTS_VARIABLE, raising=False)
    monkeypatch.delenv(api.BUS_CLIENTS_VARIABLE, raising=False)
    pipeline = await api.build_pipeline("live")
    await pipeline.start()
    try:
        assert pipeline.player.status().mode == "live"
        with pipeline.bus.subscribe(FrequencyResult, overflow=Overflow.GROW) as results:
            result = await asyncio.wait_for(results.get(), 2)
        assert result.app.name == "frequency"
        assert len(result.result.frequency_hz) == 5

        message = api.state_message(pipeline)
        assert message.player.mode == "live"
        assert message.frequency is not None
        assert message.frequency.timestamp >= result.timestamp
    finally:
        await pipeline.stop()


@pytest.mark.asyncio
async def test_state_before_the_first_frame_carries_no_result(monkeypatch):
    monkeypatch.delenv(api.DATA_CLIENTS_VARIABLE, raising=False)
    monkeypatch.delenv(api.BUS_CLIENTS_VARIABLE, raising=False)
    pipeline = await api.build_pipeline("live")
    assert api.state_message(pipeline).frequency is None


# --- one shared pipeline, and the module in another process ------------------------

IN_MEMORY_BUS = "bus:pswamp_core.datagateway.clients.in_memory:InMemoryBroker"


async def test_every_viewer_shares_one_live_pipeline(monkeypatch):
    monkeypatch.delenv(api.DATA_CLIENTS_VARIABLE, raising=False)
    monkeypatch.delenv(api.BUS_CLIENTS_VARIABLE, raising=False)
    registry = api.PipelineRegistry(api.build_pipeline, max_pipelines=api.MAX_PIPELINES, idle_seconds=60)
    registry.bind(asyncio.get_running_loop())
    try:
        first = await registry.acquire(api.PIPELINE_KEY)
        second = await registry.acquire(api.PIPELINE_KEY)
        assert first is second and registry.watchers(api.PIPELINE_KEY) == 2
        assert api.MAX_PIPELINES == 1 and first.player.status().mode == "live"
    finally:
        await registry.stop_all()


def test_environment_picks_the_bridge(monkeypatch):
    monkeypatch.delenv(api.BUS_CLIENTS_VARIABLE, raising=False)
    assert api.bus_gateway() is None
    assert isinstance(api.frequency_modules(None)[0], FrequencyModule)
    monkeypatch.setenv(api.BUS_CLIENTS_VARIABLE, IN_MEMORY_BUS)
    gateway = api.bus_gateway()
    assert gateway is not None and list(gateway.clients) == ["bus"]
    (module,) = api.frequency_modules(gateway)
    assert isinstance(module, TopicBridge)
    assert module.parameters == {
        "outbound": ["pmu.frame"], "inbound": ["frequency.result"], "prime": ["pmu.header"],
    }


@pytest.mark.asyncio
async def test_module_behaves_the_same_over_the_broker(monkeypatch):
    monkeypatch.delenv(api.DATA_CLIENTS_VARIABLE, raising=False)
    broker = InMemoryBroker()
    worker_task = asyncio.create_task(worker.serve(DataGateway([broker]), header_poll=0.02))

    gateway = api.gateway_from_env(api.DEFAULT_DATA_CLIENTS, variable=api.DATA_CLIENTS_VARIABLE)
    bus = InProcessBus()
    player = Player(gateway, bus, model=PmuFrame, autoplay=True, loop=True)
    pipeline = api.LivePipeline("live", gateway, bus, player, api.frequency_modules(DataGateway([broker])))
    await pipeline.start()
    try:
        assert pipeline.player.status().mode == "live"
        with bus.subscribe(FrequencyResult, overflow=Overflow.GROW) as results:
            result = await asyncio.wait_for(results.get(), 3)
        # Built by the real module, on the worker's bus, and back over the broker.
        assert result.app.name == "frequency"
        assert list(result.result.frequency_hz) == ["3000", "3245", "5100", "6500", "7000"]
        assert api.state_message(pipeline).frequency is not None
        assert any(isinstance(r, PmuHeader) for r in broker.records)  # primed, and re-stamped
    finally:
        await pipeline.stop()
        worker_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker_task
