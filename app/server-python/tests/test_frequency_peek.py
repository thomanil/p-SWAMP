# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The frequency-peek app: the module on its own, and the pipeline end to end."""

from __future__ import annotations

import asyncio

import pytest

from frequency_peek import api
from frequency_peek.frequency_module import FrequencyModule, FrequencyResult
from pswamp_core.bus import Overflow
from pswamp_core.datagateway import Capability
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
    pipeline = await api.build_pipeline("7")
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
    pipeline = await api.build_pipeline("42")
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
    pipeline = await api.build_pipeline("43")
    assert api.state_message(pipeline).frequency is None
