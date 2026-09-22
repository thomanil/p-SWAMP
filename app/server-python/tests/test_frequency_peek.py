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


@pytest.mark.asyncio
async def test_module_keeps_only_the_frequency_per_station(monkeypatch):
    monkeypatch.delenv(api.DATA_CLIENTS_VARIABLE, raising=False)
    pipeline = await api.build_pipeline("7")
    module = FrequencyModule()

    coverage = await pipeline.gateway.coverage(PmuFrame, capability=Capability.HISTORY_CONSUME)
    assert coverage is not None
    frame = None
    async for frame in pipeline.gateway.consume(PmuFrame, coverage.range.start, None):
        break
    assert frame is not None
    result = await module.process(frame)

    assert result is not None
    assert list(result.frequency_hz) == frame.header.stations
    assert all(v is None or 40 < v < 70 for v in result.frequency_hz.values())
    # A frame with another layout re-primes the module rather than being dropped.
    other = PmuHeader(station=["z"], channel=["f"], measurement=["f"], units=["Hz"], data_rate=1.0)
    changed = await module.process(frame.model_copy(update={"header": other, "values": [49.5]}))
    assert changed is not None and changed.frequency_hz == {"z": 49.5}


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
