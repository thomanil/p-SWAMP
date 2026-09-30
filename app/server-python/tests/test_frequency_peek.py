# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The frequency-peek app: the module on its own, and the pipeline end to end."""

from __future__ import annotations

import asyncio

import pytest
from app_test_support import Watch, fresh_transport

from frequency_peek import api, family
from frequency_peek.frequency_module import FrequencyModule, FrequencyResult
from pmu_test_streamer.sample_client import SampleRecordingClient
from pswamp_core.messages import PmuHeader


@pytest.mark.asyncio
async def test_module_keeps_only_the_frequency_per_station():
    module = FrequencyModule()
    frame = SampleRecordingClient().frames[0]

    result = await module.process(frame)

    assert result is not None
    assert list(result.frequency_hz) == frame.header.stations
    assert all(v is None or 40 < v < 70 for v in result.frequency_hz.values())
    # A frame with another layout re-primes the module rather than being dropped.
    other = PmuHeader(station=["z"], channel=["f"], measurement=["f"], units=["Hz"], data_rate=1.0)
    changed = await module.process(frame.model_copy(update={"header": other, "values": [49.5]}))
    assert changed is not None and changed.frequency_hz == {"z": 49.5}


@pytest.fixture(autouse=True)
def _own_transport(monkeypatch):
    with fresh_transport(monkeypatch) as transport:
        yield transport


@pytest.mark.asyncio
async def test_pipeline_goes_live_and_streams_frequency_results(monkeypatch):
    monkeypatch.delenv(family.DATA_CLIENTS_VARIABLE, raising=False)
    async with api.lifespan(None):
        pipeline = api.build_pipeline("42")
        seen = Watch(pipeline)
        await pipeline.start()
        try:
            # The live feed is named first, so it is the source the player starts on.
            assert pipeline.player.status().mode == "live"
            assert pipeline.player.status().sources == ["live", "sample"]
            with seen.subscribe(FrequencyResult) as results:
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
    monkeypatch.delenv(family.DATA_CLIENTS_VARIABLE, raising=False)
    pipeline = api.build_pipeline("43")
    assert api.state_message(pipeline).frequency is None
