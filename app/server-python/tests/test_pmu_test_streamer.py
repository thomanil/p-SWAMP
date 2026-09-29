# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The PMU test streamer's pieces over the core -- no server started."""

from __future__ import annotations

from datetime import datetime, timezone

from pmu_test_streamer.stats_module import FrameStatsModule, FrameStatsResult
from pswamp_core.messages import PmuFrame, PmuHeader

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)

TWO_STATIONS = PmuHeader(
    station=["a", "a", "a", "b", "b", "b"],
    channel=["V", "V", "f", "V", "V", "f"],
    measurement=["V_Magnitude", "V_Angle", "f"] * 2,
    units=["kV", "deg", "Hz"] * 2,
    data_rate=20.0,
)


# --- the module ------------------------------------------------------------------------


async def test_stats_module_computes_per_frame():
    module = FrameStatsModule()
    frame = PmuFrame(timestamp=T0, mRID="s", header=TWO_STATIONS, values=[400.0, 10.0, 50.0, 410.0, -5.0, 50.2])

    stats = await module.process(frame)

    assert stats.n_stations == 2
    assert stats.mean_frequency_hz == 50.1
    assert (stats.min_frequency_hz, stats.max_frequency_hz) == (50.0, 50.2)
    assert stats.angle_spread_deg == 15.0
    assert stats.mean_voltage_kv == 405.0
    assert FrameStatsResult.topic == "frame.stats.result"


async def test_the_module_primes_itself_from_the_frame_and_follows_a_layout_change():
    """What makes the module host-independent: the layout comes with the frame,
    so a fresh instance works from the first frame it sees, and a frame with a
    different layout re-primes it."""
    module = FrameStatsModule()
    await module.process(PmuFrame(timestamp=T0, mRID="s", header=TWO_STATIONS, values=[1.0] * 6))
    assert module.parameters["header_id"] == TWO_STATIONS.header_id

    other = PmuHeader(station=["z"], channel=["f"], measurement=["f"], units=["Hz"], data_rate=1.0)
    stats = await module.process(PmuFrame(timestamp=T0, mRID="z", header=other, values=[49.5]))
    assert stats.n_stations == 1 and stats.mean_frequency_hz == 49.5
    assert module.parameters == {"header_id": other.header_id, "stations": ["z"]}
