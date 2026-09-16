# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A module in miniature: per-frame statistics, consumed from and published to
the bus.

The point is the shape, not the arithmetic. ``FrameStatsModule`` reads
``PmuFrame`` off the pipeline's bus and publishes ``FrameStatsResult`` back onto
it -- a different message class on a different topic -- and the page subscribes
to *that*, never to the module. Nothing in ``api.py`` names this module beyond
constructing it. That is STEP 1 A3 ("a module reads a topic, analyses, writes a
different-typed result to another topic") on the smallest possible example.

Result models are ordinary pydantic, so ``FrameStatsResult`` enters the
generated OpenAPI contract as-is: the browser's type for it is generated, not
hand-written, and no adapter sits between the module and the page.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from pswamp_core.bus import Bus
from pswamp_core.datagateway import DataGateway
from pswamp_core.messages import PmuFrame, PmuHeader, ResultEnvelope
from pswamp_core.modules import Module

__all__ = ["FrameStats", "FrameStatsModule", "FrameStatsResult"]


class FrameStats(BaseModel):
    """What the module computes for one instant."""

    n_stations: int = Field(description="Stations with a frequency value in this frame.")
    mean_frequency_hz: float | None = Field(description="Mean of the per-station frequencies.")
    min_frequency_hz: float | None
    max_frequency_hz: float | None
    angle_spread_deg: float | None = Field(
        description="Largest minus smallest voltage angle across stations."
    )
    mean_voltage_kv: float | None


class FrameStatsResult(ResultEnvelope[FrameStats]):
    """The module's envelope; its class name is its topic: ``frame.stats.result``."""

    version: Literal["v1"] = "v1"


class FrameStatsModule(Module):
    """Mean/min/max frequency, angle spread and mean voltage per frame."""

    name = "frame-stats"
    input_model = PmuFrame
    output_model = FrameStatsResult
    #: What ``setup`` reads from the gateway -- so a host running this module
    #: in another process (``pswamp_core.remote``) knows to carry it across.
    setup_models = (PmuHeader,)

    def __init__(self) -> None:
        super().__init__()
        self._header: PmuHeader | None = None
        self._f_cols: list[int] = []
        self._v_cols: list[int] = []
        self._ang_cols: list[int] = []

    async def setup(self, gateway: DataGateway, bus: Bus) -> None:
        """Read the stream's header, to know which columns are which -- and keep
        listening for one on the bus, so a header that arrives later (a worker
        that started after the pipeline, a changed layout) re-primes the module
        the same way. In-process the gateway read is the whole story; in
        another process the bus is how the host hands over a late one."""
        async for header in gateway.consume(PmuHeader):
            self.use_header(header)
        bus.add_listener(PmuHeader, self.use_header)

    def use_header(self, header: PmuHeader) -> None:
        self._header = header
        self._f_cols = header.columns(measurement="f")
        self._v_cols = header.columns(measurement="V_Magnitude")
        self._ang_cols = header.columns(measurement="V_Angle")
        self.parameters = {"header_id": header.header_id, "stations": header.stations}

    async def process(self, frame: PmuFrame) -> FrameStats | None:
        if self._header is None or frame.header_id != self._header.header_id:
            return None
        f = [v for i in self._f_cols if (v := frame.values[i]) is not None]
        v = [x for i in self._v_cols if (x := frame.values[i]) is not None]
        ang = [a for i in self._ang_cols if (a := frame.values[i]) is not None]
        return FrameStats(
            n_stations=len(f),
            mean_frequency_hz=sum(f) / len(f) if f else None,
            min_frequency_hz=min(f) if f else None,
            max_frequency_hz=max(f) if f else None,
            angle_spread_deg=(max(ang) - min(ang)) if ang else None,
            mean_voltage_kv=sum(v) / len(v) if v else None,
        )
