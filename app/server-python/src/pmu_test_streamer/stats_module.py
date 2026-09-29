# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The example module: per-frame statistics, consumed from and published to the bus.

The point is the shape, not the arithmetic. ``FrameStatsModule`` reads
``PmuFrame`` off the pipeline's bus and publishes ``FrameStatsResult`` -- a
different class on a different topic -- and the page subscribes to *that*,
never to the module.

It reads the stream's layout off each frame (``frame.header``): the column
indexes are derived once per layout and kept until a frame arrives with a
different ``header_id``. So it needs no setup, and runs the same in the
pipeline's process or in a worker.

It is also the example of **a module that takes a command**: it keeps a
running count and the largest angle spread seen, and ``ResetStatsCommand`` --
declared here, beside the module, not in the core -- zeroes them. The pipeline
routes the command here by its class; in a worker it crosses the transport.

And it reads the frame's **CIM reference** (``header.cimReferenceId``, stamped
by the gateway's enricher) and hands it on in its result: the reference rides
in the frame, so the module needs no configuration for it, in a worker either.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from pswamp_core.command_routing import CommandRefused
from pswamp_core.messages import Command, PmuFrame, PmuHeader, ResultEnvelope
from pswamp_core.modules import Module

__all__ = ["FrameStats", "FrameStatsModule", "FrameStatsResult", "ResetStatsCommand"]


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
    frames_since_reset: int = Field(description="Frames processed since the last reset.")
    peak_angle_spread_deg: float | None = Field(
        description="Largest angle spread seen since the last reset."
    )
    cim_reference_id: str | None = Field(
        description="The grid (CIM) data these stats refer to, as the gateway stamped it on the frame."
    )


class FrameStatsResult(ResultEnvelope[FrameStats]):
    """The module's envelope; its class name is its topic: ``frame.stats.result``."""

    version: Literal["v1"] = "v1"


class ResetStatsCommand(Command):
    """Zero the module's running count and peak (topic ``reset.stats.command``)."""


class FrameStatsModule(Module):
    """Mean/min/max frequency, angle spread and mean voltage per frame, plus a
    running count and peak since the last ``ResetStatsCommand``."""

    name = "frame-stats"
    input_model = PmuFrame
    output_model = FrameStatsResult
    commands = (ResetStatsCommand,)

    def __init__(self) -> None:
        super().__init__()
        self._header_id: str | None = None
        self._f_cols: list[int] = []
        self._v_cols: list[int] = []
        self._ang_cols: list[int] = []
        self._frames = 0
        self._peak_spread: float | None = None

    def use_header(self, header: PmuHeader) -> None:
        """Derive the column indexes for ``header``; called on a change of layout."""
        self._header_id = header.header_id
        self._f_cols = header.columns(measurement="f")
        self._v_cols = header.columns(measurement="V_Magnitude")
        self._ang_cols = header.columns(measurement="V_Angle")
        self.parameters = {"header_id": header.header_id, "stations": header.stations}

    async def process(self, frame: PmuFrame) -> FrameStats | None:
        if frame.header.header_id != self._header_id:
            self.use_header(frame.header)
        f = [v for i in self._f_cols if (v := frame.values[i]) is not None]
        v = [x for i in self._v_cols if (x := frame.values[i]) is not None]
        ang = [a for i in self._ang_cols if (a := frame.values[i]) is not None]
        spread = (max(ang) - min(ang)) if ang else None
        self._frames += 1
        if spread is not None and (self._peak_spread is None or spread > self._peak_spread):
            self._peak_spread = spread
        return FrameStats(
            n_stations=len(f),
            mean_frequency_hz=sum(f) / len(f) if f else None,
            min_frequency_hz=min(f) if f else None,
            max_frequency_hz=max(f) if f else None,
            angle_spread_deg=spread,
            mean_voltage_kv=sum(v) / len(v) if v else None,
            frames_since_reset=self._frames,
            peak_angle_spread_deg=self._peak_spread,
            cim_reference_id=frame.header.cimReferenceId,
        )

    def validate(self, command: ResetStatsCommand) -> None:
        if self._frames == 0:
            raise CommandRefused("nothing to reset: no frames since the last reset")

    async def handle(self, command: ResetStatsCommand) -> FrameStats | None:
        """Zero the counters, and answer with the last stats as they now read:
        published like any result, carrying the command's ``request_id``."""
        self._frames = 0
        self._peak_spread = None
        if self.last_result is None:
            return None
        return self.last_result.result.model_copy(
            update={"frames_since_reset": 0, "peak_angle_spread_deg": None}
        )
