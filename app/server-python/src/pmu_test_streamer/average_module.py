# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A batch module: the average over a chunk, read straight off the gateway.

The stats module reads every ``PmuFrame`` the player paces onto the bus. This
one reads nothing off the bus (``input_model = None``). It answers an
``AverageRangeCommand`` -- declared here, beside the module, as its result is
-- by asking the *gateway* for exactly that stretch, unpaced, and averaging the
frequencies. That is the "query a chunk" case as a batch job: a bounded read
that never goes through the player, answered as one message carrying the
command's ``request_id``.

``reads_gateway`` says it keeps the gateway (``setup``), which is why it runs
in the pipeline's process even when the stats module runs in a worker. A
provider that fails part-way gives a result with ``error`` set and an
``ErrorEvent``; the module stays up.
"""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import timedelta
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, Field

from pswamp_core.command_routing import CommandRefused
from pswamp_core.messages import Command, ErrorEvent, PmuFrame, ResultEnvelope
from pswamp_core.modules import Module
from pswamp_core.util.time import utcnow

if TYPE_CHECKING:
    from pswamp_core.bus import Bus
    from pswamp_core.datagateway import DataGateway

__all__ = ["AverageRangeCommand", "RangeAverage", "RangeAverageModule", "RangeAverageResult"]


class AverageRangeCommand(Command):
    """Average the frequencies over ``[start, end)`` of the recording (topic ``average.range.command``)."""

    start_offset_s: float = Field(ge=0, description="Where the chunk starts, in seconds from the start of the recording.")
    end_offset_s: float = Field(gt=0, description="Where it ends (exclusive), in seconds from the start of the recording.")


class RangeAverage(BaseModel):
    """The average over one chunk, and what it took to get it."""

    start_offset_s: float
    end_offset_s: float
    frames: int = Field(ge=0, description="Frames read; partial when 'error' is set.")
    mean_frequency_hz: float | None = Field(description="Mean over every station's frequency in the chunk.")
    per_station_hz: dict[str, float | None] = Field(description="Mean frequency per station.")
    elapsed_s: float = Field(ge=0, description="Wall-clock seconds the query took.")
    error: str | None = Field(default=None, description="Why the read stopped early ('Type: text').")


class RangeAverageResult(ResultEnvelope[RangeAverage]):
    """The module's envelope; its class name is its topic: ``range.average.result``."""

    version: Literal["v1"] = "v1"


class RangeAverageModule(Module):
    """Average the frequencies over a chunk when told to, straight off the gateway."""

    name = "range-average"
    input_model = None
    output_model = RangeAverageResult
    commands = (AverageRangeCommand,)
    reads_gateway = True

    def __init__(self) -> None:
        super().__init__()
        self._gateway: DataGateway | None = None
        self._bus: Bus | None = None

    async def setup(self, gateway: DataGateway, bus: Bus) -> None:
        """Keep the gateway, which the query reads, and the bus, for its errors."""
        self._gateway, self._bus = gateway, bus

    def validate(self, command: AverageRangeCommand) -> None:
        if command.end_offset_s <= command.start_offset_s:
            raise CommandRefused("the range is empty: it must end after it starts")

    async def handle(self, command: AverageRangeCommand) -> RangeAverage:
        began = time.monotonic()
        sums: dict[str, float] = defaultdict(float)
        counts: dict[str, int] = defaultdict(int)
        frames = 0
        error: str | None = None
        try:
            if self._gateway is None:
                raise RuntimeError("the module has no gateway: it was not set up by a pipeline")
            coverage = await self._gateway.coverage(PmuFrame)
            if coverage is None or coverage.range.start is None:
                raise RuntimeError("the source reports no history; is it reachable?")
            start = coverage.range.start + timedelta(seconds=command.start_offset_s)
            end = coverage.range.start + timedelta(seconds=command.end_offset_s)
            async for frame in self._gateway.consume(PmuFrame, start, end):
                frames += 1
                for index in frame.header.columns(measurement="f"):
                    value = frame.values[index]
                    if value is not None:
                        station = frame.header.station[index]
                        sums[station] += value
                        counts[station] += 1
        except Exception as failure:
            error = f"{type(failure).__name__}: {failure}"
            if self._bus is not None:
                self._bus.publish(
                    ErrorEvent(
                        timestamp=utcnow(),
                        source=self.name,
                        message="the batch average did not complete: its provider failed",
                        detail=error,
                        request_id=command.request_id,
                    )
                )
        total = sum(counts.values())
        return RangeAverage(
            start_offset_s=command.start_offset_s,
            end_offset_s=command.end_offset_s,
            frames=frames,
            mean_frequency_hz=sum(sums.values()) / total if total else None,
            per_station_hz={station: sums[station] / counts[station] for station in counts},
            elapsed_s=time.monotonic() - began,
            error=error,
        )
