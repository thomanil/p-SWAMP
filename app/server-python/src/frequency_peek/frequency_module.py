# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The frequency module: one ``PmuFrame`` in, the per-station frequencies out.

The smallest module that *reduces* a frame -- it keeps one measurement of the
seven hundred-odd values in a frame and drops the rest. It reads ``PmuFrame``
off its pipeline's bus and publishes ``FrequencyResult`` back onto it; the page
subscribes to that, never to this module. Which columns carry ``f`` it reads
off the frame's own header, re-deriving them when the layout changes; nothing
else here knows the layout.

Imports only ``pswamp_core``.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from pswamp_core.messages import PmuFrame, PmuHeader, ResultEnvelope
from pswamp_core.modules import Module

__all__ = ["Frequencies", "FrequencyModule", "FrequencyResult"]


class Frequencies(BaseModel):
    """Only the frequency: one value per station, in the header's station order."""

    frequency_hz: dict[str, float | None] = Field(
        description="Measured frequency per station, in Hz; null where the frame has none."
    )


class FrequencyResult(ResultEnvelope[Frequencies]):
    """The module's envelope; its class name is its topic: ``frequency.result``."""

    version: Literal["v1"] = "v1"


class FrequencyModule(Module):
    """Pick the frequency channel of every station out of each frame."""

    name = "frequency"
    input_model = PmuFrame
    output_model = FrequencyResult

    def __init__(self) -> None:
        super().__init__()
        self._header_id: str | None = None
        self._columns: list[tuple[str, int]] = []  # (station, column index)

    def use_header(self, header: PmuHeader) -> None:
        """Derive the frequency columns for ``header``; called on a change of layout."""
        self._header_id = header.header_id
        self._columns = [(header.station[i], i) for i in header.columns(measurement="f")]
        self.parameters = {"header_id": header.header_id}

    async def process(self, frame: PmuFrame) -> Frequencies | None:
        if frame.header.header_id != self._header_id:
            self.use_header(frame.header)
        return Frequencies(frequency_hz={station: frame.values[i] for station, i in self._columns})
