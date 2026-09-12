# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The measurement layer: a header sent once, and array-valued samples.

STEP3 §4.2's decision, made concrete: channel identity is a *table*
(:class:`StreamHeader`), and a :class:`Sample` is one row of values in that
table's column order. That is the shape every existing p-SWAMP module consumes
(``TimeWindowLabeled`` + its three-row header) and the shape the recording
already stores, so a module selects channels by station or measurement type
rather than by guessing field names.

Conventions, checked by the conformance suite rather than left to comments:

- **time** is a UTC ``datetime`` on the envelope; ``data_rate`` is in Hz;
- **magnitudes are SI** (volts, amperes), **angles are radians**, **frequency is
  absolute Hz** — a provider that reads a source in kV, degrees or C37.118
  deviation encoding converts *before* it yields, so no module ever sees a
  source's units;
- **a missing value is** ``None`` on the wire and may be NaN in memory. The
  serialiser maps NaN and the infinities to ``null`` here, once, because bare
  ``NaN`` is not JSON and a browser's ``JSON.parse`` rejects it.

``values`` is a plain ``list`` in this first cut rather than a numpy-annotated
type; STEP3's ``FloatArray`` is the intended successor once a module consumes it.
"""

from __future__ import annotations

import math
from typing import Annotated, Literal

from pydantic import BaseModel, Field, PlainSerializer

from .base import DataModel

__all__ = ["StreamChannel", "Sample", "StreamHeader", "Unit", "Value"]

Unit = Literal["V", "A", "Hz", "Hz/s", "rad", ""]


def _finite_or_none(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return value


#: A measurement that may be missing: NaN and the infinities become ``null``.
Value = Annotated[float | None, PlainSerializer(_finite_or_none, when_used="json")]


class StreamChannel(BaseModel):
    """One column of a stream — p-SWAMP's three header rows plus a unit.

    ``mRID`` is the CIM equipment id when a deployment has one; it is the hook
    the grid-model track (STEP2 §6.9) attaches to, and ``None`` everywhere else.
    """

    station: str = Field(description='PMU / bus name, e.g. "3000".')
    channel: str = Field(description='Display channel, e.g. "V_Magnitude".')
    measurement: str = Field(description='Quantity key, e.g. "v_Magnitude" or "f".')
    unit: Unit
    mRID: str | None = None


class StreamHeader(DataModel):
    """What a stream carries: sent once, referenced by every sample's ``mRID``."""

    version: Literal["v1"] = "v1"
    stream_id: str = Field(description="What Sample.mRID refers to.")
    data_rate: float = Field(description="Samples per second.")
    channels: list[StreamChannel] = Field(description="Column order of Sample.values.")
    source: str = Field(default="", description="Provenance: a file, a PDC id.")


class Sample(DataModel):
    """One instant across every channel of a stream, in header order."""

    version: Literal["v1"] = "v1"
    mRID: str = Field(description="The StreamHeader.stream_id this row belongs to.")
    values: list[Value] = Field(description="One per header channel; null = missing.")
    quality: int | None = Field(default=None, description="C37.118 STAT word, if any.")
