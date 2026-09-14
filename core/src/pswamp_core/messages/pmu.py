# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The measurement models: a channel layout, and one instant of every channel.

This is the shape every p-SWAMP application consumes -- one labelled row per
instant (``TimeWindowLabeled`` over a three-row header of station / channel /
measurement) -- made a wire model. The draft's per-PMU objects (one message per
device per quantity) are a valid *ingest* shape for a deployment that receives
PMUs individually; the core itself reads frames. STEP 3 §4.1 keeps that choice
open until the broker-side cost is measured.

``PmuHeader`` is the config-frame analogue: sent once when a stream starts and
again whenever the layout changes. A consumer matches a frame to the header it
holds by ``header_id``, a content hash of the label rows, so a stale header is
detected rather than silently misread.

``PmuFrame.values`` is ``float | None`` rather than ``float`` for the reason the
web port learned the hard way (§4.6 of the port document): NaN is not JSON and is
the *normal* case -- a window is all-NaN until it fills -- so the wire spells it
``null``. pydantic already writes NaN as ``null``; the type makes the contract say
so to the browser.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from .data_model import DataModel

__all__ = ["PmuFrame", "PmuHeader", "header_id_of"]


def header_id_of(
    station: list[str],
    channel: list[str],
    measurement: list[str],
    units: list[str],
    data_rate: float,
) -> str:
    """A short content hash of a header's layout, so frames can name their header."""
    digest = hashlib.sha1()
    for row in (station, channel, measurement, units):
        digest.update("\x1f".join(row).encode())
        digest.update(b"\x1e")
    digest.update(repr(float(data_rate)).encode())
    return digest.hexdigest()[:12]


class PmuHeader(DataModel):
    """The channel layout of a PMU stream: one entry per column of a frame.

    The three label rows are exactly the ones p-SWAMP's ``Indexer`` queries
    (``get_col_idx(measurement="f")``), so an application selects its inputs the
    same way over a wire header as over a decoded config frame.
    """

    version: Literal["v1"] = "v1"
    timestamp: datetime = Field(description="When this layout became valid.")
    mRID: str = Field(description="Identity of the stream (a PDC, a recording).")
    header_id: str = Field(description="Content hash of the label rows; frames carry it.")
    station: list[str] = Field(description="Per column: the station (PMU) the value is from.")
    channel: list[str] = Field(description="Per column: the phasor or signal name.")
    measurement: list[str] = Field(
        description='Per column: "f", "df", "<name>_Magnitude" or "<name>_Angle".'
    )
    units: list[str] = Field(description="Per column: the unit the value is in.")
    data_rate: float = Field(gt=0, description="Frames per second.")
    freq_encoding: Literal["absolute_hz"] = Field(
        default="absolute_hz",
        description="How frequency columns are encoded. Only absolute Hz today.",
    )

    @model_validator(mode="after")
    def _rows_align(self) -> PmuHeader:
        lengths = {
            len(self.station),
            len(self.channel),
            len(self.measurement),
            len(self.units),
        }
        if len(lengths) != 1:
            raise ValueError("station, channel, measurement and units must be the same length")
        return self

    @classmethod
    def build(
        cls,
        *,
        timestamp: datetime,
        mRID: str,
        station: list[str],
        channel: list[str],
        measurement: list[str],
        units: list[str],
        data_rate: float,
    ) -> PmuHeader:
        """Construct a header with its ``header_id`` computed from the rows."""
        return cls(
            timestamp=timestamp,
            mRID=mRID,
            header_id=header_id_of(station, channel, measurement, units, data_rate),
            station=station,
            channel=channel,
            measurement=measurement,
            units=units,
            data_rate=data_rate,
        )

    @property
    def n_columns(self) -> int:
        return len(self.station)

    @property
    def stations(self) -> list[str]:
        """The distinct stations, in column order."""
        seen: dict[str, None] = {}
        for name in self.station:
            seen.setdefault(name, None)
        return list(seen)

    def columns(
        self,
        *,
        station: str | None = None,
        channel: str | None = None,
        measurement: str | None = None,
    ) -> list[int]:
        """Column indexes matching every given label -- the ``Indexer`` query."""
        return [
            index
            for index in range(self.n_columns)
            if (station is None or self.station[index] == station)
            and (channel is None or self.channel[index] == channel)
            and (measurement is None or self.measurement[index] == measurement)
        ]


class PmuFrame(DataModel):
    """One instant of every channel in a stream, in header column order."""

    version: Literal["v1"] = "v1"
    timestamp: datetime = Field(description="The PMU time of this instant. Never stamped later.")
    mRID: str = Field(description="Identity of the stream; matches its header.")
    header_id: str = Field(description="The header this frame's columns follow.")
    values: list[float | None] = Field(description="One value per header column; null where NaN.")
    quality: list[int] | None = Field(
        default=None,
        description="Per column, a place for the C37.118 STAT word. Absent today.",
    )
