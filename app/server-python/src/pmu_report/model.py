"""The report module: mean voltage over a range of one PMU stream.

The batch half of the thin slice (STEP-4): the smallest analysis that shows the
query/response path end to end. It is written the way STEP3 §7 says a module
is -- a pure function from a header and a list of samples to a declared
:class:`~pswamp.data.Report` subclass -- and it imports only ``pswamp.data``,
so it runs identically in a test, in a job on the web server, or one day as a
separate service. No FastAPI, no sockets, no clients.

StreamChannel selection is by the header's ``measurement`` key, not by column number:
that is what makes the same function correct over a five-station sample file
and a 700-channel recording.
"""

from __future__ import annotations

from collections.abc import Sequence
from statistics import fmean
from typing import Literal
from uuid import uuid4

from pswamp.data import ModuleRef, Report, Sample, StreamHeader, Value
from pydantic import BaseModel, Field

__all__ = ["MODULE", "StationVoltage", "VoltageReport", "mean_voltage"]

#: Who this report comes from. The uuid is per process, as a module's is.
MODULE = ModuleRef(name="mean_voltage", uuid=uuid4().hex)

#: The header key of the channels this module reads.
VOLTAGE_MEASUREMENT = "v_Magnitude"


class StationVoltage(BaseModel):
    station: str
    mean_voltage: Value = Field(description="Volts; null when the station had no values.")


class VoltageReport(Report):
    """Mean voltage magnitude over ``n_samples`` rows of one stream, in volts."""

    version: Literal["v1"] = "v1"
    stream_id: str
    mean_voltage: Value = Field(description="Volts, across every voltage channel; null if none.")
    stations: list[StationVoltage]


def mean_voltage(header: StreamHeader, samples: Sequence[Sample]) -> VoltageReport:
    """The report over ``samples``, which must be rows of ``header``'s stream."""
    columns = [
        (index, channel.station)
        for index, channel in enumerate(header.channels)
        if channel.measurement == VOLTAGE_MEASUREMENT
    ]

    per_station: list[StationVoltage] = []
    everything: list[float] = []
    for index, station in columns:
        values = [
            row.values[index]
            for row in samples
            if index < len(row.values) and row.values[index] is not None
        ]
        per_station.append(StationVoltage(station=station, mean_voltage=fmean(values) if values else None))
        everything += values

    return VoltageReport(
        module=MODULE,
        mRID=MODULE.uuid,
        stream_id=header.stream_id,
        n_samples=len(samples),
        mean_voltage=fmean(everything) if everything else None,
        stations=per_station,
        parameters={"measurement": VOLTAGE_MEASUREMENT, "channels": len(columns)},
    )
