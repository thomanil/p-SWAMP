"""The committed PMU sample as a provider: ``sample_data.txt`` behind the contract.

The file is a **one-off sample committed for testing** -- 300 simulated PMU
records extracted by hand from the Nordic 44 simulation under
``examples/nordic44_rtsim/``: five stations at 20 Hz for three seconds, one
record per line::

    t= 0.050s  PMU=3000  V= 419.95kV  ang=   -0.00deg  f= 49.9999Hz

It used to be read straight into a list of strings by the streamer's own model,
which is what the data-integration track set out to undo. Now it is a
:class:`~pswamp.data.DataClient` like any other backend: it declares what it
holds (the file's span, not live), serves a :class:`~pswamp.data.StreamHeader`
and :class:`~pswamp.data.Sample` rows through ``consume``, and nothing
downstream knows it is a text file. Swapping it for a recording, an archive or a
live PDC is a different client with the same three methods -- the A8 story of
STEP1, in the smallest form that runs.

Two decisions the provider makes so no consumer has to (STEP3 §4.2):

- **Units are converted here.** The file says kV and degrees; the stream says
  volts and radians, because that is what every p-SWAMP module expects.
- **The timeline is placed at a fixed epoch.** The file carries relative
  seconds; the contract routes on absolute time. A real archive has wall-clock
  stamps and needs no epoch; this one is pinned to a date in the past so the
  planner never mistakes it for something live.

Written against ``pswamp.data`` and nothing else, on purpose: this is what a
deployment's own provider looks like, minus the deployment.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from math import radians
from pathlib import Path
from typing import Self

from pswamp.data import (
    Capability,
    StreamChannel,
    Coverage,
    DataClient,
    DataModel,
    EnvSetting,
    MRIDFilter,
    Sample,
    StreamHeader,
    TimeRange,
)
from pswamp.data.gateway.client import ModelSelector, normalise_mrid_filter
from pswamp.data.gateway.config import env_int, env_str

__all__ = [
    "DEFAULT_FILE",
    "RECORDING_EPOCH",
    "STREAM_ID",
    "SampleFileClient",
    "parse_records",
]

#: The data file lives beside this module, so the Dockerfile's ``COPY src/``
#: ships it with no build change.
DEFAULT_FILE = Path(__file__).parent / "sample_data.txt"

#: Where the file's ``t=0`` sits on the absolute timeline. In the past, on purpose.
RECORDING_EPOCH = datetime(2026, 1, 1, tzinfo=UTC)

#: The one stream this provider serves; what every Sample.mRID says.
STREAM_ID = "n44-sample"

_LINE = re.compile(
    r"t=\s*(?P<t>[-\d.]+)s\s+PMU=(?P<station>\w+)\s+V=\s*(?P<v>[-\d.]+)kV"
    r"\s+ang=\s*(?P<ang>[-\d.]+)deg\s+f=\s*(?P<f>[-\d.]+)Hz"
)

#: Padding past the newest record so the half-open coverage includes it.
_END_PADDING = timedelta(microseconds=1)


def parse_records(
    text: str,
    *,
    source: str = "",
    epoch: datetime = RECORDING_EPOCH,
    stream_id: str = STREAM_ID,
) -> tuple[StreamHeader, list[Sample]]:
    """Turn the record lines into one header and one sample per instant.

    StreamChannel order is per station in order of first appearance, three channels
    each -- ``V_Magnitude`` (V), ``V_Angle`` (rad), ``Frequency`` (Hz) -- using
    the channel/measurement names p-SWAMP's own header uses.
    """
    rows: dict[float, dict[str, tuple[float, float, float]]] = {}
    stations: list[str] = []

    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        match = _LINE.match(line.strip())
        if match is None:
            raise ValueError(f"{source or 'sample data'} line {number}: unrecognised record {line!r}")
        station = match["station"]
        if station not in stations:
            stations.append(station)
        rows.setdefault(float(match["t"]), {})[station] = (
            float(match["v"]) * 1000.0,
            radians(float(match["ang"])),
            float(match["f"]),
        )

    if not rows:
        raise ValueError(f"{source or 'sample data'} holds no records")

    times = sorted(rows)
    data_rate = 1.0 / (times[1] - times[0]) if len(times) > 1 else 0.0

    channels: list[StreamChannel] = []
    for station in stations:
        channels += [
            StreamChannel(station=station, channel="V_Magnitude", measurement="v_Magnitude", unit="V"),
            StreamChannel(station=station, channel="V_Angle", measurement="v_Angle", unit="rad"),
            StreamChannel(station=station, channel="Frequency", measurement="f", unit="Hz"),
        ]

    header = StreamHeader(
        stream_id=stream_id,
        mRID=stream_id,
        data_rate=round(data_rate, 6),
        channels=channels,
        source=source,
        timestamp=epoch + timedelta(seconds=times[0]),
    )

    samples: list[Sample] = []
    for t in times:
        values: list[float | None] = []
        for station in stations:
            values += list(rows[t].get(station, (None, None, None)))
        samples.append(Sample(mRID=stream_id, timestamp=epoch + timedelta(seconds=t), values=values))

    return header, samples


class SampleFileClient(DataClient):
    """A read-only, history-only provider over one file of PMU records."""

    env_settings = (
        EnvSetting("PATH", "Text file of PMU records, one per line", default=str(DEFAULT_FILE)),
        EnvSetting("PRIORITY", "Preference against other clients", default="10"),
    )

    def __init__(
        self,
        name: str,
        path: str | Path = DEFAULT_FILE,
        *,
        priority: int = 10,
        epoch: datetime = RECORDING_EPOCH,
        stream_id: str = STREAM_ID,
    ):
        self.name = name
        self.priority = priority
        self.capabilities = Capability.HISTORY_CONSUME
        self.supported_models = {StreamHeader, Sample}
        self.path = Path(path)
        self.header, self.samples = parse_records(
            self.path.read_text(), source=self.path.name, epoch=epoch, stream_id=stream_id
        )

    @classmethod
    def from_env(cls, name: str, models: ModelSelector) -> Self:
        return cls(
            name,
            env_str(name, "PATH", str(DEFAULT_FILE)),
            priority=env_int(name, "PRIORITY", 10),
        )

    def _serves(self, model: type[DataModel], mRID: MRIDFilter) -> bool:
        wanted = normalise_mrid_filter(mRID)
        return self.supports(model) and (wanted is None or self.header.stream_id in wanted)

    async def coverage(self, model: type[DataModel], mRID: MRIDFilter = None) -> Coverage | None:
        if not self._serves(model, mRID):
            return None
        return Coverage(
            range=TimeRange(self.samples[0].timestamp, self.samples[-1].timestamp + _END_PADDING),
            live=False,
        )

    async def consume(
        self,
        model: type[DataModel],
        time_range: TimeRange,
        mRID: MRIDFilter = None,
    ) -> AsyncIterator[DataModel]:
        if not self._serves(model, mRID):
            return

        if issubclass(StreamHeader, model):
            if time_range.contains(self.header.timestamp):
                yield self.header
            return

        for sample in self.samples:
            if time_range.end is not None and sample.timestamp >= time_range.end:
                return
            if time_range.contains(sample.timestamp):
                yield sample

    async def produce(self, data: DataModel) -> None:
        raise TypeError(f"{self.name} is read-only: {type(data).__name__} cannot be produced into a sample file")
