# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The sample recording as a data provider: ``sample_data.txt`` behind the contract.

This is a **provider written outside the core** on purpose. It imports
``pswamp_core.datagateway`` and ``pswamp_core.messages`` and nothing else, which
is exactly what a TSO's own provider would do (STEP 1 A8): implement
``DataClient``, declare what it can do (``HISTORY_CONSUME`` -- a file cannot
tail live data), answer ``coverage`` exactly, and stream ``PmuHeader`` and
``PmuFrame`` for any time window. The conformance suite in
``tests/test_pmu_test_streamer.py`` is what proves it.

``sample_data.txt`` is a one-off sample committed for testing: 300 lines of
simulated PMU records extracted by hand from the Nordic 44 simulation, five
stations at 20 Hz, one line per station per instant::

    t= 0.050s  PMU=3000  V= 419.95kV  ang=   -0.00deg  f= 49.9999Hz

Where the old streamer replayed the *lines* verbatim, this parses them once into
the core's wire shape: one ``PmuHeader`` (five stations × three columns:
``V_Magnitude`` in kV, ``V_Angle`` in degrees, ``f`` in Hz) and sixty
``PmuFrame``s, each one instant of all fifteen values. Timestamps are the file's
relative ``t`` anchored at a fixed UTC epoch, so the recording sits at a known
place on the time axis and every replay of it is addressable by time.

The file is read lazily, on the first client built from it, and cached by path
-- not at import, so importing the package has no side effect.
"""

from __future__ import annotations

import re
import statistics
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

from pswamp_core.datagateway import (
    Capability,
    Coverage,
    DataClient,
    EnvSetting,
    MRIDFilter,
    TimeRange,
)
from pswamp_core.messages import DataModel, PmuFrame, PmuHeader
from pswamp_core.util.time import UTC

__all__ = ["DEFAULT_PATH", "EPOCH", "STREAM_ID", "SampleRecording", "SampleRecordingClient", "load_sample"]

#: The file beside this module. Ships in the image with the package.
DEFAULT_PATH = Path(__file__).parent / "sample_data.txt"

#: Where the recording's ``t = 0`` sits on the time axis.
EPOCH = datetime(2026, 1, 1, tzinfo=UTC)

#: The stream identity every header and frame carries as ``mRID``.
STREAM_ID = "n44-sample"

_LINE = re.compile(
    r"t=\s*(?P<t>[-\d.]+)s\s+PMU=(?P<pmu>\S+)\s+V=\s*(?P<v>[-\d.]+)kV\s+"
    r"ang=\s*(?P<ang>[-\d.]+)deg\s+f=\s*(?P<f>[-\d.]+)Hz"
)

_MEASUREMENTS = ("V_Magnitude", "V_Angle", "f")
_CHANNELS = ("V", "V", "f")
_UNITS = ("kV", "deg", "Hz")


@dataclass(frozen=True)
class SampleRecording:
    """The parsed file: one header and its frames, in time order."""

    path: Path
    header: PmuHeader
    frames: tuple[PmuFrame, ...]

    @property
    def frame_interval(self) -> timedelta:
        return timedelta(seconds=1.0 / self.header.data_rate)

    @property
    def coverage(self) -> TimeRange:
        """``[first frame, last frame + one interval)`` -- the exclusive end is
        where the next frame would have been, so ``count = span / interval``."""
        return TimeRange(self.frames[0].timestamp, self.frames[-1].timestamp + self.frame_interval)


@lru_cache(maxsize=4)
def load_sample(path: Path = DEFAULT_PATH) -> SampleRecording:
    """Parse the sample file into a header and frames. Cached by path."""
    instants: dict[float, dict[str, tuple[float, float, float]]] = {}
    stations: dict[str, None] = {}
    for number, line in enumerate(path.read_text().splitlines(), start=1):
        if not line.strip():
            continue
        match = _LINE.match(line.strip())
        if match is None:
            raise ValueError(f"{path}:{number}: not a sample record: {line!r}")
        t = float(match["t"])
        stations.setdefault(match["pmu"], None)
        instants.setdefault(t, {})[match["pmu"]] = (
            float(match["v"]),
            float(match["ang"]),
            float(match["f"]),
        )
    if not instants:
        raise ValueError(f"{path}: no records")

    times = sorted(instants)
    deltas = [b - a for a, b in zip(times, times[1:]) if b > a]
    data_rate = 1.0 / statistics.median(deltas) if deltas else 1.0

    names = list(stations)
    header = PmuHeader.build(
        timestamp=EPOCH,
        mRID=STREAM_ID,
        station=[name for name in names for _ in _MEASUREMENTS],
        channel=[channel for _ in names for channel in _CHANNELS],
        measurement=[m for _ in names for m in _MEASUREMENTS],
        units=[unit for _ in names for unit in _UNITS],
        data_rate=round(data_rate, 6),
    )
    frames = []
    for t in times:
        row = instants[t]
        values: list[float | None] = []
        for name in names:
            triple = row.get(name)
            values.extend(triple if triple is not None else (None, None, None))
        frames.append(
            PmuFrame(
                timestamp=EPOCH + timedelta(seconds=t),
                mRID=STREAM_ID,
                header_id=header.header_id,
                values=values,
            )
        )
    return SampleRecording(path=path, header=header, frames=tuple(frames))


class SampleRecordingClient(DataClient):
    """``DataClient`` over the sample file: history only, exact coverage.

    Args:
        name: The client's name in the gateway; also its environment prefix.
        path: The file to serve. ``{NAME}_PATH`` when built ``from_env``.
        priority: Preference against other clients covering the same instant.
    """

    env_settings = (
        EnvSetting(
            "PATH",
            "The sample text file to replay (one PMU record per line)",
            default=str(DEFAULT_PATH),
            kind="path",
        ),
        EnvSetting("PRIORITY", "Preference against other clients", default="0", kind="int"),
    )

    def __init__(self, name: str = "sample", path: Path | str = DEFAULT_PATH, *, priority: int = 0):
        self.name = name
        self.capabilities = Capability.HISTORY_CONSUME
        self.supported_models = {PmuHeader, PmuFrame}
        self.priority = priority
        self.recording = load_sample(Path(path))

    @property
    def header(self) -> PmuHeader:
        return self.recording.header

    @property
    def frames(self) -> tuple[PmuFrame, ...]:
        return self.recording.frames

    async def coverage(self, model: type[DataModel], mRID: MRIDFilter = None) -> Coverage | None:
        if not self.supports(model) or not self._wanted(mRID):
            return None
        if issubclass(model, PmuHeader):
            header = self.recording.header
            return Coverage(TimeRange(header.timestamp, header.timestamp + timedelta(microseconds=1)))
        return Coverage(self.recording.coverage)

    async def consume(
        self,
        model: type[DataModel],
        time_range: TimeRange,
        mRID: MRIDFilter = None,
    ) -> AsyncIterator[DataModel]:
        if not self._wanted(mRID):
            return
        records: tuple[DataModel, ...]
        if issubclass(model, PmuHeader):
            records = (self.recording.header,)
        elif issubclass(model, PmuFrame):
            records = self.recording.frames
        else:
            return
        for record in records:
            if time_range.end is not None and record.timestamp >= time_range.end:
                return
            if time_range.contains(record.timestamp):
                yield record

    async def produce(self, data: DataModel) -> None:
        raise TypeError(f"{self.name} is a read-only recording")

    @staticmethod
    def _wanted(mRID: MRIDFilter) -> bool:
        if mRID is None:
            return True
        wanted = {mRID} if isinstance(mRID, str) else set(mRID)
        return STREAM_ID in wanted
