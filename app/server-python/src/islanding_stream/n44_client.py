# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Nordic 44 line-trip recording as a data provider.

The grid monitor replays ``pswamp_web/data/n44_line_trip_50hz.npz`` through the
desktop package's own ``io`` seam; this puts the same recording behind the
core's ``DataClient`` contract, so a core pipeline can replay it: 3501 instants
at 50 Hz over 70 s, 44 stations, 700 channels (frequency, its derivative,
voltage and current phasors), with four lines tripped at 20 s -- separating
stations 6500, 6700 and 6701 from the main system -- and reconnected at 40 s.

It is the heavy input on purpose. Each ``PmuFrame`` carries all 700 values and
the 700-column header, so a pipeline replaying it at speed puts a realistic
payload on a module's input topic. ``{NAME}_MEASUREMENTS`` (``f``, say) trims
the columns, which is how to tell the cost of the payload apart from the cost
of the analysis.

The arrays are the grid monitor's own cached ``load_recording()`` (an inward
import, which ``pswamp_web``'s one-way rule allows); frames are built lazily in
``consume``, never materialised -- all of them at once would be ~80 MB of
Python floats per selection.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timedelta
from functools import lru_cache

import numpy as np

from pswamp_core.datagateway import Capability, Coverage, DataClient, EnvSetting, MRIDFilter, TimeRange
from pswamp_core.messages import DataModel, PmuFrame, PmuHeader
from pswamp_core.util.time import UTC

__all__ = ["EPOCH", "STREAM_ID", "N44RecordingClient"]

#: Where the recording's ``t = 0`` sits on the time axis.
EPOCH = datetime(2026, 1, 1, tzinfo=UTC)

#: The stream identity every frame carries as ``mRID``.
STREAM_ID = "n44-line-trip"

#: The recording's units, by measurement name (the simulator's SI output).
_UNITS = {
    "f": "Hz",
    "Df": "Hz/s",
    "v_Magnitude": "V",
    "v_Angle": "rad",
    "i_Magnitude": "A",
    "i_Angle": "rad",
}


def _recording():
    # Imported here, not at module import: pswamp_web pulls in the desktop
    # package, and nothing should pay for that until a client is built.
    from pswamp_web.replay import load_recording

    return load_recording()


@lru_cache(maxsize=4)
def _selection(measurements: tuple[str, ...] | None) -> tuple[np.ndarray, PmuHeader]:
    """The column indexes for a measurement selection, and their shared header."""
    recording = _recording()
    measurement = np.asarray(recording.header["measurement"])
    if measurements:
        columns = np.flatnonzero(np.isin(measurement, list(measurements)))
        if columns.size == 0:
            raise ValueError(f"no columns in the recording measure any of {list(measurements)}")
    else:
        columns = np.arange(measurement.size)
    header = PmuHeader(
        station=[str(x) for x in np.asarray(recording.header["station"])[columns]],
        channel=[str(x) for x in np.asarray(recording.header["channel"])[columns]],
        measurement=[str(x) for x in measurement[columns]],
        units=[_UNITS.get(str(m), "") for m in measurement[columns]],
        data_rate=float(recording.data_rate),
    )
    return columns, header


class N44RecordingClient(DataClient):
    """``DataClient`` over the N44 line-trip recording: history only, exact coverage.

    Args:
        name: The client's name in the gateway; also its environment prefix.
        measurements: Keep only these measurement columns (``["f"]``); all by default.
        priority: Preference against other clients covering the same instant.
    """

    env_settings = (
        EnvSetting(
            "MEASUREMENTS",
            "Comma-separated measurement names to keep (f, Df, v_Magnitude, ...); all 700 columns if unset",
            kind="list",
        ),
        EnvSetting("PRIORITY", "Preference against other clients", default="0", kind="int"),
    )

    def __init__(
        self,
        name: str = "n44",
        measurements: list[str] | tuple[str, ...] | str | None = None,
        *,
        priority: int = 0,
    ) -> None:
        if isinstance(measurements, str):
            measurements = [m.strip() for m in measurements.split(",") if m.strip()]
        self.name = name
        self.capabilities = Capability.HISTORY_CONSUME
        self.supported_models = {PmuFrame}
        self.priority = priority
        recording = _recording()
        self._columns, self.header = _selection(tuple(measurements) if measurements else None)
        self._data = recording.data
        self._times = [EPOCH + timedelta(seconds=float(t)) for t in recording.time]
        self._interval = timedelta(seconds=1.0 / recording.data_rate)

    @property
    def n_frames(self) -> int:
        return len(self._times)

    @property
    def time_range(self) -> TimeRange:
        """``[first instant, last instant + one interval)``."""
        return TimeRange(self._times[0], self._times[-1] + self._interval)

    def frame(self, index: int) -> PmuFrame:
        """The frame at ``index``: that row's selected columns, NaN as ``None``."""
        row = self._data[index, self._columns]
        values: list[float | None] = row.tolist()
        if np.isnan(row).any():
            values = [None if v != v else v for v in values]
        return PmuFrame(timestamp=self._times[index], mRID=STREAM_ID, header=self.header, values=values)

    def frames(self) -> Iterator[PmuFrame]:
        """Every frame, in order, built one at a time."""
        return (self.frame(i) for i in range(self.n_frames))

    async def coverage(self, model: type[DataModel], mRID: MRIDFilter = None) -> Coverage | None:
        if not self.supports(model) or not self._wanted(mRID):
            return None
        return Coverage(self.time_range)

    async def consume(
        self,
        model: type[DataModel],
        time_range: TimeRange,
        mRID: MRIDFilter = None,
    ) -> AsyncIterator[DataModel]:
        if not issubclass(model, PmuFrame) or not self._wanted(mRID):
            return
        for index, moment in enumerate(self._times):
            if time_range.end is not None and moment >= time_range.end:
                return
            if time_range.contains(moment):
                yield self.frame(index)

    async def produce(self, data: DataModel) -> None:
        raise TypeError(f"{self.name} is a read-only recording")

    @staticmethod
    def _wanted(mRID: MRIDFilter) -> bool:
        if mRID is None:
            return True
        wanted = {mRID} if isinstance(mRID, str) else set(mRID)
        return STREAM_ID in wanted
