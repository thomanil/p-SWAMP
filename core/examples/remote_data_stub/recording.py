# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The stub's "database": a recording of ``pmu.frame`` records, tiled to a
longer timeline.

The recording is ``sample_frames.ndjson`` beside this file: one ``pmu.frame``
per line, the same JSON a record line of the contract carries. It holds sixty
frames, three seconds of five stations at 20 Hz, from the Nordic 44 simulation.
They are the PMU test streamer's committed sample, converted once. The stub
keeps its own copy so it needs nothing from ``app/``. Any file of ``pmu.frame``
lines in time order serves (``REMOTE_DATA_STUB_PATH``).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from pswamp_core.messages import DataModel, PmuFrame, PmuHeader

__all__ = ["DEFAULT_PATH", "TiledRecording", "load_frames"]

#: The committed sample: sixty frames, five stations, 20 Hz.
DEFAULT_PATH = Path(__file__).parent / "sample_frames.ndjson"


def load_frames(path: Path | str = DEFAULT_PATH) -> tuple[PmuFrame, ...]:
    """The ``pmu.frame`` lines of ``path``, validated, in file order."""
    with Path(path).open(encoding="utf-8") as lines:
        frames = tuple(PmuFrame.model_validate_json(line) for line in lines if line.strip())
    if not frames:
        raise ValueError(f"{path}: no frames")
    return frames


@dataclass(frozen=True)
class TiledRecording:
    """The recording's frames repeated ``repeat`` times back to back, each
    copy shifted by the recording's span so the timeline is continuous at the
    recording's own rate. Every frame carries its layout.

    Serves one model by topic string: ``pmu.frame``. Anything else is unknown.
    """

    header: PmuHeader
    frames: tuple[PmuFrame, ...]
    repeat: int

    @classmethod
    def load(cls, path: Path | str = DEFAULT_PATH, repeat: int = 1) -> TiledRecording:
        if repeat < 1:
            raise ValueError("repeat must be at least 1")
        sample = load_frames(path)
        interval = timedelta(seconds=1.0 / sample[0].header.data_rate)
        span = sample[-1].timestamp - sample[0].timestamp + interval
        frames = tuple(
            frame.model_copy(update={"timestamp": frame.timestamp + span * copy})
            for copy in range(repeat)
            for frame in sample
        )
        return cls(header=sample[0].header, frames=frames, repeat=repeat)

    @property
    def frame_interval(self) -> timedelta:
        return timedelta(seconds=1.0 / self.header.data_rate)

    def knows(self, topic: str) -> bool:
        return topic == PmuFrame.topic

    def coverage(self, topic: str) -> tuple[datetime, datetime] | None:
        """``(start, exclusive end)`` of what is held for ``topic``; ``None`` if unknown."""
        if topic == PmuFrame.topic:
            return self.frames[0].timestamp, self.frames[-1].timestamp + self.frame_interval
        return None

    def select(
        self,
        topic: str,
        start: datetime | None,
        end: datetime | None,
        mrid: Sequence[str] | None = None,
    ) -> list[DataModel]:
        """The records of ``topic`` in ``[start, end)``, in time order."""
        records: Sequence[DataModel]
        if topic == PmuFrame.topic:
            records = self.frames
        else:
            raise KeyError(topic)
        wanted = None if mrid is None else set(mrid)
        return [
            record
            for record in records
            if (start is None or record.timestamp >= start)
            and (end is None or record.timestamp < end)
            and (wanted is None or record.mRID in wanted)
        ]
