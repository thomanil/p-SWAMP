# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The stub's "database": the sample recording, tiled to a longer timeline."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from pmu_test_streamer.sample_client import DEFAULT_PATH, load_sample
from pswamp_core.messages import DataModel, PmuFrame, PmuHeader

__all__ = ["TiledRecording"]


@dataclass(frozen=True)
class TiledRecording:
    """The sample file's frames repeated ``repeat`` times back to back, each
    copy shifted by the recording's span so the timeline is continuous at the
    recording's own rate. Every frame carries the recording's layout.

    Serves one model by topic string: ``pmu.frame``. Anything else is unknown.
    """

    header: PmuHeader
    frames: tuple[PmuFrame, ...]
    repeat: int

    @classmethod
    def load(cls, path: Path | str = DEFAULT_PATH, repeat: int = 1) -> TiledRecording:
        if repeat < 1:
            raise ValueError("repeat must be at least 1")
        sample = load_sample(Path(path))
        span = sample.coverage.end - sample.coverage.start
        frames = tuple(
            frame.model_copy(update={"timestamp": frame.timestamp + span * copy})
            for copy in range(repeat)
            for frame in sample.frames
        )
        return cls(header=sample.header, frames=frames, repeat=repeat)

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
