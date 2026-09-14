# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A synthetic live PMU feed as a data provider: the recording's rows, on the wall clock.

The second **provider written outside the core**, beside ``sample_client.py``,
and the one that can only *tail*: it declares ``LIVE_CONSUME`` and nothing
else. There is no broker behind it -- the point is a source whose frames are
stamped *now*, arrive at their own pace, cannot be sought, paused or replayed,
and which a page has to render as live. It is what lets the streamer show both
modes on one screen, and what exercises the core's history-plus-live routing
without infrastructure.

What it emits: the sample recording's sixty frames, taken round-robin and
re-stamped with the current time, at the recording's own ``data_rate`` (20 Hz).
The line trip therefore comes round every three seconds, which reads
unmistakably as "not the replay". Every frame carries the recording's
``header_id``, so the header the sample client serves describes these frames
exactly, and the stats module and the frame table work on them unchanged.

**It serves no header.** A ``PmuHeader`` is a point-in-time record, and a
client that can only tail is never asked for a stretch of the past -- the
planner offers it only from the live hand-off margin on -- so a header it held
would either be clipped away or tailed for ever. A deployment that names *only*
this client gets frames without a layout and renders no table; a live source
describing its own layout is the "headers as bus events" question, still open
(STEP 4 §8). The composed default in ``api.py`` pairs it with the recording,
which provides the header.

Imports only ``pswamp_core.datagateway``, ``pswamp_core.messages`` and
``pswamp_core.util.time``, plus the sibling ``sample_client`` for the rows.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator
from pathlib import Path

from pswamp_core.datagateway import (
    Capability,
    Coverage,
    DataClient,
    EnvSetting,
    MRIDFilter,
    TimeRange,
)
from pswamp_core.messages import DataModel, PmuFrame
from pswamp_core.util.time import utcnow

from .sample_client import DEFAULT_PATH, load_sample

__all__ = ["LIVE_STREAM_ID", "LiveSyntheticClient"]

#: The stream identity every live frame carries as ``mRID``.
LIVE_STREAM_ID = "n44-live"

#: How far behind its schedule the ticker may fall before dropping time
#: rather than firing a catch-up burst -- the player's rule, in frame intervals.
_BEHIND_TOLERANCE = 1.0


class LiveSyntheticClient(DataClient):
    """``DataClient`` that tails a synthetic feed: live only, no history.

    Args:
        name: The client's name in the gateway; also its environment prefix.
        path: The recording whose rows are cycled. ``{NAME}_PATH`` from the env.
        priority: Preference against other clients covering the same instant.

    The ticker starts in ``open()`` -- which the gateway calls from
    ``Pipeline.start`` -- and stops in ``close()``. Nothing runs before a
    pipeline opens it, and nothing is built while nobody is tailing.
    """

    env_settings = (
        EnvSetting(
            "PATH",
            "The sample text file whose rows are cycled as live frames",
            default=str(DEFAULT_PATH),
            kind="path",
        ),
        EnvSetting("PRIORITY", "Preference against other clients", default="0", kind="int"),
    )

    def __init__(self, name: str = "live", path: Path | str = DEFAULT_PATH, *, priority: int = 0):
        self.name = name
        self.capabilities = Capability.LIVE_CONSUME
        self.supported_models = {PmuFrame}
        self.priority = priority
        self.recording = load_sample(Path(path))
        self._tails: list[asyncio.Queue[PmuFrame]] = []
        self._task: asyncio.Task | None = None

    @property
    def ticking(self) -> bool:
        return self._task is not None and not self._task.done()

    # -- lifecycle -------------------------------------------------------------

    async def open(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._tick(), name=f"{self.name}.tick")

    async def close(self) -> None:
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _tick(self) -> None:
        frames = self.recording.frames
        interval = self.recording.frame_interval.total_seconds()
        anchor = time.monotonic()
        index = 0
        while True:
            due = anchor + (index + 1) * interval
            delay = due - time.monotonic()
            if -delay > interval * _BEHIND_TOLERANCE:
                # Behind by more than a frame: drop time rather than burst.
                anchor = time.monotonic()
                index = 0
                continue
            await asyncio.sleep(max(delay, 0.0))
            if self._tails:
                source = frames[index % len(frames)]
                frame = PmuFrame(
                    timestamp=utcnow(),
                    mRID=LIVE_STREAM_ID,
                    header_id=source.header_id,
                    values=list(source.values),
                )
                for queue in self._tails:
                    queue.put_nowait(frame)
            index += 1

    # -- the contract ------------------------------------------------------------

    async def coverage(self, model: type[DataModel], mRID: MRIDFilter = None) -> Coverage | None:
        if not self.supports(model) or not self._wanted(mRID):
            return None
        # Now-relative, recomputed on every call: "from a frame ago, onwards".
        return Coverage(TimeRange(utcnow() - self.recording.frame_interval, None), live=True)

    async def consume(
        self,
        model: type[DataModel],
        time_range: TimeRange,
        mRID: MRIDFilter = None,
    ) -> AsyncIterator[DataModel]:
        if not issubclass(model, PmuFrame) or not self._wanted(mRID):
            return
        queue: asyncio.Queue[PmuFrame] = asyncio.Queue()
        self._tails.append(queue)
        try:
            while True:
                frame = await self._next_live(queue, time_range)
                if frame is None:
                    return
                if time_range.end is not None and frame.timestamp >= time_range.end:
                    return
                if time_range.contains(frame.timestamp):
                    yield frame
        finally:
            self._tails.remove(queue)

    @staticmethod
    async def _next_live(
        queue: asyncio.Queue[PmuFrame], time_range: TimeRange
    ) -> PmuFrame | None:
        """The next frame, giving up once the window closes."""
        if time_range.end is None:
            return await queue.get()
        remaining = (time_range.end - utcnow()).total_seconds()
        if remaining <= 0:
            return None
        try:
            return await asyncio.wait_for(queue.get(), timeout=remaining)
        except TimeoutError:
            return None

    async def produce(self, data: DataModel) -> None:
        raise TypeError(f"{self.name} is a synthetic live source and cannot store data")

    @staticmethod
    def _wanted(mRID: MRIDFilter) -> bool:
        if mRID is None:
            return True
        wanted = {mRID} if isinstance(mRID, str) else set(mRID)
        return LIVE_STREAM_ID in wanted
