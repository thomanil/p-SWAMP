# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.
# Lifted from Louis Pauchet's data-gateway draft (SINTEF, 2026), see STEP3 §13.
# Adapted: the de-duplication key includes the model class (STEP2 §6.6), and
# logging is stdlib.

"""Single async stream stitched from one or more data clients.

`DataStream` walks a cursor through the requested window, asking the planner for
one segment at a time and draining that client before re-planning. Exactly one
client iterator is open at any moment, so a live source is only subscribed to
once the replay has caught up with it.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import datetime
from typing import TYPE_CHECKING, Self

from ..time import utcnow

if TYPE_CHECKING:
    from types import TracebackType

    from ..models.base import DataModel
    from .client import MRIDFilter
    from .planner import Segment, SegmentPlanner
    from .time_range import TimeRange

__all__ = ["DataStream"]

logger = logging.getLogger("pswamp.data.gateway.stream")

#: Consecutive segments without catching up on ``now`` before warning.
_LAG_WARNING_THRESHOLD = 5


class DataStream:
    """Async iterator over a model's payloads across a time window.

    Iterating the same stream twice continues where it was; it does not restart.
    """

    def __init__(
        self,
        planner: SegmentPlanner,
        model: type[DataModel],
        request: TimeRange,
        mRID: MRIDFilter = None,
    ):
        self._planner = planner
        self._model = model
        self._request = request
        self._mRID = mRID

        self._generator: AsyncIterator[DataModel] | None = None

        #: Segments taken so far, in order. Useful for debugging routing.
        self.segments: list[Segment] = []

    @property
    def model(self) -> type[DataModel]:
        return self._model

    @property
    def request(self) -> TimeRange:
        return self._request

    def __aiter__(self) -> Self:
        if self._generator is None:
            self._generator = self._iterate()

        return self

    async def __anext__(self) -> DataModel:
        if self._generator is None:
            self._generator = self._iterate()

        return await self._generator.__anext__()

    async def aclose(self) -> None:
        """Stop the stream and release the active client iterator."""
        if self._generator is None:
            return

        generator, self._generator = self._generator, None
        await generator.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def _iterate(self) -> AsyncIterator[DataModel]:
        """Drive the plan-consume-replan loop."""
        cursor = self._request.start
        watermark: datetime | None = None
        # Widened from the draft's ``mRID`` alone: two different models at the
        # same instant must never collapse into one (STEP2 §6.6).
        emitted_at_watermark: set[tuple[type, str | None]] = set()
        stalled = 0

        while True:
            segment = await self._planner.next_segment(
                self._model, cursor, self._request, self._mRID
            )

            if segment is None:
                return

            self.segments.append(segment)

            logger.debug(
                "streaming %s from %s over [%s, %s) live=%s",
                self._model.__name__,
                segment.client.name,
                segment.range.start,
                segment.range.end,
                segment.live,
            )

            iterator = segment.client.consume(self._model, segment.range, self._mRID)
            produced = False

            try:
                async for payload in iterator:
                    moment = payload.timestamp

                    if moment is None:
                        logger.warning(
                            "dropping %s payload without timestamp from %s",
                            self._model.__name__,
                            segment.client.name,
                        )
                        continue

                    if self._request.end is not None and moment >= self._request.end:
                        return

                    key = (type(payload), payload.mRID)

                    if watermark is not None:
                        if moment < watermark:
                            continue

                        if moment == watermark and key in emitted_at_watermark:
                            continue

                    if moment != watermark:
                        watermark = moment
                        emitted_at_watermark = set()

                    emitted_at_watermark.add(key)
                    produced = True

                    yield payload
            finally:
                await _aclose(iterator)

            if segment.live and segment.range.end is None:
                return

            next_cursor = _max_bound(watermark, segment.range.end)

            if next_cursor is None:
                return

            if cursor is not None and next_cursor <= cursor and not produced:
                return

            stalled = self._track_lag(next_cursor, stalled)
            cursor = next_cursor

    def _track_lag(self, cursor: datetime, stalled: int) -> int:
        """Warn when replay is not gaining on real time, which never converges."""
        if self._request.end is not None:
            return 0

        if cursor >= utcnow() - self._planner.live_handoff_margin:
            return 0

        stalled += 1

        if stalled == _LAG_WARNING_THRESHOLD:
            logger.warning(
                "%s replay is still %s behind real time after %s segments; "
                "the stream may never reach live data",
                self._model.__name__,
                utcnow() - cursor,
                stalled,
            )

        return stalled


async def _aclose(iterator: AsyncIterator[DataModel]) -> None:
    closer = getattr(iterator, "aclose", None)

    if closer is None:
        return

    await closer()


def _max_bound(first: datetime | None, second: datetime | None) -> datetime | None:
    if first is None:
        return second

    if second is None:
        return first

    return max(first, second)
