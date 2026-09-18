# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""
Single async stream stitched from one or more data clients.

Lifted from the test_pswamp draft (``core/datagateway/stream.py``). Adapted:
``typing.Self`` (3.12) became string annotations; loguru became ``logging``.

``DataStream`` walks a cursor through the requested window, asking the planner for
one segment at a time and draining that client before re-planning. Exactly one
client iterator is open at any moment, so a live source is only subscribed to
once the replay has caught up with it.

The watermark is why "seek" and "loop" are always a *new* stream: a stream never
yields anything older than what it has already produced, which is exactly right
across overlapping sources and exactly wrong for going backwards.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import TYPE_CHECKING

from ..log import get_logger
from ..util.time import utcnow

if TYPE_CHECKING:
    from types import TracebackType

    from ..messages.data_model import DataModel
    from .data_client_model import MRIDFilter
    from .planner import Segment, SegmentPlanner
    from .time_range import TimeRange

__all__ = ["DataStream"]

logger = get_logger("pswamp_core.datagateway.stream")

#: Consecutive segments without catching up on ``now`` before warning.
_LAG_WARNING_THRESHOLD = 5


class DataStream:
    """
    Async iterator over a model's payloads across a time window.

    Args:
        planner: Planner resolving which client serves each segment.
        model: Model class being streamed.
        request: Window requested by the caller. An open end tails forever.
        mRID: Optional identifier filter.

    Examples:
        >>> async with gateway.consume(PmuFrame, start=yesterday) as stream:
        ...     async for frame in stream:
        ...         handle(frame)
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
    def request(self) -> TimeRange:
        """The window this stream was opened over."""
        return self._request

    def __aiter__(self) -> "DataStream":
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

    async def __aenter__(self) -> "DataStream":
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
        emitted_at_watermark: set[str | None] = set()
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

                    if watermark is not None:
                        if moment < watermark:
                            continue

                        if moment == watermark and payload.mRID in emitted_at_watermark:
                            continue

                    if moment != watermark:
                        watermark = moment
                        emitted_at_watermark = set()

                    emitted_at_watermark.add(payload.mRID)
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
    """Close a client iterator when it supports it."""
    closer = getattr(iterator, "aclose", None)

    if closer is None:
        return

    await closer()


def _max_bound(first: datetime | None, second: datetime | None) -> datetime | None:
    """Later of two instants, ignoring ``None``."""
    if first is None:
        return second

    if second is None:
        return first

    return max(first, second)
