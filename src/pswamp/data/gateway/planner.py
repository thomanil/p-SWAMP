# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.
# Lifted from Louis Pauchet's data-gateway draft (SINTEF, 2026), see STEP3 §13;
# only the logging changed (loguru -> stdlib).

"""Segment planning across data clients.

The planner is deliberately incremental: it resolves one segment at a time
instead of computing a full route upfront. Replaying a long history takes real
time, and during that replay a temporal database keeps ingesting, so its
coverage moves forward on its own. Re-querying coverage at every boundary lets
the database carry the stream as far as it can and defers subscribing to the
live source until the cursor is genuinely close to now, which avoids holding an
idle live subscription (or buffering it in memory) for the whole replay.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Literal

from ..time import utcnow
from .time_range import Coverage, TimeRange

if TYPE_CHECKING:
    from ..models.base import DataModel
    from .client import DataClient, MRIDFilter

__all__ = [
    "DEFAULT_LIVE_HANDOFF_MARGIN",
    "DataGapError",
    "GapPolicy",
    "Segment",
    "SegmentPlanner",
]

logger = logging.getLogger("pswamp.data.gateway.planner")

#: How close to ``now`` the cursor must be before switching to a live source.
DEFAULT_LIVE_HANDOFF_MARGIN = timedelta(seconds=5)

#: What to do when no client covers the cursor but data exists further ahead.
GapPolicy = Literal["skip", "raise"]


class DataGapError(RuntimeError):
    """Raised when no client covers part of the requested window."""


@dataclass(frozen=True, slots=True)
class Segment:
    """One contiguous stretch of a stream served by a single client."""

    client: DataClient
    range: TimeRange
    live: bool


class SegmentPlanner:
    """Chooses which client serves the stream next.

    Args:
        clients: Candidate clients, in any order.
        on_gap: ``"skip"`` jumps over uncovered stretches with a warning,
            ``"raise"`` fails with :class:`DataGapError`.
        live_handoff_margin: Distance from ``now`` under which the planner is
            willing to switch to a live-capable client.
    """

    def __init__(
        self,
        clients: list[DataClient],
        *,
        on_gap: GapPolicy = "skip",
        live_handoff_margin: timedelta = DEFAULT_LIVE_HANDOFF_MARGIN,
    ):
        self._clients = list(clients)
        self._on_gap = on_gap
        self._live_handoff_margin = live_handoff_margin

    @property
    def live_handoff_margin(self) -> timedelta:
        return self._live_handoff_margin

    async def next_segment(
        self,
        model: type[DataModel],
        cursor: datetime | None,
        request: TimeRange,
        mRID: MRIDFilter = None,
    ) -> Segment | None:
        """Resolve the segment starting at ``cursor``, or ``None`` when nothing
        more can be served. ``cursor=None`` means the earliest data any client
        holds."""
        now = utcnow()

        if request.end is not None and cursor is not None and cursor >= request.end:
            return None

        offers = await self._collect_offers(model, request, mRID)
        if not offers:
            return None

        if cursor is None:
            cursor = self._earliest_start(offers)

        covering = [offer for offer in offers if offer[2].contains(cursor)]

        if not covering:
            cursor = self._skip_gap(offers, cursor, model)
            if cursor is None:
                return None

            covering = [offer for offer in offers if offer[2].contains(cursor)]
            if not covering:
                return None

        client, coverage, window = max(covering, key=lambda offer: offer[0].priority)

        if self._should_go_live(client, coverage, cursor, request, now):
            return Segment(client=client, range=TimeRange(cursor, request.end), live=True)

        end = self._segment_end(offers, client, window, cursor)

        if end is not None and end <= cursor:
            return None

        return Segment(client=client, range=TimeRange(cursor, end), live=False)

    async def _collect_offers(
        self,
        model: type[DataModel],
        request: TimeRange,
        mRID: MRIDFilter,
    ) -> list[tuple[DataClient, Coverage, TimeRange]]:
        """Fetch every eligible client's coverage, clipped to the request."""
        candidates = [client for client in self._clients if client.supports(model)]
        if not candidates:
            return []

        results = await asyncio.gather(
            *(client.coverage(model, mRID) for client in candidates),
            return_exceptions=True,
        )

        offers: list[tuple[DataClient, Coverage, TimeRange]] = []

        for client, result in zip(candidates, results, strict=True):
            if isinstance(result, BaseException):
                logger.error(
                    "client %s failed to report coverage for %s: %s",
                    client.name,
                    model.__name__,
                    result,
                )
                continue

            if result is None:
                continue

            window = result.range.intersect(request)
            if window is None or window.is_empty:
                continue

            offers.append((client, result, window))

        return offers

    @staticmethod
    def _earliest_start(
        offers: list[tuple[DataClient, Coverage, TimeRange]],
    ) -> datetime | None:
        starts = [window.start for _, _, window in offers]

        if any(start is None for start in starts):
            return None

        return min(starts)

    def _skip_gap(
        self,
        offers: list[tuple[DataClient, Coverage, TimeRange]],
        cursor: datetime,
        model: type[DataModel],
    ) -> datetime | None:
        """Advance the cursor to the next covered instant, honouring the policy."""
        upcoming = [
            window.start
            for _, _, window in offers
            if window.start is not None and window.start > cursor
        ]

        if not upcoming:
            return None

        resume = min(upcoming)

        if self._on_gap == "raise":
            raise DataGapError(
                f"No client covers {model.__name__} between "
                f"{cursor.isoformat()} and {resume.isoformat()}"
            )

        logger.warning(
            "gap in %s coverage between %s and %s; skipping ahead",
            model.__name__,
            cursor.isoformat(),
            resume.isoformat(),
        )

        return resume

    def _should_go_live(
        self,
        client: DataClient,
        coverage: Coverage,
        cursor: datetime,
        request: TimeRange,
        now: datetime,
    ) -> bool:
        if not coverage.live:
            return False

        if request.end is not None and request.end <= now:
            return False

        return cursor >= now - self._live_handoff_margin

    @staticmethod
    def _segment_end(
        offers: list[tuple[DataClient, Coverage, TimeRange]],
        chosen: DataClient,
        window: TimeRange,
        cursor: datetime,
    ) -> datetime | None:
        """Cut the segment where a preferred client takes over, if any does."""
        end = window.end

        for client, _, other in offers:
            if client is chosen or client.priority <= chosen.priority:
                continue

            if other.start is None or other.start <= cursor:
                continue

            end = other.start if end is None else min(end, other.start)

        return end
