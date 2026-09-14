# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""
Segment planning across data clients.

Lifted from the test_pswamp draft (``core/datagateway/planner.py``); only the
import paths and the logger changed.

The planner is deliberately incremental: it resolves one segment at a time
instead of computing a full route upfront. Replaying a long history takes real
time, and during that replay a temporal database keeps ingesting, so its
coverage moves forward on its own. Re-querying coverage at every boundary lets
the database carry the stream as far as it can and defers subscribing to the
live source until the cursor is genuinely close to now, which avoids holding an
idle live subscription (or buffering it in memory) for the whole replay.

Adapted in p-SWAMP: the planner honours **declared capabilities** (the promise
in ``DataClient``'s docstring that the core never asks for what was not
declared). A history segment goes only to a client with ``HISTORY_CONSUME``;
the live hand-off goes only to one with ``LIVE_CONSUME``; a client with
neither contributes no offer. A *live-only* client is offered only from
``now - live_handoff_margin`` onwards, since that is the only stretch it can
be asked for -- a cursor before that sits in a gap, and skipping the gap lands
exactly where the hand-off is allowed.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Literal

from ..log import get_logger
from ..util.time import utcnow
from .data_client_model import Capability, can_consume
from .time_range import Coverage, TimeRange

if TYPE_CHECKING:
    from ..messages.data_model import DataModel
    from .data_client_model import DataClient, MRIDFilter

__all__ = [
    "DEFAULT_LIVE_HANDOFF_MARGIN",
    "DataGapError",
    "GapPolicy",
    "Segment",
    "SegmentPlanner",
]

logger = get_logger("pswamp_core.datagateway.planner")

#: How close to ``now`` the cursor must be before switching to a live source.
DEFAULT_LIVE_HANDOFF_MARGIN = timedelta(seconds=5)

#: What to do when no client covers the cursor but data exists further ahead.
GapPolicy = Literal["skip", "raise"]


class DataGapError(RuntimeError):
    """Raised when no client covers part of the requested window."""


@dataclass(frozen=True, slots=True)
class Segment:
    """
    One contiguous stretch of a stream served by a single client.

    Attributes:
        client: Client that will serve the segment.
        range: Window to request from that client.
        live: Whether this segment follows live data instead of replaying stored
            records.
    """

    client: DataClient
    range: TimeRange
    live: bool


class SegmentPlanner:
    """
    Chooses which client serves the stream next.

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
        """Distance from ``now`` under which a live source is picked up."""
        return self._live_handoff_margin

    async def next_segment(
        self,
        model: type[DataModel],
        cursor: datetime | None,
        request: TimeRange,
        mRID: MRIDFilter = None,
    ) -> Segment | None:
        """
        Resolve the segment starting at ``cursor``.

        Args:
            model: Model class being streamed.
            cursor: Where the stream currently stands. ``None`` means start from
                the earliest data any client holds.
            request: Full window originally requested.
            mRID: Optional identifier filter.

        Returns:
            The next segment, or ``None`` when nothing more can be served.

        Raises:
            DataGapError: When ``on_gap="raise"`` and the cursor sits in a gap.
        """
        now = utcnow()

        if request.end is not None and cursor is not None and cursor >= request.end:
            return None

        offers = await self._collect_offers(model, request, mRID, now)
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

        # An offer is eligible for a *live* segment if the hand-off applies to
        # it, and for a *history* segment if the client may be asked for one.
        # A live-only client whose window covers the cursor but which cannot go
        # live yet is neither, and drops out here.
        eligible: list[tuple[DataClient, Coverage, TimeRange, bool]] = []
        for client, coverage, window in covering:
            go_live = self._should_go_live(client, model, coverage, cursor, request, now)
            if go_live or client.supports(model, Capability.HISTORY_CONSUME):
                eligible.append((client, coverage, window, go_live))
        if not eligible:
            return None

        client, coverage, window, go_live = max(eligible, key=lambda offer: offer[0].priority)

        if go_live:
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
        now: datetime,
    ) -> list[tuple[DataClient, Coverage, TimeRange]]:
        """Fetch every consuming client's coverage, clipped to the request.

        A client that can only tail (``LIVE_CONSUME`` without
        ``HISTORY_CONSUME``) is offered only for the stretch it can actually be
        asked for: from ``now - live_handoff_margin`` on, and only if it reports
        itself live.
        """
        candidates = [client for client in self._clients if can_consume(client, model)]
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

            if not client.supports(model, Capability.HISTORY_CONSUME):
                if not result.live:
                    continue
                window = window.intersect(TimeRange(now - self._live_handoff_margin, None))
                if window is None or window.is_empty:
                    continue

            offers.append((client, result, window))

        return offers

    @staticmethod
    def _earliest_start(
        offers: list[tuple[DataClient, Coverage, TimeRange]],
    ) -> datetime | None:
        """Earliest instant any client can serve, or ``None`` if unbounded."""
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
        model: type[DataModel],
        coverage: Coverage,
        cursor: datetime,
        request: TimeRange,
        now: datetime,
    ) -> bool:
        """Whether the cursor has caught up enough to follow live data on
        ``client`` -- which must have declared that it can."""
        if not client.supports(model, Capability.LIVE_CONSUME):
            return False

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
