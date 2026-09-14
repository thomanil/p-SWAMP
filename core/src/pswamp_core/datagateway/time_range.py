# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""Time interval primitives used to describe requested windows and client coverage.

Lifted verbatim from the test_pswamp draft (``core/datagateway/time_range.py``);
only the import path changed.

``TimeRange`` models a half-open interval ``[start, end)``. A ``None`` bound is
unbounded: ``start=None`` reaches infinitely into the past, ``end=None`` stays
open towards the future.

``Coverage`` is what a :class:`~pswamp_core.datagateway.data_client_model.DataClient`
reports about the data it currently holds. The range always uses concrete
bounds, and the ``live`` flag states whether the client can keep yielding beyond
that end as new data arrives.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ..util.time import ensure_utc

__all__ = ["Coverage", "TimeRange"]


def _latest_start(first: datetime | None, second: datetime | None) -> datetime | None:
    """Later of two start bounds, where ``None`` means unbounded past."""
    if first is None:
        return second
    if second is None:
        return first

    return max(first, second)


def _earliest_end(first: datetime | None, second: datetime | None) -> datetime | None:
    """Earlier of two end bounds, where ``None`` means unbounded future."""
    if first is None:
        return second
    if second is None:
        return first

    return min(first, second)


@dataclass(frozen=True, slots=True)
class TimeRange:
    """
    Half-open time interval ``[start, end)``.

    Args:
        start: Inclusive lower bound, or ``None`` for unbounded past.
        end: Exclusive upper bound, or ``None`` for an open, live-following range.

    Examples:
        >>> from datetime import datetime, timezone
        >>> window = TimeRange(datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 2, 1, tzinfo=timezone.utc))
        >>> window.contains(datetime(2026, 1, 15, tzinfo=timezone.utc))
        True
    """

    start: datetime | None = None
    end: datetime | None = None

    def __post_init__(self) -> None:
        if self.start is not None:
            object.__setattr__(self, "start", ensure_utc(self.start))

        if self.end is not None:
            object.__setattr__(self, "end", ensure_utc(self.end))

        if self.start is not None and self.end is not None and self.end < self.start:
            raise ValueError(
                f"TimeRange end {self.end.isoformat()} precedes start {self.start.isoformat()}"
            )

    @property
    def is_open(self) -> bool:
        """Whether the range has no upper bound."""
        return self.end is None

    @property
    def is_empty(self) -> bool:
        """Whether the range cannot contain any moment."""
        return self.start is not None and self.end is not None and self.start == self.end

    def contains(self, moment: datetime | None) -> bool:
        """
        Test whether ``moment`` falls inside the half-open interval.

        Args:
            moment: Instant to test. ``None`` represents the unbounded past and
                is only contained by a range with an unbounded start.

        Returns:
            ``True`` when the moment lies within ``[start, end)``.
        """
        if moment is None:
            return self.start is None

        moment = ensure_utc(moment)

        if self.start is not None and moment < self.start:
            return False

        return self.end is None or moment < self.end

    def intersect(self, other: TimeRange) -> TimeRange | None:
        """
        Overlapping portion of two ranges.

        Args:
            other: Range to intersect with.

        Returns:
            The shared interval, or ``None`` when the ranges do not overlap.
        """
        start = _latest_start(self.start, other.start)
        end = _earliest_end(self.end, other.end)

        if start is not None and end is not None and end <= start:
            return None

        return TimeRange(start, end)

    def overlaps(self, other: TimeRange) -> bool:
        """Whether the two ranges share at least one instant."""
        return self.intersect(other) is not None

    def resolve(self, now: datetime) -> TimeRange:
        """Materialise an open end at ``now``, leaving bounded ranges untouched."""
        if self.end is not None:
            return self

        return TimeRange(self.start, ensure_utc(now))


@dataclass(frozen=True, slots=True)
class Coverage:
    """
    Data a client currently holds for a given model.

    Args:
        range: Concrete interval backed by stored data.
        live: Whether the client can keep emitting past ``range.end`` as new
            data arrives. Kafka-like sources set this; a static file does not.
    """

    range: TimeRange
    live: bool = False
