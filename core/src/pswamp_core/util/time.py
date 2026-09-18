# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""The one time helper every layer shares.

Lifted from the test_pswamp draft (``utils/time.py``). Adapted: ``datetime.UTC``
is Python 3.12; this package runs on 3.11, so it is ``timezone.utc``.

Every timestamp in the core is UTC-aware. ``ensure_utc`` is what makes the
half-open interval arithmetic in ``datagateway.time_range`` safe: a naive
datetime is *taken* as UTC rather than rejected, and an aware one is converted.
"""

from datetime import datetime, timezone

__all__ = ["UTC", "ensure_utc", "utcnow"]

UTC = timezone.utc


def ensure_utc(moment: datetime) -> datetime:
    """Return ``moment`` as a UTC-aware datetime. Naive input is taken as UTC."""
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def utcnow() -> datetime:
    """The current instant, UTC-aware."""
    return datetime.now(UTC)
