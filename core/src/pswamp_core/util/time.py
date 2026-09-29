# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The one time helper every layer shares.

Every timestamp in the core is UTC-aware. A naive datetime is *taken* as UTC
rather than rejected; an aware one is converted.
"""

from datetime import datetime, timezone

__all__ = ["UTC", "ensure_utc", "utcnow"]

UTC = timezone.utc  # datetime.UTC is 3.12; the image runs 3.11


def ensure_utc(moment: datetime) -> datetime:
    """Return ``moment`` as a UTC-aware datetime. Naive input is taken as UTC."""
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def utcnow() -> datetime:
    """The current instant, UTC-aware."""
    return datetime.now(UTC)
