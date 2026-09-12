# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.
# Lifted from Louis Pauchet's data-gateway draft (SINTEF, 2026), see STEP3 §13.

"""Time helpers shared by the data models and the data gateway."""

from __future__ import annotations

from datetime import UTC, datetime

__all__ = ["ensure_utc", "utcnow"]


def ensure_utc(moment: datetime) -> datetime:
    """Return ``moment`` as a UTC-aware datetime, assuming UTC when naive."""
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)

    return moment.astimezone(UTC)


def utcnow() -> datetime:
    """Current UTC-aware time."""
    return datetime.now(UTC)
