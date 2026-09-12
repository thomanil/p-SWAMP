# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.
# The fixture style is Louis Pauchet's (test_pswamp/tests/conftest.py): a fixed
# anchor well in the past, so history tests never touch the live logic.

"""Shared fixtures for the data-layer tests (``pswamp.data``).

Imported by the test modules as ``from conftest import ...``; pytest puts this
directory on ``sys.path`` for them.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal

import pytest

from pswamp.data import DataModel


class Measurement(DataModel):
    """Minimal time-series payload used across the tests."""

    version: Literal["v1"] = "v1"
    value: float = 0.0


#: Fixed anchor well in the past, so history tests never touch live logic.
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def at(offset_seconds: float) -> datetime:
    """Instant ``offset_seconds`` after the fixed anchor."""
    return T0 + timedelta(seconds=offset_seconds)


def measurement(index: int, moment: datetime) -> Measurement:
    """Payload identified by ``index`` and stamped at ``moment``."""
    return Measurement(mRID=f"m{index}", value=float(index), timestamp=moment)


async def collect(stream) -> list[str]:
    """Drain a stream into the list of identifiers it produced."""
    return [payload.mRID async for payload in stream]


@pytest.fixture
def model() -> type[DataModel]:
    return Measurement
