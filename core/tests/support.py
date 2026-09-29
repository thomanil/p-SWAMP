# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Shared helpers for the core tests: a plain module, so a test imports it by
name regardless of which conftest.py pytest loaded first."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal

from pydantic import BaseModel

from pswamp_core.messages import DataModel, ResultEnvelope
from pswamp_core.util.time import UTC


class Measurement(DataModel):
    """Minimal time-series payload used across the tests."""

    version: Literal["v1"] = "v1"
    value: float = 0.0


class Number(BaseModel):
    value: float


class NumberResult(ResultEnvelope[Number]):
    """A module output, for the bus and module tests."""

    version: Literal["v1"] = "v1"


#: Fixed anchor well in the past, so history tests never touch live logic.
T0 = datetime(2026, 1, 1, tzinfo=UTC)


def at(offset_seconds: float) -> datetime:
    """Instant ``offset_seconds`` after the fixed anchor."""
    return T0 + timedelta(seconds=offset_seconds)


def measurement(index: int, moment: datetime) -> Measurement:
    """Payload identified by ``index`` and stamped at ``moment``."""
    return Measurement(mRID=f"m{index}", value=float(index), timestamp=moment)


def measurements(n: int, step_seconds: float = 1.0) -> list[Measurement]:
    return [measurement(i, at(i * step_seconds)) for i in range(n)]
