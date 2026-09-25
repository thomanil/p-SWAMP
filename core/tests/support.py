# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Shared helpers for the core tests (a plain module, so tests can import it by
name regardless of which conftest.py pytest loaded first).

``Measurement``, ``T0``, ``at``, ``measurement`` and ``collect`` are the
test_pswamp draft's ``tests/conftest.py`` helpers, lifted.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Literal

from pydantic import BaseModel

from pswamp_core.bus import Subscription
from pswamp_core.command_routing import CommandRefused
from pswamp_core.datagateway import Capability, EnvSetting
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.messages import Command, DataModel, ResultEnvelope
from pswamp_core.modules import Module
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


class HalveCommand(Command):
    """Answer with half of ``value``: the command the ``Halver`` takes."""

    value: float


class Halver(Module):
    """A module that reads nothing off the bus and answers a command: refuses a
    negative value when checked, fails on zero when applied."""

    name = "halver"
    input_model = None
    output_model = NumberResult
    commands = (HalveCommand,)

    def validate(self, command: HalveCommand) -> None:
        if command.value < 0:
            raise CommandRefused("no negatives")

    async def handle(self, command: HalveCommand) -> Number:
        if command.value == 0:
            raise RuntimeError("cannot halve zero")
        return Number(value=command.value / 2)


#: Fixed anchor well in the past, so history tests never touch live logic.
T0 = datetime(2026, 1, 1, tzinfo=UTC)

HISTORY = Capability.HISTORY_CONSUME | Capability.PRODUCE
LIVE = Capability.LIVE_CONSUME | Capability.HISTORY_CONSUME


def at(offset_seconds: float) -> datetime:
    """Instant ``offset_seconds`` after the fixed anchor."""
    return T0 + timedelta(seconds=offset_seconds)


def measurement(index: int, moment: datetime) -> Measurement:
    """Payload identified by ``index`` and stamped at ``moment``."""
    return Measurement(mRID=f"m{index}", value=float(index), timestamp=moment)


def measurements(n: int, step_seconds: float = 1.0) -> list[Measurement]:
    return [measurement(i, at(i * step_seconds)) for i in range(n)]


async def collect(stream) -> list[str]:
    """Drain a stream into the list of identifiers it produced."""
    return [payload.mRID async for payload in stream]


async def take(subscription: Subscription, n: int, timeout: float = 2.0) -> list[DataModel]:
    """The next ``n`` messages off a subscription, or fail after ``timeout``."""

    async def _take() -> list[DataModel]:
        out = []
        while len(out) < n:
            out.append(await subscription.get())
        return out

    return await asyncio.wait_for(_take(), timeout)


class EnvTestClient(InMemoryClient):
    """An in-memory client configurable from the environment, for the config and
    ``gateway_from_env`` tests. Holds three measurements."""

    env_settings = (
        EnvSetting("LABEL", "A label the test asserts on", required=True),
        EnvSetting("PRIORITY", "Preference against other clients", default="0", kind="int"),
        EnvSetting(
            "CAPABILITIES",
            "Comma-separated capability names",
            default="HISTORY_CONSUME,PRODUCE",
            kind="capabilities",
        ),
        EnvSetting("COUNT", "How many records to hold", default="3", kind="int"),
    )

    def __init__(
        self,
        name: str,
        *,
        label: str,
        priority: int = 0,
        capabilities: Capability = HISTORY,
        count: int = 3,
    ) -> None:
        super().__init__(
            name,
            Measurement,
            measurements(count),
            priority=priority,
            capabilities=capabilities,
        )
        self.label = label
