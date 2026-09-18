# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A module consumes one class off the bus and publishes its result envelope."""

from __future__ import annotations

import asyncio
import contextlib

from support import Measurement, Number, NumberResult, at, measurement, take

from pswamp_core.bus import InProcessBus
from pswamp_core.messages import AppStatus, Command, ErrorEvent, ResultEnvelope
from pswamp_core.modules import Module


class Doubler(Module):
    name = "doubler"
    input_model = Measurement
    output_model = NumberResult

    async def process(self, message: Measurement) -> Number | None:
        if message.value < 0:
            return None
        if message.value > 100:
            raise ValueError("too big")
        return Number(value=message.value * 2)


async def test_module_publishes_an_envelope_per_input():
    bus = InProcessBus()
    module = Doubler()
    assert module.status is AppStatus.INITIALIZING
    task = asyncio.create_task(module.run(bus))
    await asyncio.sleep(0)
    try:
        with bus.subscribe(ResultEnvelope) as results:
            bus.publish(measurement(3, at(3)))
            bus.publish(Measurement(value=-1, mRID="skip", timestamp=at(4)))
            bus.publish(Measurement(value=1000, mRID="boom", timestamp=at(5)))
            bus.publish(measurement(4, at(6)))
            first, second = await take(results, 2)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert isinstance(first, NumberResult)
    assert first.result.value == 6.0
    assert first.timestamp == at(3)
    assert first.app.name == "doubler"
    assert second.result.value == 8.0
    # The failure on "boom" set UNDEFINED; the next success restored OK.
    assert module.status is AppStatus.OK
    assert module.last_result is second
    assert NumberResult.topic == "number.result"


class Refuser(Module):
    """A module addressed by commands, whose process always fails."""

    name = "refuser"
    input_model = Command
    output_model = NumberResult

    async def process(self, message: Command) -> Number | None:
        raise RuntimeError("cannot " + message.verb)


async def test_a_failing_process_publishes_an_error_event_with_the_request_id():
    bus = InProcessBus()
    module = Refuser()
    task = asyncio.create_task(module.run(bus))
    await asyncio.sleep(0)
    try:
        with bus.subscribe(ErrorEvent) as errors:
            command = Command(target="refuser", verb="frobnicate")
            bus.publish(command)
            (event,) = await take(errors, 1)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    assert event.source == "refuser"
    assert event.request_id == command.request_id
    assert event.detail == "RuntimeError: cannot frobnicate"
    assert module.status is AppStatus.UNDEFINED
