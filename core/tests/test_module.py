# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A module consumes one class off the bus and publishes its result envelope."""

from __future__ import annotations

import asyncio
import contextlib

from support import Halver, HalveCommand, Measurement, Number, NumberResult, at, measurement, take

from pswamp_core.bus import InProcessBus
from pswamp_core.messages import AppStatus, ErrorEvent, ResultEnvelope
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


async def test_a_command_is_answered_in_the_output_model_with_its_request_id():
    bus = InProcessBus()
    module = Halver()
    inbox = module.command_inbox(bus)
    inbox.start()
    try:
        with bus.subscribe(NumberResult) as results:
            command = HalveCommand(value=8)
            bus.publish(command)
            (result,) = await take(results, 1)
    finally:
        await inbox.stop()
    assert result.result.value == 4
    assert result.request_id == command.request_id
    assert result.app == module.identity
    assert module.status is AppStatus.OK and module.last_result is result
    await module.run(bus)  # no input_model: nothing to read, returns at once


async def test_a_refused_or_failing_command_publishes_an_error_event_with_the_request_id():
    bus = InProcessBus()
    module = Halver()
    inbox = module.command_inbox(bus)
    inbox.start()
    try:
        with bus.subscribe(ErrorEvent) as errors, bus.subscribe(NumberResult) as results:
            refused, failing = HalveCommand(value=-1), HalveCommand(value=0)
            bus.publish(refused)
            bus.publish(failing)
            first, second = await take(errors, 2)
            assert results.get_nowait() is None
    finally:
        await inbox.stop()
    assert (first.source, first.request_id, first.detail) == ("halver", refused.request_id, "no negatives")
    assert first.message == "halver refused halve"
    assert (second.request_id, second.detail) == (failing.request_id, "RuntimeError: cannot halve zero")
