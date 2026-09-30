# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A module reads one class off its input queue and publishes its result envelope
into its ``out`` sink; its commands come through its inbox."""

from __future__ import annotations

import asyncio
import contextlib

from support import Halver, HalveCommand, Measurement, Number, NumberResult, Tap, at, measurement, take

from pswamp_core.messages import AppStatus, ErrorEvent
from pswamp_core.modules import Module
from pswamp_core.subscription import Overflow


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
    feed, out = Tap(), Tap()
    module = Doubler()
    assert module.status is AppStatus.INITIALIZING
    inputs = feed.subscribe(Measurement, overflow=module.overflow, maxsize=module.maxsize)
    task = asyncio.create_task(module.run(inputs, out))
    try:
        with out.subscribe(NumberResult) as results, out.subscribe(ErrorEvent) as errors:
            feed.publish(measurement(3, at(3)))
            feed.publish(Measurement(value=-1, mRID="skip", timestamp=at(4)))
            feed.publish(Measurement(value=1000, mRID="boom", timestamp=at(5)))
            feed.publish(measurement(4, at(6)))
            first, second = await take(results, 2)
            (failure,) = await take(errors, 1)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert isinstance(first, NumberResult)
    assert first.result.value == 6.0
    assert first.timestamp == at(3)
    assert first.app.name == "doubler"
    assert second.result.value == 8.0
    assert failure.source == "doubler" and failure.detail == "ValueError: too big"
    # The failure on "boom" set UNDEFINED; the next success restored OK.
    assert module.status is AppStatus.OK
    assert module.last_result is second
    assert NumberResult.topic == "number.result"


async def test_a_command_is_answered_in_the_output_model_with_its_request_id():
    commands, out = Tap(), Tap()
    module = Halver()
    inbox = module.command_inbox(commands.subscribe(HalveCommand, overflow=Overflow.GROW), out)
    inbox.start()
    try:
        with out.subscribe(NumberResult) as results:
            command = HalveCommand(value=8)
            commands.publish(command)
            (result,) = await take(results, 1)
    finally:
        await inbox.stop()
    assert result.result.value == 4
    assert result.request_id == command.request_id
    assert result.app == module.identity
    assert module.status is AppStatus.OK and module.last_result is result
    await module.run(Tap().subscribe(Measurement), out)  # no input_model: returns at once


async def test_a_refused_or_failing_command_publishes_an_error_event_with_the_request_id():
    commands, out = Tap(), Tap()
    module = Halver()
    inbox = module.command_inbox(commands.subscribe(HalveCommand, overflow=Overflow.GROW), out)
    inbox.start()
    try:
        with out.subscribe(ErrorEvent) as errors, out.subscribe(NumberResult) as results:
            refused, failing = HalveCommand(value=-1), HalveCommand(value=0)
            commands.publish(refused)
            commands.publish(failing)
            first, second = await take(errors, 2)
            assert results.get_nowait() is None
    finally:
        await inbox.stop()
    assert (first.source, first.request_id, first.detail) == ("halver", refused.request_id, "no negatives")
    assert first.message == "halver refused halve"
    assert (second.request_id, second.detail) == (failing.request_id, "RuntimeError: cannot halve zero")
