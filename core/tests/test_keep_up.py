# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A module that falls behind its input says so, and so does the outbox in
front of the transport.

``Module.run`` watches its own input queue (``KeepUpMonitor``): drops, and the
age of input since the transport sent it. Past its ``KeepUp`` policy it
publishes an ``ErrorEvent`` into its ``out`` -- which its host puts on the
app's error topic, for the tray.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import ClassVar

from support import Measurement, Number, NumberResult, Tap, at, measurement, take

from pswamp_core.messages import ErrorEvent, stamp_sent_at
from pswamp_core.modules import KeepUp, Module
from pswamp_core.subscription import Overflow
from pswamp_core.transport import InMemoryTransport, Outbox


class Slow(Module):
    """Takes ``delay`` seconds per message, over a small queue."""

    name = "slow"
    input_model = Measurement
    output_model = NumberResult
    maxsize: ClassVar[int] = 4
    keep_up: ClassVar[KeepUp | None] = KeepUp(max_input_age_s=1.0, report_every_s=0.1)
    delay: ClassVar[float] = 0.01

    async def process(self, message: Measurement) -> Number:
        await asyncio.sleep(self.delay)
        return Number(value=message.value)


class Quiet(Slow):
    name = "quiet"
    keep_up = None


class Failing(Module):
    name = "failing"
    input_model = Measurement
    output_model = NumberResult

    async def process(self, message: Measurement) -> Number:
        raise ValueError("boom")


@contextlib.asynccontextmanager
async def running(module: Module):
    """The module running over a feed (what the tests publish into) and an
    ``out`` (what they read the reports off): the pair of taps."""
    feed, out = Tap(), Tap()
    inputs = feed.subscribe(module.input_model, overflow=module.overflow, maxsize=module.maxsize)
    task = asyncio.create_task(module.run(inputs, out))
    try:
        yield feed, out
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def burst(feed: Tap, n: int, start: int = 0) -> None:
    for i in range(start, start + n):
        feed.publish(measurement(i, at(i)))


async def test_a_module_that_drops_input_reports_it_and_then_that_it_caught_up():
    module = Slow()
    async with running(module) as (feed, out):
        with out.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            burst(feed, 40)  # a queue of 4: most of these are dropped
            (behind,) = await take(errors, 1)
            assert behind.source == "slow"
            assert "is not keeping up with measurement" in behind.message
            assert "input dropped" in (behind.detail or "")
            assert module.monitor.behind and module.monitor.input_dropped > 0

            # Then a trickle it can handle: a whole quiet interval means caught up.
            for i in range(40, 60):
                feed.publish(measurement(i, at(i)))
                await asyncio.sleep(0.02)
            reports = [e.message for e in await take(errors, 1)]
            assert any("caught up" in m for m in reports)
    assert not module.monitor.behind


async def test_input_read_too_long_after_it_was_sent_counts_as_behind():
    module = Slow()
    async with running(module) as (feed, out):
        with out.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            late = measurement(1, at(1))
            stamp_sent_at(late, time.time() - 10)
            feed.publish(late)
            (behind,) = await take(errors, 1)
    assert "is not keeping up" in behind.message
    assert "10." in (behind.detail or "")  # "oldest input read 10.0 s after it was sent"
    assert module.monitor.input_age_s is not None and module.monitor.input_age_s >= 10


async def test_while_behind_it_reports_at_most_once_per_interval():
    module = Slow()
    async with running(module) as (feed, out):
        with out.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            started = time.monotonic()
            for n in range(12):  # keep it behind for ~0.3 s
                burst(feed, 20, start=n * 20)
                await asyncio.sleep(0.025)
            elapsed = time.monotonic() - started
            await asyncio.sleep(0.05)
            reports = []
            while (error := errors.get_nowait()) is not None:
                reports.append(error)
    # One on falling behind, then at most one per 0.1 s interval.
    assert 1 <= len(reports) <= 2 + elapsed / 0.1


async def test_no_policy_means_readings_but_no_reports():
    module = Quiet()
    async with running(module) as (feed, out):
        with out.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            burst(feed, 40)
            await asyncio.sleep(0.2)
            assert errors.get_nowait() is None
    assert module.monitor.input_dropped > 0 and module.monitor.reports == 0


# --- the outbox in front of the transport ------------------------------------------


async def test_an_outbox_that_cannot_publish_fast_enough_drops_data_never_control_and_says_so():
    class SlowBroker(InMemoryTransport):
        async def publish(self, message, *, app, key):
            await asyncio.sleep(0.01)
            await super().publish(message, app=app, key=key)

    broker = SlowBroker()
    outbox = Outbox(
        broker, app="t", key="k1", maxsize=4,
        keep_up=KeepUp(max_input_age_s=1.0, report_every_s=0.1), source="slow",
        label="the server-side publisher for slow",
    )
    with broker.subscribe(Measurement, ErrorEvent, app="t", overflow=Overflow.GROW) as sent:
        outbox.start()
        burst(outbox, 40)  # an Outbox is a sink like any other
        error = ErrorEvent(timestamp=at(0), source="player", message="never dropped")
        outbox.publish(error)
        await asyncio.sleep(0.3)
        await outbox.close()
        got = []
        while (item := sent.get_nowait()) is not None:
            got.append(item[1])
    reports = [m for m in got if isinstance(m, ErrorEvent) and m.source == "slow"]
    assert outbox.dropped > 0 and outbox.failed == 0
    assert any(m.message == "never dropped" for m in got if isinstance(m, ErrorEvent))
    assert reports and reports[0].message.startswith("the server-side publisher for slow cannot publish")
    measurements = [m for m in got if isinstance(m, Measurement)]
    assert measurements[-1].mRID == "m39"  # the newest survives; the oldest were dropped
