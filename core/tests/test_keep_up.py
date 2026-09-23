# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A module that falls behind its input says so, in-process and from a worker.

``Module.run`` watches its own input queue (``KeepUpMonitor``): drops, and the
age of input that crossed a transport. Past its ``KeepUp`` policy it publishes
an ``ErrorEvent`` on its bus -- which, for a module hosted by a worker, the
host sends back over the transport and the pipeline's ``RemoteModule`` puts on
the pipeline's bus.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import ClassVar

from support import Measurement, Number, NumberResult, at, measurement, take

from pswamp_core.bus import InProcessBus, Overflow
from pswamp_core.datagateway import DataGateway
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.messages import ErrorEvent, sent_at, stamp_sent_at
from pswamp_core.modules import KeepUp, Module
from pswamp_core.remote import ModuleHost, RemoteModule
from pswamp_core.transport import InMemoryTransport


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
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    task = asyncio.create_task(module.run(bus))
    await asyncio.sleep(0)
    try:
        yield bus
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        bus.bind(None)


def burst(bus: InProcessBus, n: int, start: int = 0) -> None:
    for i in range(start, start + n):
        bus.publish(measurement(i, at(i)))


async def test_a_module_that_drops_input_reports_it_and_then_that_it_caught_up():
    module = Slow()
    async with running(module) as bus:
        with bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            burst(bus, 40)  # a queue of 4: most of these are dropped
            (behind,) = await take(errors, 1)
            assert behind.source == "slow"
            assert "is not keeping up with measurement" in behind.message
            assert "input dropped" in (behind.detail or "")
            assert module.monitor.behind and module.monitor.input_dropped > 0

            # Then a trickle it can handle: a whole quiet interval means caught up.
            for i in range(40, 60):
                bus.publish(measurement(i, at(i)))
                await asyncio.sleep(0.02)
            reports = [e.message for e in await take(errors, 1)]
            assert any("caught up" in m for m in reports)
    assert not module.monitor.behind


async def test_input_that_crossed_a_transport_too_long_ago_counts_as_behind():
    module = Slow()
    async with running(module) as bus:
        with bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            late = measurement(1, at(1))
            stamp_sent_at(late, time.time() - 10)
            bus.publish(late)
            (behind,) = await take(errors, 1)
    assert "is not keeping up" in behind.message
    assert "10." in (behind.detail or "")  # "oldest input read 10.0 s after it was sent"
    assert module.monitor.input_age_s is not None and module.monitor.input_age_s >= 10


async def test_while_behind_it_reports_at_most_once_per_interval():
    module = Slow()
    async with running(module) as bus:
        with bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            started = time.monotonic()
            for n in range(12):  # keep it behind for ~0.3 s
                burst(bus, 20, start=n * 20)
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
    async with running(module) as bus:
        with bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            burst(bus, 40)
            await asyncio.sleep(0.2)
            assert errors.get_nowait() is None
    assert module.monitor.input_dropped > 0 and module.monitor.reports == 0


# --- across a transport ---------------------------------------------------------------


@contextlib.asynccontextmanager
async def hosted(module_cls, transport):
    host = ModuleHost(module_cls, transport)
    task = asyncio.create_task(host.serve())
    await asyncio.sleep(0.01)
    try:
        yield host
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@contextlib.asynccontextmanager
async def pipeline_side(remote: RemoteModule):
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    await remote.setup(DataGateway([InMemoryClient("none", Measurement)]), bus)
    task = asyncio.create_task(remote.run(bus))
    await asyncio.sleep(0.01)
    try:
        yield bus
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        bus.bind(None)


async def test_the_in_memory_transport_stamps_what_it_delivers():
    broker = InMemoryTransport()
    with broker.subscribe(Measurement) as feed:
        before = time.time()
        await broker.publish(measurement(1, at(1)), "k")
        ((_, message),) = await take(feed, 1)
    assert sent_at(message) is not None and sent_at(message) >= before


async def test_a_worker_side_failure_reaches_its_own_pipeline_only():
    broker = InMemoryTransport()
    mine = RemoteModule(Failing, broker, "k1")
    theirs = RemoteModule(Failing, broker, "k2")
    async with hosted(Failing, broker), pipeline_side(mine) as bus1, pipeline_side(theirs) as bus2:
        with bus1.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors1, bus2.subscribe(
            ErrorEvent, overflow=Overflow.GROW
        ) as errors2:
            bus1.publish(measurement(1, at(1)))
            (error,) = await take(errors1, 1)
            await asyncio.sleep(0.02)
            assert errors2.get_nowait() is None
    assert error.source == "failing" and "ValueError: boom" in (error.detail or "")
    assert mine.errors_received == 1 and theirs.errors_received == 0


async def test_a_remote_module_ignores_errors_from_another_module_on_its_key():
    broker = InMemoryTransport()
    remote = RemoteModule(Slow, broker, "k1")
    async with pipeline_side(remote) as bus:
        with bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            await broker.publish(ErrorEvent(timestamp=at(0), source="someone-else", message="x"), "k1")
            await asyncio.sleep(0.02)
            assert errors.get_nowait() is None
    assert remote.errors_received == 0


async def test_a_hosted_module_that_falls_behind_is_reported_on_the_pipeline_bus():
    broker = InMemoryTransport()

    class SlowHosted(Slow):
        maxsize = 64  # nothing dropped: only the age says it is behind
        keep_up = KeepUp(max_input_age_s=0.05, report_every_s=0.1)
        delay = 0.03

    remote = RemoteModule(SlowHosted, broker, "k1")
    async with hosted(SlowHosted, broker), pipeline_side(remote) as bus:
        with bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            burst(bus, 10)
            (behind,) = await take(errors, 1)
    assert behind.source == "slow" and "is not keeping up with measurement" in behind.message
    assert "after it was sent" in (behind.detail or "")


async def test_a_pipeline_that_cannot_publish_fast_enough_says_so_under_its_own_label():
    class SlowBroker(InMemoryTransport):
        async def publish(self, message, key):
            await asyncio.sleep(0.01)
            await super().publish(message, key)

    class Small(Slow):
        maxsize = 4

    remote = RemoteModule(Small, SlowBroker(), "k1", maxsize=4)
    async with pipeline_side(remote) as bus:
        with bus.subscribe(ErrorEvent, overflow=Overflow.GROW) as errors:
            burst(bus, 40)
            (behind,) = await take(errors, 1)
    assert behind.source == "slow"  # what the tray groups by, and RemoteModule filters on
    assert behind.message.startswith("the server-side publisher for slow cannot publish measurement")
    assert remote.monitor.input_dropped > 0
