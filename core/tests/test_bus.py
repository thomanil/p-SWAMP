# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The in-process bus: fan-out, subclass matching, overflow, the thread seam."""

from __future__ import annotations

import asyncio
import threading

import pytest
from support import Measurement, Number, NumberResult, at, measurement, take

from pswamp_core.bus import InProcessBus, Latest, Overflow
from pswamp_core.messages import AppIdentity, ResultEnvelope

APP = AppIdentity(name="t", uuid="1")


def result(value: float) -> NumberResult:
    return NumberResult(timestamp=at(value), app=APP, result=Number(value=value))


async def test_every_subscription_receives_every_message():
    bus = InProcessBus()
    with bus.subscribe(Measurement) as a, bus.subscribe(Measurement) as b:
        bus.publish(measurement(1, at(1)))
        bus.publish(measurement(2, at(2)))
        assert [m.mRID for m in await take(a, 2)] == ["m1", "m2"]
        assert [m.mRID for m in await take(b, 2)] == ["m1", "m2"]
    assert bus.subscriber_count == 0


async def test_subscribing_to_a_base_class_receives_subclasses():
    bus = InProcessBus()
    with bus.subscribe(ResultEnvelope) as results, bus.subscribe(Measurement) as measurements:
        bus.publish(result(1.0))
        bus.publish(measurement(1, at(1)))
        (got,) = await take(results, 1)
        assert isinstance(got, NumberResult)
        assert got.result.value == 1.0
        assert measurements.get_nowait().mRID == "m1"
        assert results.get_nowait() is None


async def test_drop_oldest_keeps_the_newest():
    bus = InProcessBus()
    with bus.subscribe(Measurement, overflow=Overflow.DROP_OLDEST, maxsize=2) as sub:
        for i in range(4):
            bus.publish(measurement(i, at(i)))
        assert [m.mRID for m in await take(sub, 2)] == ["m2", "m3"]
        assert sub.dropped == 2


async def test_latest_only_keeps_one():
    bus = InProcessBus()
    with bus.subscribe(Measurement, overflow=Overflow.LATEST_ONLY) as sub:
        for i in range(4):
            bus.publish(measurement(i, at(i)))
        assert (await take(sub, 1))[0].mRID == "m3"
        assert sub.get_nowait() is None


async def test_grow_keeps_everything():
    bus = InProcessBus()
    with bus.subscribe(Measurement, overflow=Overflow.GROW, maxsize=1) as sub:
        for i in range(10):
            bus.publish(measurement(i, at(i)))
        assert len(await take(sub, 10)) == 10


async def test_close_ends_iteration():
    bus = InProcessBus()
    sub = bus.subscribe(Measurement)
    bus.publish(measurement(0, at(0)))
    sub.close()
    got = [m.mRID async for m in sub]
    assert got == ["m0"]
    assert bus.subscriber_count == 0


async def test_publish_threadsafe_crosses_from_a_thread():
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    with bus.subscribe(Measurement) as sub:
        thread = threading.Thread(target=bus.publish_threadsafe, args=(measurement(7, at(7)),))
        thread.start()
        thread.join()
        (got,) = await take(sub, 1)
        assert got.mRID == "m7"


async def test_publish_threadsafe_is_a_noop_when_unbound():
    bus = InProcessBus()
    with bus.subscribe(Measurement) as sub:
        bus.publish_threadsafe(measurement(1, at(1)))
        await asyncio.sleep(0.01)
        assert sub.get_nowait() is None


async def test_listeners_run_before_subscriptions_and_latest_is_current():
    bus = InProcessBus()
    latest = Latest(bus)
    seen: list[str] = []
    remove = bus.add_listener(Measurement, lambda m: seen.append(m.mRID))
    with bus.subscribe(Measurement) as sub:
        bus.publish(measurement(1, at(1)))
        bus.publish(result(2.0))
        (got,) = await take(sub, 1)
        assert seen == ["m1"]
        assert latest.get(Measurement) is got
        assert latest.get(ResultEnvelope).result.value == 2.0
        assert latest.get(NumberResult).result.value == 2.0
    remove()
    bus.publish(measurement(2, at(2)))
    assert seen == ["m1"]
    latest.detach()
    bus.publish(measurement(3, at(3)))
    assert latest.get(Measurement).mRID == "m2"


async def test_a_failing_listener_does_not_stop_delivery():
    bus = InProcessBus()

    def boom(_message):
        raise RuntimeError("boom")

    bus.add_listener(Measurement, boom)
    with bus.subscribe(Measurement) as sub:
        bus.publish(measurement(1, at(1)))
        assert (await take(sub, 1))[0].mRID == "m1"


def test_subscribe_needs_a_model():
    with pytest.raises(ValueError):
        InProcessBus().subscribe()
