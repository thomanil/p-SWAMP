# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The in-memory transport behaves as a broker does: every message goes through
JSON, a topic carries exactly one class of one app, and the key filters."""

from __future__ import annotations

import math

import pytest
from support import Measurement, Number, NumberResult, at, measurement, take

from pswamp_core.datagateway import MissingSettingError
from pswamp_core.messages import PmuFrame, PmuHeader, ResultEnvelope, sent_at
from pswamp_core.subscription import Overflow
from pswamp_core.transport import InMemoryTransport, transport_from_env

HEADER = PmuHeader(station=["a", "b"], channel=["f", "f"], measurement=["f", "f"], units=["Hz", "Hz"], data_rate=1.0)


async def test_a_subscriber_receives_an_equal_message_that_went_through_json():
    broker = InMemoryTransport()
    sent = measurement(1, at(1))
    with broker.subscribe(Measurement, app="a") as feed:
        await broker.publish(sent, app="a", key="k")
        ((key, got),) = await take(feed, 1)
    assert key == "k"
    assert got.model_dump() == sent.model_dump() and got is not sent
    assert sent_at(got) is not None and sent_at(sent) is None


async def test_nan_arrives_as_null():
    broker = InMemoryTransport()
    frame = PmuFrame(timestamp=at(0), mRID="s", header=HEADER, values=[50.0, math.nan])
    with broker.subscribe(PmuFrame, app="a") as feed:
        await broker.publish(frame, app="a", key="k")
        ((_, got),) = await take(feed, 1)
    assert got.values == [50.0, None]


async def test_a_topic_carries_one_class_so_a_base_class_hears_no_subclass():
    broker = InMemoryTransport()
    result = NumberResult(timestamp=at(0), app={"name": "n", "uuid": "u"}, result=Number(value=1))
    with broker.subscribe(ResultEnvelope, app="a") as base, broker.subscribe(NumberResult, app="a") as exact:
        await broker.publish(result, app="a", key="k")
        await take(exact, 1)
    assert base.get_nowait() is None


async def test_apps_are_apart_and_the_key_filters():
    broker = InMemoryTransport()
    with broker.subscribe(Measurement, app="a", key="k1") as mine, broker.subscribe(
        Measurement, app="b"
    ) as other_app, broker.subscribe(Measurement, app="a") as every_key:
        await broker.publish(measurement(1, at(1)), app="a", key="k1")
        await broker.publish(measurement(2, at(2)), app="a", key="k2")
        got = await take(every_key, 2)
        ((_, only),) = await take(mine, 1)
    assert [key for key, _ in got] == ["k1", "k2"]
    assert only.mRID == "m1" and mine.get_nowait() is None
    assert other_app.get_nowait() is None


async def test_one_subscription_can_cover_several_classes_and_keeps_publish_order():
    broker = InMemoryTransport()
    with broker.subscribe(Measurement, NumberResult, app="a", overflow=Overflow.GROW) as feed:
        for i in (3, 1, 2):  # timestamps going backwards are carried as published
            await broker.publish(measurement(i, at(i)), app="a", key="k")
        await broker.publish(
            NumberResult(timestamp=at(0), app={"name": "n", "uuid": "u"}, result=Number(value=9)),
            app="a",
            key="k",
        )
        got = await take(feed, 4)
    assert [m.mRID for _, m in got[:3]] == ["m3", "m1", "m2"]
    assert isinstance(got[3][1], NumberResult)


async def test_overflow_policies():
    broker = InMemoryTransport()
    with broker.subscribe(Measurement, app="a", maxsize=2) as oldest_dropped, broker.subscribe(
        Measurement, app="a", overflow=Overflow.LATEST_ONLY
    ) as latest, broker.subscribe(Measurement, app="a", overflow=Overflow.GROW) as everything:
        for i in range(5):
            await broker.publish(measurement(i, at(i)), app="a", key="k")
        assert [m.mRID for _, m in await take(oldest_dropped, 2)] == ["m3", "m4"]
        assert oldest_dropped.dropped == 3
        assert [m.mRID for _, m in await take(latest, 1)] == ["m4"]
        assert len(await take(everything, 5)) == 5


async def test_a_closed_subscription_ends_its_iteration():
    broker = InMemoryTransport()
    feed = broker.subscribe(Measurement, app="a")
    feed.close()
    assert [item async for item in feed] == []
    await broker.publish(measurement(1, at(1)), app="a", key="k")  # nobody listening: fine


def test_transport_from_env(monkeypatch):
    monkeypatch.delenv("T", raising=False)
    unset = transport_from_env("T")
    assert isinstance(unset, InMemoryTransport) and unset.in_process  # one process: the default
    monkeypatch.setenv("T", "mem:pswamp_core.transport:InMemoryTransport")
    transport = transport_from_env("T")
    assert isinstance(transport, InMemoryTransport) and transport.name == "mem"
    monkeypatch.setenv("T", "not-a-spec")
    with pytest.raises(MissingSettingError):
        transport_from_env("T")
