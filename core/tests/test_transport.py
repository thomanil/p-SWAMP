# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The transport contract, over the portless InMemoryTransport."""

from __future__ import annotations

import pytest
from support import Measurement, at, measurement, take

from pswamp_core.settings import MissingSettingError
from pswamp_core.transport import InMemoryTransport, transport_from_env


async def test_a_keyed_subscriber_hears_only_its_key():
    broker = InMemoryTransport()
    with broker.subscribe(Measurement, "k1") as k1, broker.subscribe(Measurement) as every:
        await broker.publish(measurement(1, at(1)), "k1")
        await broker.publish(measurement(2, at(2)), "k2")
        ((key, got),) = await take(k1, 1)
        assert key == "k1" and got.mRID == "m1"
        assert [k for k, _ in await take(every, 2)] == ["k1", "k2"]
        assert k1.get_nowait() is None
    await broker.close()


async def test_it_carries_backwards_timestamps_in_order():
    """A looping replay goes back in time; a transport never reads a timestamp."""
    broker = InMemoryTransport()
    with broker.subscribe(Measurement, "k") as sub:
        for i, t in enumerate([5, 6, 0, 1]):
            await broker.publish(measurement(i, at(t)), "k")
        assert [m.mRID for _, m in await take(sub, 4)] == ["m0", "m1", "m2", "m3"]


def test_transport_from_env(monkeypatch):
    monkeypatch.delenv("X_MODULE_TRANSPORT", raising=False)
    assert transport_from_env("X_MODULE_TRANSPORT") is None
    monkeypatch.setenv("X_MODULE_TRANSPORT", "mem:pswamp_core.transport:InMemoryTransport")
    transport = transport_from_env("X_MODULE_TRANSPORT")
    assert isinstance(transport, InMemoryTransport) and transport.name == "mem"
    monkeypatch.setenv("X_MODULE_TRANSPORT", "not-a-spec")
    with pytest.raises(MissingSettingError):
        transport_from_env("X_MODULE_TRANSPORT")
    monkeypatch.setenv("X_MODULE_TRANSPORT", "x:pswamp_core.messages:PmuFrame")
    with pytest.raises(MissingSettingError):
        transport_from_env("X_MODULE_TRANSPORT")
