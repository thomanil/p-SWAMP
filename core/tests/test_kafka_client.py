# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Kafka client's behaviour that needs no broker (and no aiokafka)."""

from __future__ import annotations

from datetime import timedelta

import pytest
from support import Measurement

from pswamp_core.datagateway import Capability, MissingSettingError
from pswamp_core.datagateway.clients.kafka import KafkaClient
from pswamp_core.messages import DataModel, PmuFrame
from pswamp_core.util.time import utcnow


def _client(**kwargs) -> KafkaClient:
    return KafkaClient("bus", "localhost:9092", **kwargs)


def test_topic_is_the_model_descriptor_under_an_optional_prefix():
    assert _client().topic_for(Measurement) == "measurement"
    assert _client(topic_prefix="no").topic_for(PmuFrame) == "no.pmu.frame"
    assert _client(topic_prefix="no", topics={PmuFrame: "custom"}).topic_for(PmuFrame) == "custom"


def test_carries_every_model_by_default():
    client = _client()
    assert client.supports(Measurement) and client.supports(PmuFrame)
    assert not _client(supported_models=Measurement).supports(PmuFrame)


async def test_live_coverage_is_a_rolling_window_with_an_open_end():
    before = utcnow()
    coverage = await _client(retention_seconds=timedelta(minutes=20)).coverage(Measurement)
    assert coverage is not None and coverage.live and coverage.range.end is None
    assert timedelta(minutes=19) < before - coverage.range.start <= timedelta(minutes=20)
    bounded = await _client(capabilities=Capability.HISTORY_CONSUME).coverage(Measurement)
    assert bounded is not None and not bounded.live and bounded.range.end is not None
    assert await _client(supported_models=Measurement).coverage(PmuFrame) is None


def test_from_env_reads_the_clients_block(monkeypatch):
    monkeypatch.setenv("BUS_BOOTSTRAP_SERVERS", "a:9092, b:9092")
    monkeypatch.setenv("BUS_TOPIC_PREFIX", "se")
    monkeypatch.setenv("BUS_RETENTION_SECONDS", "60")
    monkeypatch.setenv("BUS_CAPABILITIES", "LIVE_CONSUME,PRODUCE")
    client = KafkaClient.from_env("bus")
    assert client.bootstrap_servers == ["a:9092", "b:9092"]
    assert client.topic_for(Measurement) == "se.measurement"
    assert client.retention == timedelta(seconds=60)
    assert client.capabilities == Capability.LIVE_CONSUME | Capability.PRODUCE
    assert client.supports(DataModel)


def test_from_env_requires_bootstrap_servers(monkeypatch):
    monkeypatch.delenv("BUS_BOOTSTRAP_SERVERS", raising=False)
    with pytest.raises(MissingSettingError):
        KafkaClient.from_env("bus")


async def test_produce_refuses_before_touching_the_broker():
    with pytest.raises(TypeError):
        await _client(capabilities=Capability.LIVE_CONSUME).produce(Measurement(mRID="m", timestamp=utcnow()))
    with pytest.raises(ValueError):
        await _client().produce(Measurement(mRID="m"))
