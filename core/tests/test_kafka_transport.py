# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Kafka transport: what needs no broker, and -- gated -- what does.

    KAFKA_TEST_BOOTSTRAP_SERVERS=127.0.0.1:19092 ./scripts/run-python-server-tests.sh -k kafka -v

runs the gated half against the compose stack's Kafka (its EXTERNAL
listener). Every run uses its own topic prefix, so a shared broker never bleeds
between runs.
"""

from __future__ import annotations

import os
import sys
from uuid import uuid4

import pytest
from support import Measurement, take

from pswamp_core.datagateway import MissingSettingError
from pswamp_core.transport import transport_from_env
from pswamp_core.transport.kafka import KafkaTransport
from pswamp_core.util.time import utcnow

BOOTSTRAP = os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "")
needs_broker = pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_TEST_BOOTSTRAP_SERVERS is not set")


def test_constructing_needs_no_broker_and_no_aiokafka_import():
    before = "aiokafka" in sys.modules
    transport = KafkaTransport("k", "localhost:9092")
    assert transport.name == "k" and ("aiokafka" in sys.modules) == before


def test_topic_is_the_model_descriptor_under_an_optional_prefix():
    assert KafkaTransport("k", "h:1").topic_for(Measurement) == "measurement"
    assert KafkaTransport("k", "h:1", topic_prefix="no").topic_for(Measurement) == "no.measurement"


def test_from_env_reads_the_block_and_requires_the_servers(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "a:9092, b:9092")
    monkeypatch.setenv("KAFKA_TOPIC_PREFIX", "se")
    monkeypatch.setenv("T", "kafka:pswamp_core.transport.kafka:KafkaTransport")
    transport = transport_from_env("T")
    assert isinstance(transport, KafkaTransport)
    assert transport.bootstrap_servers == ["a:9092", "b:9092"]
    assert transport.topic_for(Measurement) == "se.measurement"
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS")
    with pytest.raises(MissingSettingError):
        transport_from_env("T")


def make_transport() -> KafkaTransport:
    return KafkaTransport("kafka", BOOTSTRAP, topic_prefix=f"test-{uuid4().hex[:8]}")


@needs_broker
async def test_stream_round_trip_by_key():
    transport = make_transport()
    try:
        with transport.subscribe(Measurement, "k1") as k1, transport.subscribe(Measurement) as every:
            await k1.ready(20)
            await transport.publish(Measurement(mRID="m1", value=1.0, timestamp=utcnow()), "k1")
            await transport.publish(Measurement(mRID="m2", value=2.0, timestamp=utcnow()), "k2")
            (got,) = await take(k1, 1, timeout=10)
            both = await take(every, 2, timeout=10)
    finally:
        await transport.close()
    assert got == ("k1", got[1]) and got[1].mRID == "m1"
    assert sorted(k for k, _ in both) == ["k1", "k2"]

