# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Kafka client against a real broker -- skipped unless one is named.

    KAFKA_TEST_BOOTSTRAP_SERVERS=127.0.0.1:19092 ./scripts/run-python-server-tests.sh -k kafka -v

is the run against the compose stack's Redpanda (its external listener). Each
run uses its own topic prefix, so a shared broker never bleeds between runs.
"""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from support import Measurement

from pswamp_core.bridge import newest
from pswamp_core.datagateway import Capability, DataGateway
from pswamp_core.datagateway.conformance import DataClientConformance
from pswamp_core.util.time import utcnow

BOOTSTRAP = os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "")
pytestmark = pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_TEST_BOOTSTRAP_SERVERS is not set")
if BOOTSTRAP:
    pytest.importorskip("aiokafka")
    from pswamp_core.datagateway.clients.kafka import KafkaClient


def make_client(capabilities: Capability) -> "KafkaClient":
    return KafkaClient(
        "bus",
        BOOTSTRAP,
        supported_models=Measurement,
        topic_prefix=f"test-{uuid4().hex[:8]}",
        capabilities=capabilities,
    )


class TestKafkaClientConformance(DataClientConformance):
    @pytest.fixture
    def client_under_test(self):
        return make_client(Capability.LIVE_CONSUME | Capability.PRODUCE)

    @pytest.fixture
    def conformance_model(self):
        return Measurement

    @pytest.fixture
    def conformance_records(self):
        return []


async def test_produce_reaches_an_open_tail():
    client = make_client(Capability.LIVE_CONSUME | Capability.PRODUCE)
    async with DataGateway([client]) as gateway:
        stream = gateway.consume(Measurement, utcnow(), None)
        first = asyncio.create_task(stream.__anext__())
        await asyncio.sleep(2)  # the consumer's start and assignment
        await gateway.produce(Measurement(mRID="m1", value=1.0, timestamp=utcnow()))
        got = await asyncio.wait_for(first, 10)
        await stream.aclose()
    assert got.mRID == "m1"


async def test_bounded_history_read_returns_the_newest():
    client = make_client(Capability.HISTORY_CONSUME | Capability.PRODUCE)
    async with DataGateway([client]) as gateway:
        for i in range(3):
            await gateway.produce(Measurement(mRID=f"m{i}", value=float(i), timestamp=utcnow()))
        got = await asyncio.wait_for(newest(gateway, Measurement), 15)
    assert got is not None and got.mRID == "m2"


async def test_topics_are_created_on_first_use():
    client = make_client(Capability.PRODUCE)
    async with DataGateway([client]):
        await client.ensure_topic(Measurement)
        assert client.topic_for(Measurement) in client._known_topics
