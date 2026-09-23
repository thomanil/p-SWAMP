# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Remote Data Client against the stub service, end to end and in-process.

The client's HTTP half runs over ``httpx.ASGITransport`` straight into the
stub's FastAPI app, and the stub's sink *is* the client's ``InMemoryResultFeed``
-- so the REST + envelope contract is exercised exactly as over the wire, with
no port and no broker. The core's conformance suite then proves the client is
a provider; a player over it proves a bounded replay works through it; and,
gated on ``KAFKA_TEST_BOOTSTRAP_SERVERS``, the Kafka sink and feed round-trip
through a real topic:

    KAFKA_TEST_BOOTSTRAP_SERVERS=127.0.0.1:19092 ./scripts/run-python-server-tests.sh -k broker -v
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest

from pmu_test_streamer.sample_client import load_sample
from pswamp_core.bus import InProcessBus, Overflow
from pswamp_core.datagateway import DataGateway, Player
from pswamp_core.datagateway.clients.remote_data import (
    InMemoryResultFeed,
    KafkaResultFeed,
    RemoteDataClient,
)
from pswamp_core.datagateway.conformance import DataClientConformance
from pswamp_core.messages import Command, PlayerStatus, PmuFrame
from remote_data_stub.app import create_app
from remote_data_stub.kafka_sink import KafkaSink
from remote_data_stub.recording import TiledRecording
from remote_data_stub.service import QueryService

BOOTSTRAP = os.environ.get("KAFKA_TEST_BOOTSTRAP_SERVERS", "")
needs_broker = pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_TEST_BOOTSTRAP_SERVERS is not set")


def hermetic_client(repeat: int = 1, **kwargs) -> RemoteDataClient:
    """The client wired to the stub in-process: ASGI for HTTP, one feed as the sink."""
    feed = InMemoryResultFeed()
    service = QueryService(TiledRecording.load(repeat=repeat), feed)
    http = httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(service)), base_url="http://stub")
    return RemoteDataClient("remote_data", url="http://stub", http_client=http, feed=feed, **kwargs)


class TestRemoteDataClientConformance(DataClientConformance):
    """The provider contract, through a gateway, over REST and the envelope."""

    @pytest.fixture
    def client_under_test(self):
        return hermetic_client()

    @pytest.fixture
    def conformance_model(self):
        return PmuFrame

    @pytest.fixture
    def conformance_records(self):
        return list(load_sample().frames)


async def test_frames_are_served_through_the_contract_with_their_layout():
    client = hermetic_client(repeat=2)
    gateway = DataGateway([client])
    coverage = await gateway.coverage(PmuFrame)
    assert coverage is not None
    assert coverage.range.end - coverage.range.start == timedelta(seconds=6.0)
    t0 = coverage.range.start
    chunk = [f async for f in gateway.consume(PmuFrame, t0 + timedelta(seconds=4), t0 + timedelta(seconds=5))]
    assert len(chunk) == 20 and all(f.header == load_sample().header for f in chunk)
    await client.close()


async def test_a_bounded_replay_runs_through_the_remote_store():
    client = hermetic_client(repeat=2)
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    player = Player(DataGateway([client]), bus, model=PmuFrame, paced=False)
    with bus.subscribe(PmuFrame, overflow=Overflow.GROW) as frames, bus.subscribe(
        PlayerStatus, overflow=Overflow.GROW
    ) as statuses:
        await player.start()
        try:
            t0 = player.status().coverage_start
            bus.publish(Command(verb="replay", args={"offset_s": 1.0, "end_offset_s": 1.25, "play": True}))
            got = [await asyncio.wait_for(frames.get(), 2) for _ in range(5)]
            ended = await _wait_status(statuses, lambda s: s.ended)
        finally:
            await player.stop()
    assert [f.timestamp for f in got] == [t0 + timedelta(seconds=1.0 + 0.05 * i) for i in range(5)]
    assert ended.paused and ended.range_end == t0 + timedelta(seconds=1.25) and ended.error is None
    await client.close()


async def test_a_stub_that_stops_answering_surfaces_as_a_player_error():
    client = hermetic_client(timeout=timedelta(seconds=0.1))
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    player = Player(DataGateway([client]), bus, model=PmuFrame, paced=True, speed=2.0)
    with bus.subscribe(PlayerStatus, overflow=Overflow.GROW) as statuses:
        await player.start()
        try:
            # Silence the service: the feed drops every envelope from now on.
            client.feed.dispatch = lambda result: None  # type: ignore[method-assign]
            await player.seek(player.status().coverage_start + timedelta(seconds=1))
            player.resume()
            failed = await _wait_status(statuses, lambda s: s.error is not None, timeout=3)
        finally:
            await player.stop()
    assert failed.paused and failed.ended and failed.error.startswith("TimeoutError")
    await client.close()


async def _wait_status(subscription, predicate, timeout: float = 2.0) -> PlayerStatus:
    async def _wait():
        while True:
            message = await subscription.get()
            if predicate(message):
                return message

    return await asyncio.wait_for(_wait(), timeout)


@needs_broker
async def test_results_round_trip_through_a_real_topic():
    """The Kafka halves on both sides -- the stub's sink and the client's feed --
    over one fresh topic on the compose broker; HTTP still in-process."""
    topic = f"test-remote-data-{uuid4().hex[:8]}"
    sink = KafkaSink(BOOTSTRAP, topic)
    service = QueryService(TiledRecording.load(repeat=1), sink)
    feed = KafkaResultFeed(BOOTSTRAP, topic)
    http = httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(service)), base_url="http://stub")
    client = RemoteDataClient("remote_data", url="http://stub", http_client=http, feed=feed)
    try:
        await sink.open()
        await client.open()
        assert feed.running
        gateway = DataGateway([client])
        coverage = await gateway.coverage(PmuFrame)
        t0 = coverage.range.start
        first = [f async for f in gateway.consume(PmuFrame, t0, t0 + timedelta(seconds=1))]
        second = [f async for f in gateway.consume(PmuFrame, t0 + timedelta(seconds=2), None)]
    finally:
        await client.close()
        await sink.close()
    assert len(first) == 20 and first[0].timestamp == t0
    assert len(second) == 20 and second[-1].timestamp == t0 + timedelta(seconds=2.95)
    assert sink.published == 42 and feed.dropped == 0
