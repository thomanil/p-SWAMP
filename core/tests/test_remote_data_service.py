# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Remote Data Client against the stub service, end to end and in-process.

The client's HTTP runs over ``httpx.ASGITransport`` straight into the stub's
FastAPI app, so the REST contract and its NDJSON answer are exercised exactly
as over the wire, with no port. (``ASGITransport`` collects the whole body
before it returns, so this proves the contract, not the streaming: the lazy
pull is pinned in ``test_remote_data_client.py``, and the real socket is
``scripts/check-remote-data-service.sh``'s and the e2e smoke test's.) The core's conformance suite then proves the
client is a provider, and a player over it proves a bounded replay works
through it.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

httpx = pytest.importorskip("httpx")
pytest.importorskip("fastapi")

from pswamp_core.bus import InProcessBus, Overflow  # noqa: E402
from pswamp_core.datagateway import DataGateway, Player  # noqa: E402
from pswamp_core.datagateway.clients.remote_data import RemoteDataClient  # noqa: E402
from pswamp_core.datagateway.conformance import DataClientConformance  # noqa: E402
from pswamp_core.messages import Command, PlayerStatus, PmuFrame  # noqa: E402
from remote_data_stub.app import create_app  # noqa: E402
from remote_data_stub.recording import TiledRecording, load_frames  # noqa: E402
from remote_data_stub.service import QueryService  # noqa: E402


class GoesQuiet(QueryService):
    """The stub, until ``quiet`` is set: from then on a query is accepted and
    never answered, which is a store that hangs."""

    quiet = False

    async def stream(self, query):
        if self.quiet:
            await asyncio.Event().wait()
        async for line in super().stream(query):
            yield line


def hermetic_client(repeat: int = 1, service: QueryService | None = None, **kwargs) -> RemoteDataClient:
    """The client wired to the stub in-process, over ASGI."""
    service = service if service is not None else QueryService(TiledRecording.load(repeat=repeat))
    http = httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(service)), base_url="http://stub")
    return RemoteDataClient("remote_data", url="http://stub", http_client=http, **kwargs)


class TestRemoteDataClientConformance(DataClientConformance):
    """The provider contract, through a gateway, over REST and the streamed answer."""

    @pytest.fixture
    def client_under_test(self):
        return hermetic_client()

    @pytest.fixture
    def conformance_model(self):
        return PmuFrame

    @pytest.fixture
    def conformance_records(self):
        return list(load_frames())


async def test_frames_are_served_through_the_contract_with_their_layout():
    client = hermetic_client(repeat=2)
    gateway = DataGateway([client])
    coverage = await gateway.coverage(PmuFrame)
    assert coverage is not None
    assert coverage.range.end - coverage.range.start == timedelta(seconds=6.0)
    t0 = coverage.range.start
    chunk = [f async for f in gateway.consume(PmuFrame, t0 + timedelta(seconds=4), t0 + timedelta(seconds=5))]
    assert len(chunk) == 20 and all(f.header == load_frames()[0].header for f in chunk)
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
    service = GoesQuiet(TiledRecording.load(repeat=1))
    client = hermetic_client(service=service, timeout=timedelta(seconds=0.1))
    bus = InProcessBus()
    bus.bind(asyncio.get_running_loop())
    player = Player(DataGateway([client]), bus, model=PmuFrame, paced=True, speed=2.0)
    with bus.subscribe(PlayerStatus, overflow=Overflow.GROW) as statuses:
        await player.start()
        try:
            service.quiet = True  # the next query is accepted and never answered
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

