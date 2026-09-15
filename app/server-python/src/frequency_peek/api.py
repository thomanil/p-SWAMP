# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Frequency peek app's backend: the web edge over one core pipeline per client.

A module and the page that shows it, built by the recipe in
``doc/server-data-architecture.md`` ("Adding things"): the configured PMU
providers behind the core's gateway → player → bus chain, one
``FrequencyModule`` reading frames off that bus, and this edge forwarding its
results down one socket::

    providers ── DataGateway ── Player ──▶ bus ──▶ FrequencyModule ──▶ bus ──▶ this socket

The page is **live only**: the pipeline switches its player to the live feed
as soon as it starts, so the frequencies on screen are stamped now and there
are no transport controls -- and so no commands. State goes down the socket;
nothing comes up.

Per client: one pipeline, built by ``REGISTRY`` on first connect and keyed by
the browser's client id, capped and idle-evicted like the streamer's.

server.py mounts this ``router`` under /api/frequency-peek. Nothing here knows
about that prefix.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Literal

from fastapi import APIRouter, FastAPI, WebSocket
from pydantic import BaseModel, Field
from shared import event_queue, get_logger, read_client_id, serve_updates

from pswamp_core.bus import InProcessBus
from pswamp_core.datagateway import DataGateway, Player, gateway_from_env
from pswamp_core.messages import PlayerStatus, PmuFrame
from pswamp_core.pipeline import CapacityError, Pipeline, PipelineRegistry

from .frequency_module import FrequencyModule, FrequencyResult

logger = get_logger("frequency-peek")

#: The providers a deployment gets unless FREQUENCY_PEEK_DATA_CLIENTS names
#: others. The recording is here for the header the module reads its column
#: layout from; the live feed is what the page shows.
DEFAULT_DATA_CLIENTS = (
    "sample:pmu_test_streamer.sample_client:SampleRecordingClient,"
    "live:pmu_test_streamer.live_client:LiveSyntheticClient"
)
DATA_CLIENTS_VARIABLE = "FREQUENCY_PEEK_DATA_CLIENTS"

MAX_PIPELINES = 8
IDLE_EVICT_SECONDS = 300.0


# --- the pipeline, per client -------------------------------------------------


class LivePipeline(Pipeline):
    """A pipeline whose player goes live as soon as it starts.

    The core's player opens a replay when the gateway has history, and this
    page has no transport to resume it with -- so switch to the live feed on
    start, and fall back to an autoplaying replay when no live source is
    configured, rather than showing dashes for ever.
    """

    async def start(self) -> None:
        await super().start()
        if self.player.can_go_live:
            await self.player.go_live()


async def build_pipeline(client_id: str) -> LivePipeline:
    """One client's pipeline: the configured providers, a bus, a live player and
    the frequency module. Called by the registry, never directly."""
    gateway: DataGateway = gateway_from_env(DEFAULT_DATA_CLIENTS, variable=DATA_CLIENTS_VARIABLE)
    bus = InProcessBus()
    player = Player(gateway, bus, model=PmuFrame, autoplay=True, loop=True)
    return LivePipeline(client_id, gateway, bus, player, [FrequencyModule()])


REGISTRY: PipelineRegistry[LivePipeline] = PipelineRegistry(
    build_pipeline, max_pipelines=MAX_PIPELINES, idle_seconds=IDLE_EVICT_SECONDS
)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """Bind the registry to the loop for as long as the server is up; drain it
    on shutdown. An idle server runs no pipeline at all."""
    REGISTRY.bind(asyncio.get_running_loop())
    try:
        yield
    finally:
        await REGISTRY.stop_all()
        REGISTRY.bind(None)


# --- the socket message ---------------------------------------------------------


class FrequencyPeekState(BaseModel):
    """The one message pushed on connect and on every new frequency result.

    Keys are snake_case on the wire, and stay that way: the page reads the
    generated type for this model rather than a renamed mirror of it.
    """

    type: Literal["state"] = "state"
    player: PlayerStatus = Field(description="Which stream is open: live, or a replay.")
    frequency: FrequencyResult | None = Field(
        description="The frequency module's latest result; null until the first frame."
    )


def state_message(pipeline: Pipeline) -> FrequencyPeekState:
    latest = pipeline.latest
    return FrequencyPeekState(
        player=pipeline.player.status(),
        frequency=latest.get(FrequencyResult) if latest else None,
    )


# --- websocket endpoint (downstream only) ---------------------------------------

router = APIRouter()


@contextlib.asynccontextmanager
async def connected_pipeline(ws: WebSocket) -> AsyncIterator[Pipeline | None]:
    """Accept one socket and hold its client's pipeline for as long as it lives.

    Yields ``None`` when the connection was refused: no usable client id is
    closed *before* accepting (1008); at capacity the socket is accepted first
    and then closed with 1013, because a code only reaches the browser on an
    established connection, and the web client treats 1013 as terminal.

    (A copy of the streamer's, pointed at this registry -- the handshake is not
    shared yet; see ``doc/server-data-architecture.md``, "Adding things".)
    """
    client_id = read_client_id(ws)
    if client_id is None:
        await ws.close(code=1008)  # policy violation
        yield None
        return

    await ws.accept()
    try:
        pipeline = await REGISTRY.acquire(client_id)
    except CapacityError:
        logger.warning("refused client %s: all %s pipelines in use", client_id, REGISTRY.max_pipelines)
        await ws.close(code=1013)  # try again later
        yield None
        return
    except Exception:
        logger.exception("failed to start pipeline for client %s", client_id)
        await ws.close(code=1011)  # unexpected server error
        yield None
        return

    try:
        yield pipeline
    finally:
        REGISTRY.release(client_id)


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_pipeline(ws) as pipeline:
        if pipeline is None:
            return
        logger.info("client %s: connected (%s live)", pipeline.key, len(REGISTRY.keys()))
        # Open the queue before the opening message, so a result published in
        # between is not lost; the builder reads the bus's newest, so it
        # ignores which event woke it.
        with event_queue(pipeline.bus, FrequencyResult) as updates:
            await serve_updates(ws, updates, lambda _event: state_message(pipeline))
        logger.info("client %s: disconnected", pipeline.key)
