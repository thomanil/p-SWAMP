# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Frequency peek app's backend: the web edge over one shared live pipeline.

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

**One pipeline per process, not per client.** This app knowingly steps off the
"everything is per client" invariant: a live stream is keyed by the *stream*
(the right-hand column of the table in the architecture doc), so every viewer
sees the same instant and the analysis runs once. The registry still manages
it -- built on the first socket, kept alive across reconnects, stopped when
the last viewer has been gone for ``IDLE_EVICT_SECONDS`` -- under the one key
``PIPELINE_KEY``. Don't "fix" this back to the client id.

**The module runs here or in another process, and nothing else changes.**
With ``FREQUENCY_PEEK_BUS_CLIENTS`` unset the pipeline's module list holds the
``FrequencyModule`` itself. Set to a provider spec naming a broker (the
``KafkaClient`` in ``pswamp_core``, or the portless ``InMemoryBroker``), the
list holds a ``TopicBridge`` instead: it produces this pipeline's frames (and
its header) onto the broker's topics and publishes the results it tails back
onto this bus, where the socket subscribes to them exactly as before. The
worker on the other side (``worker.py``) runs the same module code. See
"Running a module in another process" in the architecture doc.

server.py mounts this ``router`` under /api/frequency-peek. Nothing here knows
about that prefix.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator
from typing import Literal

from fastapi import APIRouter, FastAPI, WebSocket
from pydantic import BaseModel, Field
from shared import event_queue, get_logger, read_client_id, serve_updates

from pswamp_core.bridge import TopicBridge
from pswamp_core.bus import InProcessBus
from pswamp_core.datagateway import DataGateway, Player, gateway_from_env
from pswamp_core.messages import PlayerStatus, PmuFrame, PmuHeader
from pswamp_core.modules import Module
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

#: A provider spec for the broker the module is reached through, e.g.
#: ``bus:pswamp_core.datagateway.clients.kafka:KafkaClient`` (with
#: ``BUS_BOOTSTRAP_SERVERS`` beside it). Unset, the module runs in-process.
#: Either way the module code, the bus message and the page are the same.
BUS_CLIENTS_VARIABLE = "FREQUENCY_PEEK_BUS_CLIENTS"

#: The one pipeline every viewer shares. A live stream is keyed by the stream.
PIPELINE_KEY = "live"
MAX_PIPELINES = 1
IDLE_EVICT_SECONDS = 300.0


# --- the pipeline, one per process ------------------------------------------------


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


def bus_gateway() -> DataGateway | None:
    """The broker's gateway, when the environment names one; ``None`` otherwise."""
    if not os.environ.get(BUS_CLIENTS_VARIABLE, "").strip():
        return None
    return gateway_from_env(None, variable=BUS_CLIENTS_VARIABLE)


def frequency_modules(bus: DataGateway | None) -> list[Module]:
    """The pipeline's module list: the module itself, or the bridge that
    carries this pipeline's frames to it and its results back."""
    if bus is None:
        return [FrequencyModule()]
    return [
        TopicBridge(
            bus,
            outbound=[PmuFrame],
            prime=[PmuHeader],
            inbound=[FrequencyResult],
            name="frequency@bus",
        )
    ]


async def build_pipeline(key: str) -> LivePipeline:
    """The shared pipeline: the configured providers, a bus, a live player and
    the frequency module -- in this process or behind the broker. Called by
    the registry, never directly."""
    gateway: DataGateway = gateway_from_env(DEFAULT_DATA_CLIENTS, variable=DATA_CLIENTS_VARIABLE)
    bus = InProcessBus()
    player = Player(gateway, bus, model=PmuFrame, autoplay=True, loop=True)
    modules = frequency_modules(bus_gateway())
    logger.info("pipeline %s: frequency module runs as %s", key, modules[0].name)
    return LivePipeline(key, gateway, bus, player, modules)


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
    """Accept one socket and hold the shared pipeline for as long as it lives.

    Yields ``None`` when the connection was refused: no usable client id is
    closed *before* accepting (1008); a pipeline that fails to build is closed
    with 1011 (the registry's capacity refusal, 1013, cannot happen with one
    key, but is mapped for the day the key changes).

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
        pipeline = await REGISTRY.acquire(PIPELINE_KEY)
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
        REGISTRY.release(PIPELINE_KEY)


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_pipeline(ws) as pipeline:
        if pipeline is None:
            return
        client_id = ws.query_params.get("client_id", "?")
        logger.info("client %s: joined pipeline %s (%d watching)", client_id, pipeline.key, REGISTRY.watchers(pipeline.key))
        # Open the queue before the opening message, so a result published in
        # between is not lost; the builder reads the bus's newest, so it
        # ignores which event woke it.
        with event_queue(pipeline.bus, FrequencyResult) as updates:
            await serve_updates(ws, updates, lambda _event: state_message(pipeline))
        logger.info("client %s: left pipeline %s", client_id, pipeline.key)
