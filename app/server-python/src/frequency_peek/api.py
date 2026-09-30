# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Frequency peek app's backend: the web edge over one core pipeline per client.

A module and the page that shows it, built by the recipe in
``doc/server-data-architecture.md`` ("Adding things"): the family in
``family.py`` (the live feed first, the frequency module), one pipeline per
client from it, and this edge pushing the newest result down one socket::

    live feed ── DataGateway ── Player ──▶ topic frequency-peek.pmu.frame ──▶ FrequencyModule
                                   this socket ◀── latest ◀── topic …frequency.result ◀──┘

The page is **live only**: the live feed is the gateway's first-named source,
so the player tails it from the start, the frequencies on screen are stamped
now and there are no transport controls -- and so no commands. (A deployment
naming only a history source gets an autoplaying replay instead.)

Per client: one pipeline, built by ``REGISTRY`` on first connect and keyed by
the browser's client id, capped and idle-evicted like the streamer's.

server.py mounts this ``router`` under /api/frequency-peek. Nothing here knows
about that prefix.
"""

from __future__ import annotations

import contextlib
from typing import Literal

from fastapi import APIRouter, FastAPI, WebSocket
from pydantic import BaseModel, Field
from shared import connected_pipeline, get_logger, push_changes, serve_family, transport

from pswamp_core.messages import PlayerStatus
from pswamp_core.pipeline import Pipeline, PipelineRegistry

from .family import FAMILY
from .frequency_module import FrequencyResult

logger = get_logger("frequency-peek")

MAX_PIPELINES = 8
IDLE_EVICT_SECONDS = 300.0


# --- the pipeline, per client -------------------------------------------------


def build_pipeline(client_id: str) -> Pipeline:
    """One client's pipeline over the family: a player that plays at once and
    loops, should the source be a history. Called by the registry, never directly."""
    return Pipeline(client_id, FAMILY, transport(), autoplay=True, loop=True)


REGISTRY: PipelineRegistry[Pipeline] = PipelineRegistry(
    build_pipeline, max_pipelines=MAX_PIPELINES, idle_seconds=IDLE_EVICT_SECONDS
)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """The registry bound, errors forwarded, and (in one process) the frequency
    module hosted, for as long as the server is up."""
    async with serve_family(FAMILY, REGISTRY):
        yield


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
    return FrequencyPeekState(
        player=pipeline.player.status(),
        frequency=pipeline.latest.get(FrequencyResult),
    )


# --- websocket endpoint (downstream only) ---------------------------------------

router = APIRouter()


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_pipeline(ws, REGISTRY) as pipeline:
        if pipeline is None:
            return
        logger.info("client %s: connected (%s live)", pipeline.key, len(REGISTRY.keys()))
        await push_changes(ws, pipeline, lambda: state_message(pipeline))
        logger.info("client %s: disconnected", pipeline.key)
