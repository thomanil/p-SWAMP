# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Islanding stream app's backend: a heavy module over the core, under load.

p-SWAMP's islanding detector (``islanding_module.py``) reading the Nordic 44
line-trip recording (``n44_client.py``: 44 stations, 700 channels, 50 Hz), one
pipeline per client from the family in ``family.py``::

    N44 recording ── DataGateway ── Player ──▶ topic islanding-stream.pmu.frame ──▶ IslandingModule
                                                        this socket ◀── latest ◀── topic …result ◀──┘

It exists to find out what breaks first. The replay speed is the load knob --
up to 50x, 2500 frames a second of 700 values each -- and the page shows how the
pipeline is coping: frames the player actually emitted per second, how far the
replay actually advanced per second, what the pipeline managed to publish on
the module's topic and what it had to drop. When the module (or the publisher
in front of the transport) cannot keep up, the core's keep-up reports arrive on
the layout's error tray as ``ErrorEvent``s.

The module runs wherever the transport says: in this process with the
in-memory one (the tests, CI's bare ``docker run``), in the ``module-worker``
with Kafka (compose, k8s).

Commands: play, stop, speed -- each a POST that builds one typed player command
and dispatches it into the client's pipeline (``shared.dispatch_command``). The
socket carries the result and the throughput, never the frames: at 50x the page
would otherwise be the bottleneck it is meant to watch.
"""

from __future__ import annotations

import contextlib
import time
from typing import Literal

from fastapi import APIRouter, FastAPI, WebSocket
from pydantic import BaseModel, Field
from shared import (
    COMMAND_RESPONSES,
    ClientId,
    CommandAck,
    connected_pipeline,
    dispatch_command,
    get_logger,
    push_changes,
    serve_family,
    transport,
)

from pswamp_core.messages import PauseCommand, PlayCommand, PlayerStatus, SpeedCommand
from pswamp_core.pipeline import Pipeline, PipelineRegistry

from .family import FAMILY
from .islanding_module import IslandingStreamResult

logger = get_logger("islanding-stream")

MAX_PIPELINES = 8
IDLE_EVICT_SECONDS = 300.0

#: The fastest replay the page may ask for: 50 Hz x 50 = 2500 frames a second.
MAX_SPEED = 50.0

#: How often the socket pushes throughput readings with nothing new to carry,
#: and the fastest it pushes at all.
TICK_SECONDS = 0.5
MIN_PUSH_INTERVAL = 0.1


# --- the pipeline, per client -------------------------------------------------


def build_pipeline(client_id: str) -> Pipeline:
    """One client's pipeline over the family, with a looping player that plays
    at once. Called by the registry, never directly."""
    return Pipeline(client_id, FAMILY, transport(), autoplay=True, loop=True)


REGISTRY: PipelineRegistry[Pipeline] = PipelineRegistry(
    build_pipeline, max_pipelines=MAX_PIPELINES, idle_seconds=IDLE_EVICT_SECONDS
)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """The registry bound, errors forwarded, and (in one process) the islanding
    module hosted, for as long as the server is up."""
    async with serve_family(FAMILY, REGISTRY):
        yield


# --- the socket message ---------------------------------------------------------


class Throughput(BaseModel):
    """How the pipeline is coping, from this server's side of the transport."""

    frames_per_s: float = Field(description="Frames the player emitted per wall-clock second, recently.")
    effective_speed: float | None = Field(
        description="Seconds of recording replayed per wall-clock second, recently; null while paused."
    )
    published: int = Field(description="Messages published to the module's topics.")
    publish_failed: int = Field(description="Messages the transport refused (broker down, timeout).")
    queue_dropped: int = Field(
        description=(
            "Frames this server dropped for falling behind, in the outbox in front of the transport. "
            "The module's own drops are in the result's input_dropped."
        )
    )


class IslandingStreamState(BaseModel):
    """The one message pushed on connect, on every result or player change, and
    on a slow tick for the throughput readings.

    A declared model, because this IS the downstream half of the published
    contract: api_contract.py collects it via this package's WS_MESSAGE export.
    """

    type: Literal["state"] = "state"
    player: PlayerStatus = Field(description="Where the replay is, and how fast it is asked to go.")
    offset_s: float | None = Field(description="Seconds into the recording at the cursor.")
    duration_s: float | None = Field(description="How long the recording is.")
    result: IslandingStreamResult | None = Field(
        description="The islanding module's latest result; null until the first 10 s window fills."
    )
    throughput: Throughput


class RateMeter:
    """Frames and replay time per wall-clock second, between two readings."""

    def __init__(self, pipeline: Pipeline) -> None:
        self.pipeline = pipeline
        self._at = time.monotonic()
        self._frames = pipeline.player.frames_emitted
        self._cursor = pipeline.player.status().cursor
        self.frames_per_s = 0.0
        self.effective_speed: float | None = None

    def read(self) -> None:
        now = time.monotonic()
        elapsed = now - self._at
        if elapsed < 0.2:
            return  # too short to mean anything; keep the last reading
        status = self.pipeline.player.status()
        frames = self.pipeline.player.frames_emitted
        self.frames_per_s = round((frames - self._frames) / elapsed, 1)
        if status.paused or status.cursor is None or self._cursor is None or status.cursor < self._cursor:
            self.effective_speed = None
        else:
            self.effective_speed = round((status.cursor - self._cursor).total_seconds() / elapsed, 2)
        self._at, self._frames, self._cursor = now, frames, status.cursor


def state_message(pipeline: Pipeline, meter: RateMeter) -> IslandingStreamState:
    meter.read()
    status = pipeline.player.status()
    outbox = pipeline.outbox
    offset = duration = None
    if status.coverage_start is not None:
        if status.cursor is not None:
            offset = round((status.cursor - status.coverage_start).total_seconds(), 2)
        if status.coverage_end is not None:
            duration = round((status.coverage_end - status.coverage_start).total_seconds(), 2)
    return IslandingStreamState(
        player=status,
        offset_s=offset,
        duration_s=duration,
        result=pipeline.latest.get(IslandingStreamResult),
        throughput=Throughput(
            frames_per_s=meter.frames_per_s,
            effective_speed=meter.effective_speed,
            published=outbox.published,
            publish_failed=outbox.failed,
            queue_dropped=outbox.dropped,
        ),
    )


# --- REST commands ----------------------------------------------------------------

router = APIRouter()


@router.post("/playback/play", operation_id="islanding_stream_play", responses=COMMAND_RESPONSES)
async def play(client_id: ClientId) -> CommandAck:
    """Resume this client's replay."""
    return dispatch_command(REGISTRY, PlayCommand(client_id=client_id), logger)


@router.post("/playback/stop", operation_id="islanding_stream_stop", responses=COMMAND_RESPONSES)
async def stop(client_id: ClientId) -> CommandAck:
    """Pause this client's replay where it is."""
    return dispatch_command(REGISTRY, PauseCommand(client_id=client_id), logger)


class SpeedBody(BaseModel):
    speed: float = Field(gt=0, le=MAX_SPEED, description="Replay speed multiplier; 1 is real time (50 frames/s).")


@router.post("/playback/speed", operation_id="islanding_stream_speed", responses=COMMAND_RESPONSES)
async def speed(client_id: ClientId, body: SpeedBody) -> CommandAck:
    """Change the replay speed: the load on the module and its topic."""
    return dispatch_command(REGISTRY, SpeedCommand(client_id=client_id, speed=body.speed), logger)


# --- websocket endpoint (downstream only) ---------------------------------------


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_pipeline(ws, REGISTRY) as pipeline:
        if pipeline is None:
            return
        logger.info("client %s: connected (%s live)", pipeline.key, len(REGISTRY.keys()))
        meter = RateMeter(pipeline)
        await push_changes(
            ws, pipeline, lambda: state_message(pipeline, meter), min_interval=MIN_PUSH_INTERVAL, tick=TICK_SECONDS
        )
        logger.info("client %s: disconnected", pipeline.key)
