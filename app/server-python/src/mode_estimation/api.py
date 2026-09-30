# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Mode estimation app's backend: the heavy module, under load.

p-SWAMP's N4SID mode estimation (``n4sid_module.py``) reading the Nordic 44
line-trip recording -- through the islanding stream's provider, named by its
spec string, never imported -- one pipeline per client from the family in
``family.py``::

    N44 recording ── DataGateway ── Player ──▶ topic mode-estimation.pmu.frame ──▶ N4SIDModule
                                                      this socket ◀── latest ◀── topic …result ◀──┘

Where the islanding stream's module is cheap and its load is the frames, this
one is expensive: ~200 ms of CPU per identification, once per second of data,
so the replay speed multiplies the *analysis*. How the identification runs
(on the loop, a thread pool or a process pool) is the module's own setting,
``MODE_ESTIMATION_EXECUTION``, read where the module runs -- in this process
with the in-memory transport, in the ``mode-estimation-worker`` with Kafka.

Commands: play, stop, speed. The socket carries the latest result and the
throughput, never the frames.
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
from .n4sid_module import ModeEstimationResult

logger = get_logger("mode-estimation")

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
    """The registry bound, errors forwarded, and (in one process) the N4SID
    module hosted, for as long as the server is up."""
    async with serve_family(FAMILY, REGISTRY):
        yield


# --- the socket message ---------------------------------------------------------


class ModeEstimationThroughput(BaseModel):
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


class ModeEstimationState(BaseModel):
    """The one message pushed on connect, on every result or player change, and
    on a slow tick for the throughput readings.

    A declared model, because this IS the downstream half of the published
    contract: api_contract.py collects it via this package's WS_MESSAGE export.
    """

    type: Literal["state"] = "state"
    player: PlayerStatus = Field(description="Where the replay is, and how fast it is asked to go.")
    offset_s: float | None = Field(description="Seconds into the recording at the cursor.")
    duration_s: float | None = Field(description="How long the recording is.")
    result: ModeEstimationResult | None = Field(
        description="The N4SID module's latest result; null until the first window fills."
    )
    throughput: ModeEstimationThroughput


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


def state_message(pipeline: Pipeline, meter: RateMeter) -> ModeEstimationState:
    meter.read()
    status = pipeline.player.status()
    outbox = pipeline.outbox
    offset = duration = None
    if status.coverage_start is not None:
        if status.cursor is not None:
            offset = round((status.cursor - status.coverage_start).total_seconds(), 2)
        if status.coverage_end is not None:
            duration = round((status.coverage_end - status.coverage_start).total_seconds(), 2)
    return ModeEstimationState(
        player=status,
        offset_s=offset,
        duration_s=duration,
        result=pipeline.latest.get(ModeEstimationResult),
        throughput=ModeEstimationThroughput(
            frames_per_s=meter.frames_per_s,
            effective_speed=meter.effective_speed,
            published=outbox.published,
            publish_failed=outbox.failed,
            queue_dropped=outbox.dropped,
        ),
    )


# --- REST commands ----------------------------------------------------------------

router = APIRouter()


@router.post("/playback/play", operation_id="mode_estimation_play", responses=COMMAND_RESPONSES)
async def play(client_id: ClientId) -> CommandAck:
    """Resume this client's replay."""
    return dispatch_command(REGISTRY, PlayCommand(client_id=client_id), logger)


@router.post("/playback/stop", operation_id="mode_estimation_stop", responses=COMMAND_RESPONSES)
async def stop(client_id: ClientId) -> CommandAck:
    """Pause this client's replay where it is."""
    return dispatch_command(REGISTRY, PauseCommand(client_id=client_id), logger)


class SpeedBody(BaseModel):
    speed: float = Field(gt=0, le=MAX_SPEED, description="Replay speed multiplier; 1 is real time (50 frames/s).")


@router.post("/playback/speed", operation_id="mode_estimation_speed", responses=COMMAND_RESPONSES)
async def speed(client_id: ClientId, body: SpeedBody) -> CommandAck:
    """Change the replay speed: identifications per wall-clock second, and frames on the topic."""
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
