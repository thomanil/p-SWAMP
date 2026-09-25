# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Islanding stream app's backend: a heavy module over the core, under load.

p-SWAMP's islanding detector (``islanding_module.py``) reading the Nordic 44
line-trip recording (``n44_client.py``: 44 stations, 700 channels, 50 Hz), one
pipeline per client, built by the recipe in ``doc/server-data-architecture.md``
("Adding things")::

    N44 recording ── DataGateway ── Player ──▶ bus ──▶ IslandingModule ──▶ bus ──▶ this socket
                                                   (or RemoteModule ⇄ topic ⇄ the worker)

It exists to find out what breaks first. The replay speed is the load knob --
up to 50x, 2500 frames a second of 700 values each -- and the page shows how the
pipeline is coping: frames the player actually emitted per second, how far the
replay actually advanced per second, and on the worker path what the pipeline
managed to publish and what it had to drop. When the module (or either side of
the hop) cannot keep up, the core's keep-up reports arrive on the layout's
error tray as ``ErrorEvent``s, through the ``ErrorForwarderModule`` every
pipeline here carries.

``ISLANDING_STREAM_MODULE_TRANSPORT`` moves the module into the worker
(``worker.py``) exactly as the streamer's variable does for its stats module;
unset -- the tests, CI's bare ``docker run`` -- it runs in-process.

Commands: play, stop, speed -- each a POST that builds one typed player command
and dispatches it into the client's pipeline (``shared.dispatch_command``). The socket carries the result and the throughput, never the
frames: at 50x the page would otherwise be the bottleneck it is meant to watch.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import AsyncIterator
from typing import Literal

from fastapi import APIRouter, FastAPI, WebSocket
from pydantic import BaseModel, Field
from shared import (
    COMMAND_RESPONSES,
    ClientId,
    CommandAck,
    ErrorForwarderModule,
    dispatch_command,
    get_logger,
    read_client_id,
    send_state,
    wait_for_disconnect,
)

from pswamp_core.bus import InProcessBus, Overflow, Subscription
from pswamp_core.datagateway import CimReferenceEnricher, DataGateway, Player, gateway_from_env
from pswamp_core.messages import (
    PauseCommand,
    PlayCommand,
    PlayerStatus,
    PmuFrame,
    SpeedCommand,
    StreamChanged,
)
from pswamp_core.modules import Module
from pswamp_core.pipeline import CapacityError, Pipeline, PipelineRegistry
from pswamp_core.remote import RemoteModule
from pswamp_core.transport import Transport, transport_from_env

from .islanding_module import IslandingModule, IslandingStreamResult

logger = get_logger("islanding-stream")

#: The providers a deployment gets unless ISLANDING_STREAM_DATA_CLIENTS names others.
DEFAULT_DATA_CLIENTS = "n44:islanding_stream.n44_client:N44RecordingClient"
DATA_CLIENTS_VARIABLE = "ISLANDING_STREAM_DATA_CLIENTS"

#: The placeholder ``cimReferenceId`` the gateway's stub enricher stamps on
#: every frame; ISLANDING_STREAM_CIM_REFERENCE sets another, or ``none`` for
#: no enrichment.
DEFAULT_CIM_REFERENCE = "n44-stub"
CIM_REFERENCE_VARIABLE = "ISLANDING_STREAM_CIM_REFERENCE"

#: A transport spec, e.g. ``islanding:pswamp_core.transport.kafka:KafkaTransport``
#: (with ``ISLANDING_BOOTSTRAP_SERVERS`` and ``ISLANDING_TOPIC_PREFIX`` beside
#: it): the islanding module then runs in the worker (``worker.py``). Unset, it
#: runs in this process. The worker reads the same variable, so the two sides
#: cannot be configured apart. Give the transport a name -- and so a topic
#: prefix -- of its own: another app's worker tails the unprefixed pmu.frame.
MODULE_TRANSPORT_VARIABLE = "ISLANDING_STREAM_MODULE_TRANSPORT"

MAX_PIPELINES = 8
IDLE_EVICT_SECONDS = 300.0

#: The fastest replay the page may ask for: 50 Hz x 50 = 2500 frames a second.
MAX_SPEED = 50.0

#: How often the socket pushes throughput readings with no result to carry,
#: and the fastest it pushes at all.
TICK_SECONDS = 0.5
MIN_PUSH_INTERVAL = 0.1


# --- the pipeline, per client -------------------------------------------------

#: The process's one transport to the worker; ``None`` while the module runs here.
TRANSPORT: Transport | None = None


def module_transport() -> Transport | None:
    """The transport the environment names, built once per process."""
    global TRANSPORT
    if TRANSPORT is None:
        TRANSPORT = transport_from_env(MODULE_TRANSPORT_VARIABLE)
    return TRANSPORT


class IslandingPipeline(Pipeline):
    """A pipeline that also counts the frames its player emits."""

    frames_emitted = 0

    async def start(self) -> None:
        self.bus.add_listener(PmuFrame, self._count)
        await super().start()

    def _count(self, _frame: object) -> None:
        self.frames_emitted += 1

    @property
    def islanding(self) -> Module:
        return self.modules[0]


def cim_reference_enrichers() -> list[CimReferenceEnricher]:
    """The gateway's stub CIM reference enricher, unless switched off."""
    reference = os.environ.get(CIM_REFERENCE_VARIABLE, "").strip() or DEFAULT_CIM_REFERENCE
    return [] if reference.lower() == "none" else [CimReferenceEnricher(reference)]


async def build_pipeline(client_id: str) -> IslandingPipeline:
    """One client's pipeline: the recording, a looping autoplaying player, the
    islanding module (here, or its stand-in for the worker) and the error
    forwarder. Called by the registry, never directly."""
    gateway: DataGateway = gateway_from_env(
        DEFAULT_DATA_CLIENTS,
        variable=DATA_CLIENTS_VARIABLE,
        enrichers=cim_reference_enrichers(),
    )
    bus = InProcessBus()
    player = Player(gateway, bus, model=PmuFrame, autoplay=True, loop=True)
    transport = module_transport()
    islanding: Module = (
        IslandingModule() if transport is None else RemoteModule(IslandingModule, transport, client_id)
    )
    logger.info(
        "pipeline %s: islanding runs %s", client_id, "in-process" if transport is None else "in the worker"
    )
    modules = [islanding, ErrorForwarderModule(client_id, "islanding-stream")]
    return IslandingPipeline(client_id, gateway, bus, player, modules)


REGISTRY: PipelineRegistry[IslandingPipeline] = PipelineRegistry(
    build_pipeline, max_pipelines=MAX_PIPELINES, idle_seconds=IDLE_EVICT_SECONDS
)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """Bind the registry to the loop for as long as the server is up; drain it
    on shutdown, then close the transport if one was opened."""
    global TRANSPORT
    REGISTRY.bind(asyncio.get_running_loop())
    try:
        yield
    finally:
        await REGISTRY.stop_all()
        REGISTRY.bind(None)
        transport, TRANSPORT = TRANSPORT, None
        if transport is not None:
            await transport.close()


# --- the socket message ---------------------------------------------------------


class Throughput(BaseModel):
    """How the pipeline is coping, from this server's side of the hop."""

    module_runs: Literal["in-process", "worker"] = Field(description="Where the islanding module runs.")
    frames_per_s: float = Field(description="Frames the player emitted per wall-clock second, recently.")
    effective_speed: float | None = Field(
        description="Seconds of recording replayed per wall-clock second, recently; null while paused."
    )
    published: int | None = Field(description="Frames published to the module's topic; null in-process.")
    publish_failed: int | None = Field(
        description="Frames the transport refused to publish (broker down, timeout); null in-process."
    )
    queue_dropped: int = Field(
        description=(
            "Frames this server dropped for falling behind: from the module's queue in-process, "
            "from the queue in front of the publisher when the module runs in the worker. "
            "The worker's own drops are in the result's input_dropped."
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

    def __init__(self, pipeline: IslandingPipeline) -> None:
        self.pipeline = pipeline
        self._at = time.monotonic()
        self._frames = pipeline.frames_emitted
        self._cursor = pipeline.player.status().cursor
        self.frames_per_s = 0.0
        self.effective_speed: float | None = None

    def read(self) -> None:
        now = time.monotonic()
        elapsed = now - self._at
        if elapsed < 0.2:
            return  # too short to mean anything; keep the last reading
        status = self.pipeline.player.status()
        frames = self.pipeline.frames_emitted
        self.frames_per_s = round((frames - self._frames) / elapsed, 1)
        if status.paused or status.cursor is None or self._cursor is None or status.cursor < self._cursor:
            self.effective_speed = None
        else:
            self.effective_speed = round((status.cursor - self._cursor).total_seconds() / elapsed, 2)
        self._at, self._frames, self._cursor = now, frames, status.cursor


def state_message(pipeline: IslandingPipeline, meter: RateMeter) -> IslandingStreamState:
    status = pipeline.player.status()
    latest = pipeline.latest
    module = pipeline.islanding
    remote = isinstance(module, RemoteModule)
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
        result=latest.get(IslandingStreamResult) if latest else None,
        throughput=Throughput(
            module_runs="worker" if remote else "in-process",
            frames_per_s=meter.frames_per_s,
            effective_speed=meter.effective_speed,
            published=module.published if remote else None,
            publish_failed=module.dropped if remote else None,
            queue_dropped=module.monitor.input_dropped,
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


@contextlib.asynccontextmanager
async def connected_pipeline(ws: WebSocket) -> AsyncIterator[IslandingPipeline | None]:
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


async def serve_state(ws: WebSocket, pipeline: IslandingPipeline, updates: Subscription) -> None:
    """Push on each result or player change, at most every ``MIN_PUSH_INTERVAL``,
    and every ``TICK_SECONDS`` regardless, until the client disconnects.

    Coalesces: whatever arrived while it waited becomes one message built from
    the pipeline's newest state, so a socket never falls behind a fast replay.
    """
    meter = RateMeter(pipeline)

    async def push() -> None:
        while True:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(updates.get(), timeout=TICK_SECONDS)
            while updates.get_nowait() is not None:
                pass
            meter.read()
            await send_state(ws, state_message(pipeline, meter))
            await asyncio.sleep(MIN_PUSH_INTERVAL)

    pusher = asyncio.create_task(push())
    try:
        await wait_for_disconnect(ws)
    finally:
        pusher.cancel()
        with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
            await pusher


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_pipeline(ws) as pipeline:
        if pipeline is None:
            return
        logger.info("client %s: connected (%s live)", pipeline.key, len(REGISTRY.keys()))
        # Subscribe first, then snapshot, then serve, so nothing published in
        # between is lost. Never PmuFrame: see the module docstring.
        with pipeline.bus.subscribe(
            IslandingStreamResult, PlayerStatus, StreamChanged, overflow=Overflow.LATEST_ONLY
        ) as updates:
            await send_state(ws, state_message(pipeline, RateMeter(pipeline)))
            await serve_state(ws, pipeline, updates)
        logger.info("client %s: disconnected", pipeline.key)
