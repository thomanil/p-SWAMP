# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Time Series Explorer's backend: two ways to ask for a range of a time series.

The example beside the streamer for a provider that answers *queries* -- in the
deployments this is written for, the ``RemoteDataClient``, which sends each
query to a deployment's own data service and reads the answers off a Kafka
topic (see its docstring in ``pswamp_core.datagateway.clients.remote_data``).
The page is named for what is queried on the other end: a time series, kept in
whatever store the deployment runs, which this page never sees.

One pipeline per client over the configured providers, and three commands,
each a ``POST`` here that becomes a ``Command`` on the client's bus::

    providers ── DataGateway ── Player ──▶ bus ──▶ this socket        (a) play a range
                     ▲                      │
                     └── RowCountModule ◀───┘                         (b) count a range
    POST /playback/play-range ── Command(target=player, replay to/end/play) ──▶ Player
    POST /playback/stop       ── Command(target=player, stop)              ──▶ Player
    POST /count               ── Command(target="row-count", count)        ──▶ RowCountModule

(a) is the *stream* case: the player opens ``gateway.consume(PmuFrame, start,
end)``, paces it, and ends paused at ``end`` (the bounded replay the player
grew for this page; ``PlayerStatus.range_end`` says so). (b) is the *batch*
case: the module opens the same kind of stream itself, unpaced, and publishes
one ``RowCountResult`` with the command's ``request_id``. Both reach the same
provider through the same gateway, which is the point: a data service implements
one contract and gets both.

**Which providers.** ``TIME_SERIES_EXPLORER_DATA_CLIENTS`` names them, in the
usual ``name:module:Class`` form; unset, the page runs over the streamer's
sample recording (``DEFAULT_DATA_CLIENTS``), which is what CI's bare
``docker run`` exercises. Compose and the local k8s manifest set it to the
Remote Data Client over the stub service, with the ``REMOTE_DATA_*`` block beside it.

**Failure reaches the page two ways.** A provider that fails mid-replay ends the
stream paused with ``PlayerStatus.error`` set; a count that fails carries
``error`` in its result. Both are *state*, shown inline. The same failures are
also ``ErrorEvent``s on the bus, which the layout's error tray shows on every
page (``src/errors/``). A range outside the coverage is refused here with a 409
before anything is published.

Per client: one pipeline, built by ``REGISTRY`` on first connect, capped and
idle-evicted like the streamer's. server.py mounts this ``router`` under
/api/time-series-explorer; nothing here knows about that prefix.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Literal

from fastapi import APIRouter, FastAPI, HTTPException, WebSocket
from pydantic import BaseModel, Field, model_validator
from shared import (
    ClientId,
    CommandAck,
    ErrorForwarderModule,
    get_logger,
    read_client_id,
    send_state,
    wait_for_disconnect,
)

from pswamp_core.bus import InProcessBus, Overflow, Subscription
from pswamp_core.datagateway import DataGateway, Player, gateway_from_env
from pswamp_core.messages import Command, PlayerStatus, PmuFrame, StreamChanged
from pswamp_core.pipeline import CapacityError, Pipeline, PipelineRegistry
from pswamp_core.util.time import ensure_utc

from .row_count_module import RowCountModule, RowCountResult

logger = get_logger("time-series-explorer")

SLUG = "time-series-explorer"

#: The providers a deployment gets unless TIME_SERIES_EXPLORER_DATA_CLIENTS
#: names others: the streamer's sample recording, history only. Compose and k8s
#: replace it with the Remote Data Client over the stub service.
DEFAULT_DATA_CLIENTS = "sample:pmu_test_streamer.sample_client:SampleRecordingClient"
DATA_CLIENTS_VARIABLE = "TIME_SERIES_EXPLORER_DATA_CLIENTS"

MAX_PIPELINES = 8
IDLE_EVICT_SECONDS = 300.0


# --- the pipeline, per client -------------------------------------------------


async def build_pipeline(client_id: str) -> Pipeline:
    """One client's pipeline: the configured providers, a bus, a non-looping
    player, the row-count module, and the error forwarder. Called by the
    registry, never directly."""
    gateway: DataGateway = gateway_from_env(DEFAULT_DATA_CLIENTS, variable=DATA_CLIENTS_VARIABLE)
    bus = InProcessBus()
    player = Player(gateway, bus, model=PmuFrame, loop=False)
    modules = [RowCountModule(), ErrorForwarderModule(client_id, SLUG)]
    logger.info("pipeline %s over %s", client_id, ", ".join(gateway.clients))
    return Pipeline(client_id, gateway, bus, player, modules)


REGISTRY: PipelineRegistry[Pipeline] = PipelineRegistry(
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


class TimeSeriesExplorerState(BaseModel):
    """The one message pushed on connect and on every change.

    Its parts are core messages carried as they are: the browser's types for
    ``PmuFrame`` (with its ``PmuHeader`` inside), ``PlayerStatus`` and
    ``RowCountResult`` are generated from these very classes. Errors are inside them --
    ``player.error`` and ``count.result.error`` -- because they are *state*: why
    the replay is stopped, why the count is short. The error *event* goes to
    the layout's tray, not here.
    """

    type: Literal["state"] = "state"
    player: PlayerStatus = Field(
        description="Where the replay is, its coverage, its bounded range and any error."
    )
    frame: PmuFrame | None = Field(
        description="The frame at the cursor, with its channel layout, once one has played."
    )
    count: RowCountResult | None = Field(
        description="The row-count module's latest result; null until the first count."
    )


def state_message(pipeline: Pipeline) -> TimeSeriesExplorerState:
    latest = pipeline.latest
    frame = pipeline.player.last_frame
    return TimeSeriesExplorerState(
        player=pipeline.player.status(),
        frame=frame if isinstance(frame, PmuFrame) else None,
        count=latest.get(RowCountResult) if latest else None,
    )


# --- REST commands ----------------------------------------------------------------

router = APIRouter()

_REFUSED = {409: {"description": "The range lies outside the provider's coverage, or there is none."}}


class RangeBody(BaseModel):
    """A half-open range ``[start, end)`` of the provider's timeline."""

    start: datetime = Field(description="Inclusive start, ISO 8601.")
    end: datetime = Field(description="Exclusive end, ISO 8601; must be after start.")

    @model_validator(mode="after")
    def _ordered(self) -> RangeBody:
        self.start, self.end = ensure_utc(self.start), ensure_utc(self.end)
        if self.end <= self.start:
            raise ValueError("end must be after start")
        return self


def live_pipeline(client_id: str) -> Pipeline:
    """The pipeline a command applies to, or 404 -- "you have no page open"."""
    pipeline = REGISTRY.peek(client_id)
    if pipeline is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"no live pipeline for client {client_id}; "
                "open the page (and its WebSocket) before sending commands"
            ),
        )
    return pipeline


def refusal(status: PlayerStatus, body: RangeBody | None, verb: str = "") -> str | None:
    """Why a command cannot be applied right now, or ``None`` if it can."""
    if verb == "refresh":
        return None  # the one command that is *for* a provider reporting nothing
    if status.coverage_start is None or status.coverage_end is None:
        return "the provider reports no coverage: nothing to play or count"
    if body is not None and (body.start < status.coverage_start or body.end > status.coverage_end):
        return (
            f"range [{body.start.isoformat()}, {body.end.isoformat()}) lies outside the coverage "
            f"[{status.coverage_start.isoformat()}, {status.coverage_end.isoformat()})"
        )
    return None


def publish(client_id: str, verb: str, body: RangeBody | None = None, *, target: str | None = None, **args: object) -> CommandAck:
    """Publish one command on the client's bus and acknowledge it -- or refuse
    it with a 409 when the coverage cannot take it."""
    pipeline = live_pipeline(client_id)
    reason = refusal(pipeline.player.status(), body, verb)
    if reason is not None:
        logger.info("client %s: refused %s: %s", client_id, verb, reason)
        raise HTTPException(status_code=409, detail=reason)
    command = Command(client_id=client_id, target=target, verb=verb, args=dict(args))
    pipeline.bus.publish(command)
    logger.info("client %s: %s %s (request %s)", client_id, verb, args or "", command.request_id)
    return CommandAck(applied=verb)


@router.post("/playback/play-range", operation_id="time_series_explorer_play_range", responses=_REFUSED)
async def play_range(client_id: ClientId, body: RangeBody) -> CommandAck:
    """Replay exactly ``[start, end)`` at real time, then end paused. The stream
    case: the player asks the provider for the range and paces it."""
    return publish(
        client_id, "replay", body,
        to=body.start.isoformat(), end=body.end.isoformat(), play=True,
    )


@router.post("/playback/stop", operation_id="time_series_explorer_stop", responses=_REFUSED)
async def stop(client_id: ClientId) -> CommandAck:
    """Pause the replay where it is."""
    return publish(client_id, "stop")


@router.post("/refresh", operation_id="time_series_explorer_refresh")
async def refresh(client_id: ClientId) -> CommandAck:
    """Ask the provider again what it holds. The one command a page sends when
    the provider stopped answering: if it is back, the next state carries its
    coverage and clears the error; if not, the state still says so."""
    return publish(client_id, "refresh")


@router.post("/count", operation_id="time_series_explorer_count", responses=_REFUSED)
async def count(client_id: ClientId, body: RangeBody) -> CommandAck:
    """Count the frames in ``[start, end)``. The batch case: the row-count module
    asks the provider for the range itself, unpaced, and publishes one result
    carrying this command's request id."""
    return publish(
        client_id, "count", body, target=RowCountModule.target,
        start=body.start.isoformat(), end=body.end.isoformat(),
    )


# --- websocket endpoint (downstream only) ---------------------------------------


@contextlib.asynccontextmanager
async def connected_pipeline(ws: WebSocket) -> AsyncIterator[Pipeline | None]:
    """Accept one socket and hold its client's pipeline for as long as it lives.
    Yields ``None`` when refused: 1008 before accepting for no usable client id,
    1013 after accepting at capacity, 1011 when the pipeline failed to start.
    (The streamer's handshake, pointed at this registry.)"""
    client_id = read_client_id(ws)
    if client_id is None:
        await ws.close(code=1008)
        yield None
        return

    await ws.accept()
    try:
        pipeline = await REGISTRY.acquire(client_id)
    except CapacityError:
        logger.warning("refused client %s: all %s pipelines in use", client_id, REGISTRY.max_pipelines)
        await ws.close(code=1013)
        yield None
        return
    except Exception:
        logger.exception("failed to start pipeline for client %s", client_id)
        await ws.close(code=1011)
        yield None
        return

    try:
        yield pipeline
    finally:
        REGISTRY.release(client_id)


def subscribe_updates(pipeline: Pipeline) -> Subscription:
    """What this page shows, off the client's bus. Opened *before* the first
    send, so nothing published in between is lost."""
    return pipeline.bus.subscribe(
        PmuFrame, PlayerStatus, RowCountResult, StreamChanged,
        overflow=Overflow.DROP_OLDEST, maxsize=64,
    )


async def serve_stream(
    ws: WebSocket, pipeline: Pipeline, updates: Subscription
) -> None:
    """Push the state on every change until the client disconnects, coalescing
    a backlog into one message built from the latest state."""

    async def push() -> None:
        async for _ in updates:
            while updates.get_nowait() is not None:
                pass
            await send_state(ws, state_message(pipeline))

    pusher = asyncio.create_task(push())
    try:
        await wait_for_disconnect(ws)
    finally:
        pusher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await pusher


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_pipeline(ws) as pipeline:
        if pipeline is None:
            return
        logger.info("client %s: connected (%s live)", pipeline.key, len(REGISTRY.keys()))
        with subscribe_updates(pipeline) as updates:
            await send_state(ws, state_message(pipeline))
            await serve_stream(ws, pipeline, updates)
        logger.info("client %s: disconnected", pipeline.key)
