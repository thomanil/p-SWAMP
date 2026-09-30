# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The Time Series Explorer's backend: two ways to ask for a range of a time series.

The example beside the streamer for a provider that answers *queries* -- in the
deployments this is written for, the ``RemoteDataClient``, which sends each
query to a deployment's own data service and reads each answer back as that
call's streamed response (see its docstring in
``pswamp_core.datagateway.clients.remote_data``).
The page is named for what is queried on the other end: a time series, kept in
whatever store the deployment runs, which this page never sees.

One pipeline per client over the configured providers (``family.py``), and
four commands, each a ``POST`` here that builds one typed command and
dispatches it into the client's pipeline, which publishes it on its class's
topic::

    providers ── DataGateway ── Player ─────────────────────────▶ this socket  (a) play a range
    providers ── DataGateway ── RowCountModule (hosted) ── topic …row.count.result ──▶ this socket  (b) count a range
    POST /playback/play-range ── ReplayCommand(start, end, play) ──▶ Player
    POST /playback/stop       ── PauseCommand                    ──▶ Player
    POST /refresh             ── RefreshCommand                  ──▶ Player
    POST /count               ── CountRangeCommand(start, end)   ──▶ RowCountModule

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
also ``ErrorEvent``s, which the layout's error tray shows on every
page (``src/errors/``). A replay the player cannot start -- no coverage, or a
start outside it -- is refused with a 409 before anything is published; a count
is not checked against the coverage, and counts what is there.

Per client: one pipeline, built by ``REGISTRY`` on first connect, capped and
idle-evicted like the streamer's. server.py mounts this ``router`` under
/api/time-series-explorer; nothing here knows about that prefix.
"""

from __future__ import annotations

import contextlib
from datetime import datetime
from typing import Literal

from fastapi import APIRouter, FastAPI, WebSocket
from pydantic import BaseModel, Field, model_validator
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

from pswamp_core.messages import (
    PauseCommand,
    PlayerStatus,
    PmuFrame,
    RefreshCommand,
    ReplayCommand,
)
from pswamp_core.pipeline import Pipeline, PipelineRegistry
from pswamp_core.util.time import ensure_utc

from .family import FAMILY
from .row_count_module import CountRangeCommand, RowCountResult

logger = get_logger("time-series-explorer")

MAX_PIPELINES = 8
IDLE_EVICT_SECONDS = 300.0


# --- the pipeline, per client -------------------------------------------------


def build_pipeline(client_id: str) -> Pipeline:
    """One client's pipeline over the family, with a non-looping player.
    Called by the registry, never directly."""
    return Pipeline(client_id, FAMILY, transport(), loop=False)


REGISTRY: PipelineRegistry[Pipeline] = PipelineRegistry(
    build_pipeline, max_pipelines=MAX_PIPELINES, idle_seconds=IDLE_EVICT_SECONDS
)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """The registry bound, errors forwarded, and (in one process) the row-count
    module hosted, for as long as the server is up."""
    async with serve_family(FAMILY, REGISTRY):
        yield


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
    frame = pipeline.player.last_frame
    return TimeSeriesExplorerState(
        player=pipeline.player.status(),
        frame=frame if isinstance(frame, PmuFrame) else None,
        count=pipeline.latest.get(RowCountResult),
    )


# --- REST commands ----------------------------------------------------------------

router = APIRouter()


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


@router.post("/playback/play-range", operation_id="time_series_explorer_play_range", responses=COMMAND_RESPONSES)
async def play_range(client_id: ClientId, body: RangeBody) -> CommandAck:
    """Replay exactly ``[start, end)`` at real time, then end paused. The stream
    case: the player asks the provider for the range and paces it."""
    command = ReplayCommand(client_id=client_id, start=body.start, end=body.end, play=True)
    return dispatch_command(REGISTRY, command, logger)


@router.post("/playback/stop", operation_id="time_series_explorer_stop", responses=COMMAND_RESPONSES)
async def stop(client_id: ClientId) -> CommandAck:
    """Pause the replay where it is."""
    return dispatch_command(REGISTRY, PauseCommand(client_id=client_id), logger)


@router.post("/refresh", operation_id="time_series_explorer_refresh", responses=COMMAND_RESPONSES)
async def refresh(client_id: ClientId) -> CommandAck:
    """Ask the provider again what it holds. The one command a page sends when
    the provider stopped answering: if it is back, the next state carries its
    coverage and clears the error; if not, the state still says so."""
    return dispatch_command(REGISTRY, RefreshCommand(client_id=client_id), logger)


@router.post("/count", operation_id="time_series_explorer_count", responses=COMMAND_RESPONSES)
async def count(client_id: ClientId, body: RangeBody) -> CommandAck:
    """Count the frames in ``[start, end)``. The batch case: the row-count module
    asks the provider for the range itself, unpaced, and publishes one result
    carrying this command's request id. Accepted as it is: the module checks
    it where it runs."""
    command = CountRangeCommand(client_id=client_id, start=body.start, end=body.end)
    return dispatch_command(REGISTRY, command, logger)


# --- websocket endpoint (downstream only) ---------------------------------------


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_pipeline(ws, REGISTRY) as pipeline:
        if pipeline is None:
            return
        logger.info("client %s: connected (%s live)", pipeline.key, len(REGISTRY.keys()))
        await push_changes(ws, pipeline, lambda: state_message(pipeline))
        logger.info("client %s: disconnected", pipeline.key)
