# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The PMU test streamer's web edge: one core pipeline per client, over REST and a socket.

Everything between the data and this module is ``pswamp_core``; the pipeline
itself is defined in ``pipeline.py``. What is left here is the edge::

    POST /playback/{play,stop,forward,back,seek,speed,live,replay} ── typed PlayerCommand ─┐
    POST /stats/reset                                              ── ResetStatsCommand ──┤
                                                           shared.dispatch_command ◀──────┘
                                                           (404 no pipeline · 409 refused)
    bus ── PmuFrame · PlayerStatus · FrameStatsResult · ErrorEvent ──▶ one PmuStreamState ──▶ /ws

Commands go up over REST and state comes down over the socket, as everywhere
in this backend (doc/the-client-server-api.md). A command's reply is only an
acknowledgement; its effect arrives on the socket like any other change.

server.py mounts this ``router`` under /api/pmu-test-streamer.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Literal

from fastapi import APIRouter, FastAPI, WebSocket
from pydantic import BaseModel, Field
from shared import (
    COMMAND_RESPONSES,
    ClientId,
    CommandAck,
    dispatch_command,
    get_logger,
    read_client_id,
    send_state,
    wait_for_disconnect,
)

from pswamp_core.bus import Overflow, Subscription
from pswamp_core.messages import (
    Command,
    ErrorEvent,
    GoLiveCommand,
    PauseCommand,
    PlayCommand,
    PlayerStatus,
    PmuFrame,
    ReplayCommand,
    SeekCommand,
    SpeedCommand,
    StepCommand,
    StreamChanged,
)
from pswamp_core.pipeline import CapacityError, Pipeline, PipelineRegistry

from .pipeline import IDLE_EVICT_SECONDS, MAX_PIPELINES, build_pipeline, close_module_transport
from .average_module import AverageRangeCommand, RangeAverageResult
from .stats_module import FrameStatsResult, ResetStatsCommand

logger = get_logger("pmu")


# --- one pipeline per client ------------------------------------------------------


def build_client_pipeline(client_id: str) -> Pipeline:
    """The streamer's pipeline, plus the edge's one addition: its errors in the log."""
    pipeline = build_pipeline(client_id)
    pipeline.bus.add_listener(ErrorEvent, lambda error: _log_error(client_id, error))
    return pipeline


def _log_error(client_id: str, error: ErrorEvent) -> None:
    logger.warning(
        "client %s: %s: %s (%s)%s", client_id, error.source, error.message, error.detail,
        f" [request {error.request_id}]" if error.request_id else "",
    )


REGISTRY: PipelineRegistry[Pipeline] = PipelineRegistry(
    build_client_pipeline, max_pipelines=MAX_PIPELINES, idle_seconds=IDLE_EVICT_SECONDS
)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """Bind the registry to the loop while the server is up; drain it on shutdown."""
    REGISTRY.bind(asyncio.get_running_loop())
    try:
        yield
    finally:
        await REGISTRY.stop_all()
        REGISTRY.bind(None)
        await close_module_transport()


# --- the socket message ---------------------------------------------------------------


class PmuStreamState(BaseModel):
    """The one message pushed on connect and on every change.

    Its parts are core messages carried as they are, so the browser's types for
    them are generated from these very classes. The channel layout comes with
    every frame, as ``frame.header``.
    """

    type: Literal["state"] = "state"
    frame: PmuFrame | None = Field(
        description="The frame at the cursor, with its channel layout, once one has played."
    )
    player: PlayerStatus = Field(description="Where the replay is and which controls apply.")
    stats: FrameStatsResult | None = Field(description="The stats module's latest result.")
    error: ErrorEvent | None = Field(description="The pipeline's latest operational error, if any.")
    average: RangeAverageResult | None = Field(description="The batch average's latest answer.")
    frame_index: int | None = Field(description="0-based position of the cursor in the recording.")
    frame_count: int | None = Field(description="How many frames the recording holds.")


def _interval(frame: PmuFrame | None, status: PlayerStatus) -> float | None:
    """Seconds between frames: the layout's declared rate first, since it is exact."""
    return (1.0 / frame.header.data_rate) if frame else status.frame_interval_s


def _position(status: PlayerStatus, frame: PmuFrame | None) -> tuple[int | None, int | None]:
    """The cursor as an index into the recording -- meaningless while live."""
    if status.mode == "live":
        return None, None
    interval = _interval(frame, status)
    if not interval or status.coverage_start is None:
        return None, None
    count = None
    if status.coverage_end is not None:
        count = round((status.coverage_end - status.coverage_start).total_seconds() / interval)
    index = None
    if status.cursor is not None:
        index = round((status.cursor - status.coverage_start).total_seconds() / interval)
    return index, count


def state_message(pipeline: Pipeline) -> PmuStreamState:
    """The current state: the player's live status, the frame it last played on the
    open stream, and the module's result for that frame."""
    latest = pipeline.latest
    # The player's status, not the last *published* one: the cursor moves with
    # every frame, and only control changes publish a PlayerStatus.
    status = pipeline.player.status()
    # The player's last frame, not the bus's newest: the player forgets it on a
    # stream switch, so a page never shows the live feed's values under a
    # "recorded" badge.
    frame = pipeline.player.last_frame
    if not isinstance(frame, PmuFrame):
        frame = None
    stats = latest.get(FrameStatsResult) if latest else None
    if frame is None or not _current(stats, frame, status):
        stats = None
    index, count = _position(status, frame)
    return PmuStreamState(
        frame=frame,
        player=status,
        stats=stats,
        error=latest.get(ErrorEvent) if latest else None,
        average=latest.get(RangeAverageResult) if latest else None,
        frame_index=index,
        frame_count=count,
    )


def _current(stats: FrameStatsResult | None, frame: PmuFrame, status: PlayerStatus) -> bool:
    """Whether ``stats`` belongs with ``frame``: for it, an answer to a command
    (the reset), or for the frame just before it on the same stream. The grace
    frame is because a worker's result lands a few ms after its frame, and this
    message is pushed for the frame first; without it the stats line would
    blank and refill twenty times a second."""
    if stats is None:
        return False
    if stats.timestamp == frame.timestamp or stats.request_id is not None:
        return True
    if stats.mRID not in (None, frame.mRID):
        return False
    interval = _interval(frame, status)
    if not interval:
        return False
    behind = (frame.timestamp - stats.timestamp).total_seconds()
    return 0 < behind <= interval * 1.5


# --- REST commands ----------------------------------------------------------------------
#
# One POST per operation, each building one typed command. Routing, the 404 and
# the 409 are shared.dispatch_command's; the effect arrives on the socket.

router = APIRouter()


def dispatch(command: Command) -> CommandAck:
    """Dispatch one command into its client's pipeline."""
    return dispatch_command(REGISTRY.peek(command.client_id or ""), command, logger)


@router.post("/playback/play", operation_id="pmu_test_streamer_play", responses=COMMAND_RESPONSES)
async def play(client_id: ClientId) -> CommandAck:
    """Start (or resume) this client's replay."""
    return dispatch(PlayCommand(client_id=client_id))


@router.post("/playback/stop", operation_id="pmu_test_streamer_stop", responses=COMMAND_RESPONSES)
async def stop(client_id: ClientId) -> CommandAck:
    """Pause this client's replay where it is."""
    return dispatch(PauseCommand(client_id=client_id))


@router.post("/playback/forward", operation_id="pmu_test_streamer_forward", responses=COMMAND_RESPONSES)
async def forward(client_id: ClientId) -> CommandAck:
    """Play one frame, independently of the play/pause state."""
    return dispatch(StepCommand(client_id=client_id, n=1))


@router.post("/playback/back", operation_id="pmu_test_streamer_back", responses=COMMAND_RESPONSES)
async def back(client_id: ClientId) -> CommandAck:
    """Step one frame back: the player reopens its stream one interval earlier."""
    return dispatch(StepCommand(client_id=client_id, n=-1))


class SeekBody(BaseModel):
    offset_s: float = Field(ge=0, description="Seconds from the start of the recording.")


@router.post("/playback/seek", operation_id="pmu_test_streamer_seek", responses=COMMAND_RESPONSES)
async def seek(client_id: ClientId, body: SeekBody) -> CommandAck:
    """Jump the replay to an offset into the recording. 409 while live."""
    return dispatch(SeekCommand(client_id=client_id, offset_s=body.offset_s))


class SpeedBody(BaseModel):
    speed: float = Field(gt=0, le=10, description="Replay speed multiplier; 1 is real time.")


@router.post("/playback/speed", operation_id="pmu_test_streamer_speed", responses=COMMAND_RESPONSES)
async def speed(client_id: ClientId, body: SpeedBody) -> CommandAck:
    """Change the replay speed. 409 while live."""
    return dispatch(SpeedCommand(client_id=client_id, speed=body.speed))


@router.post("/playback/live", operation_id="pmu_test_streamer_live", responses=COMMAND_RESPONSES)
async def live(client_id: ClientId) -> CommandAck:
    """Switch this client to the live feed. 409 when no live source is configured."""
    return dispatch(GoLiveCommand(client_id=client_id))


@router.post("/playback/replay", operation_id="pmu_test_streamer_replay", responses=COMMAND_RESPONSES)
async def replay(client_id: ClientId) -> CommandAck:
    """Switch this client back to the recording, paused at its start."""
    return dispatch(ReplayCommand(client_id=client_id))


class RangeBody(BaseModel):
    start_offset_s: float = Field(ge=0, description="Where the chunk starts, in seconds from the start of the recording.")
    end_offset_s: float = Field(gt=0, description="Where it ends (exclusive), in seconds from the start of the recording.")


@router.post("/playback/range", operation_id="pmu_test_streamer_play_range", responses=COMMAND_RESPONSES)
async def play_range(client_id: ClientId, body: RangeBody) -> CommandAck:
    """Play exactly one chunk of the recording, ``[start, end)``, paced, and stop
    there: the player's bounded replay. 409 when the chunk is empty or starts
    outside the recording."""
    return dispatch(
        ReplayCommand(
            client_id=client_id,
            offset_s=body.start_offset_s,
            end_offset_s=body.end_offset_s,
            play=True,
        )
    )


@router.post("/stats/average", operation_id="pmu_test_streamer_average", responses=COMMAND_RESPONSES)
async def average(client_id: ClientId, body: RangeBody) -> CommandAck:
    """Average the frequencies over one chunk -- a *batch* query: the module
    reads the range from the gateway itself, unpaced, and answers once. 409 when
    the range is empty."""
    return dispatch(
        AverageRangeCommand(
            client_id=client_id, start_offset_s=body.start_offset_s, end_offset_s=body.end_offset_s
        )
    )


@router.post("/stats/reset", operation_id="pmu_test_streamer_reset_stats", responses=COMMAND_RESPONSES)
async def reset_stats(client_id: ClientId) -> CommandAck:
    """Zero the stats module's running count and peak -- a command to a *module*,
    routed by its class like the player's. 409 when there is nothing to reset
    (in-process; a module in a worker refuses there, as an ErrorEvent)."""
    return dispatch(ResetStatsCommand(client_id=client_id))


# --- websocket endpoint (downstream only) ------------------------------------------------


@contextlib.asynccontextmanager
async def connected_pipeline(ws: WebSocket) -> AsyncIterator[Pipeline | None]:
    """Accept one socket and hold its client's pipeline for as long as it lives.

    Yields ``None`` when refused: no usable client id is closed *before*
    accepting (1008); at capacity the socket is accepted first and closed with
    1013, because a close code only reaches the browser on an established
    connection, and the web client treats 1013 as terminal.
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


def subscribe_updates(pipeline: Pipeline) -> Subscription:
    """What this page shows, off the client's bus. Opened *before* the first
    send, so nothing published in between is lost."""
    return pipeline.bus.subscribe(
        PmuFrame, PlayerStatus, FrameStatsResult, RangeAverageResult, StreamChanged, ErrorEvent,
        overflow=Overflow.DROP_OLDEST, maxsize=64,
    )


async def serve_stream(ws: WebSocket, pipeline: Pipeline, updates: Subscription) -> None:
    """Push the state on every change until the client disconnects.

    Coalesces: when the reader wakes it drains whatever else is pending and
    sends **one** message built from the latest of each, so a socket that falls
    behind sees the newest state rather than a backlog.
    """

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
        logger.info("client %s: connected (%s live)", pipeline.key, REGISTRY.live)
        with subscribe_updates(pipeline) as updates:
            await send_state(ws, state_message(pipeline))
            await serve_stream(ws, pipeline, updates)
        logger.info("client %s: disconnected", pipeline.key)
