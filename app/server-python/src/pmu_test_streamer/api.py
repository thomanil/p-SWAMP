# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The PMU test streamer's web edge: a pipeline per source, recordings per client, live shared.

Everything between the data and this module is ``pswamp_core``; the pipelines
themselves are defined in ``pipeline.py``, one registry per source::

    source  registry key                          what it is
    local   "local-<client id>"                   each visitor's own replay of the image's recording
    remote  "remote-<client id>"                  the same, from a remote data service over REST
    live    "live" (the stream name)              one pipeline for every viewer; its modules run once

What is per client is the **source**: which pipeline the client watches.
``POST /source`` switches it and the client's sockets follow; switching to a
recording restarts it at the beginning, paused. The edge otherwise::

    POST /playback/{play,stop,forward,back,seek,speed} ── typed PlayerCommand ─┐
    POST /stats/reset                                  ── ResetStatsCommand ──┤  to the client's recording;
                                                shared.dispatch_command ◀─────┘  409 on the shared live stream
    bus ── PmuFrame · PlayerStatus · FrameStatsResult · ErrorEvent ──▶ one PmuStreamState ──▶ /ws

Commands go up over REST and state comes down over the socket, as everywhere
in this backend (doc/the-client-server-api.md). A command's reply is only an
acknowledgement; its effect arrives on the socket like any other change.

server.py mounts this ``router`` under /api/pmu-test-streamer.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Literal

from fastapi import APIRouter, FastAPI, HTTPException, WebSocket
from pydantic import BaseModel, Field
from shared import (
    COMMAND_RESPONSES,
    ClientId,
    CommandAck,
    SessionRegistry,
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

from .pipeline import (
    IDLE_EVICT_SECONDS,
    LIVE_STREAM,
    MAX_PIPELINES,
    SOURCES,
    Source,
    available,
    build_pipeline,
    close_module_transport,
    pipeline_key,
)
from .stats_module import FrameStatsResult, ResetStatsCommand

logger = get_logger("pmu")


# --- one registry per source ----------------------------------------------------------------


def _factory(source: Source):
    """The source's pipeline definition, plus the edge's one addition: its errors in the log."""

    def build(key: str) -> Pipeline:
        pipeline = build_pipeline(key, source)

        def log(error: ErrorEvent) -> None:
            logger.warning(
                "pipeline %s: %s: %s (%s)%s", key, error.source, error.message, error.detail,
                f" [request {error.request_id}]" if error.request_id else "",
            )

        pipeline.bus.add_listener(ErrorEvent, log)
        return pipeline

    return build


#: A recording's registry holds one pipeline per client; the live one holds one, full stop.
REGISTRIES: dict[Source, PipelineRegistry[Pipeline]] = {
    source: PipelineRegistry(
        _factory(source),
        max_pipelines=1 if source == "live" else MAX_PIPELINES,
        idle_seconds=IDLE_EVICT_SECONDS,
    )
    for source in SOURCES
}

#: Which source each client watches; ``local`` until it asks for another.
#: A short string per client id, never evicted: a bounded leak, as the reference app's.
CLIENT_SOURCES: dict[str, Source] = {}
#: Each open socket's wake-up, per client: a source switch has to reach them all.
SOCKETS: SessionRegistry[asyncio.Event] = SessionRegistry()


def source_of(client_id: str) -> Source:
    return CLIENT_SOURCES.get(client_id, "local")


def pipeline_of(client_id: str, source: Source | None = None) -> Pipeline | None:
    """The client's pipeline for ``source`` (its current one by default), if it has one."""
    source = source or source_of(client_id)
    return REGISTRIES[source].peek(pipeline_key(source, client_id))


def sources_available() -> list[Source]:
    return [source for source in SOURCES if available(source)]


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """Bind the registries to the loop while the server is up; drain them on shutdown."""
    for registry in REGISTRIES.values():
        registry.bind(asyncio.get_running_loop())
    try:
        yield
    finally:
        for registry in REGISTRIES.values():
            await registry.stop_all()
            registry.bind(None)
        await close_module_transport()


# --- the socket message ---------------------------------------------------------------


class PmuStreamState(BaseModel):
    """The one message pushed on connect and on every change.

    Its parts are core messages carried as they are, so the browser's types for
    them are generated from these very classes. The channel layout comes with
    every frame, as ``frame.header``.
    """

    type: Literal["state"] = "state"
    source: Source = Field(description="Which source this client is watching.")
    sources_available: list[Source] = Field(description="The sources configured in this deployment.")
    live_viewers: int = Field(description="How many sockets are watching the shared live stream.")
    frame: PmuFrame | None = Field(
        description="The frame at the cursor, with its channel layout, once one has played."
    )
    player: PlayerStatus = Field(description="Where the replay is and which controls apply.")
    stats: FrameStatsResult | None = Field(description="The stats module's latest result.")
    error: ErrorEvent | None = Field(description="The pipeline's latest operational error, if any.")
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


def state_message(
    pipeline: Pipeline, source: Source = "local", sources_available: list[Source] | None = None
) -> PmuStreamState:
    """The current state of the pipeline a client watches: its player's status,
    the frame it last played on the open stream, and the module's result for it."""
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
        source=source,
        sources_available=[source] if sources_available is None else sources_available,
        live_viewers=REGISTRIES["live"].watchers(LIVE_STREAM),
        frame=frame,
        player=status,
        stats=stats,
        error=latest.get(ErrorEvent) if latest else None,
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
# One POST per operation, each building one typed command for the client's own
# recording. Routing, the 404 and the 409 are shared.dispatch_command's; the
# effect arrives on the socket. The live stream takes no commands from a
# viewer: it is not one viewer's to pause, seek or reset.

router = APIRouter()


def dispatch(command: Command) -> CommandAck:
    """Dispatch one command into the client's recording; 409 on the shared live stream."""
    client_id = command.client_id or ""
    if source_of(client_id) == "live":
        raise HTTPException(
            status_code=409,
            detail=f"{command.name} does not apply to the shared live stream; switch to a recording first",
        )
    return dispatch_command(pipeline_of(client_id), command, logger)


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


@router.post("/stats/reset", operation_id="pmu_test_streamer_reset_stats", responses=COMMAND_RESPONSES)
async def reset_stats(client_id: ClientId) -> CommandAck:
    """Zero the stats module's running count and peak -- a command to a *module*,
    routed by its class like the player's. 409 when there is nothing to reset
    (in-process; a module in a worker refuses there, as an ErrorEvent)."""
    return dispatch(ResetStatsCommand(client_id=client_id))


class SourceBody(BaseModel):
    source: Source = Field(description="The source to watch.")


@router.post("/source", operation_id="pmu_test_streamer_source", responses=COMMAND_RESPONSES)
async def choose_source(client_id: ClientId, body: SourceBody) -> CommandAck:
    """Switch which source this client watches; its sockets follow. A recording
    restarts at its beginning, paused; the one left behind pauses. 409 when the
    source is not configured.

    Not a command to a player as such: the live pipeline runs for every viewer
    whether this client watches or not. What changes is which pipeline this
    client's sockets follow."""
    if not SOCKETS.of(client_id):
        raise HTTPException(status_code=404, detail=f"client {client_id} has no socket open; open the page first")
    if body.source not in sources_available():
        raise HTTPException(status_code=409, detail=f"no {body.source} source is configured")
    left = source_of(client_id)
    if left != "live" and (recording := pipeline_of(client_id, left)) is not None and not recording.player.paused:
        with contextlib.suppress(Exception):
            recording.dispatch(PauseCommand(client_id=client_id))
    if body.source != "live" and (recording := pipeline_of(client_id, body.source)) is not None:
        recording.dispatch(ReplayCommand(client_id=client_id))  # from the beginning, paused
    CLIENT_SOURCES[client_id] = body.source
    for wake in SOCKETS.of(client_id):
        wake.set()
    logger.info("client %s: source %s (%s watching live)", client_id, body.source, REGISTRIES["live"].watchers(LIVE_STREAM))
    return CommandAck(applied=f"source.{body.source}")


# --- websocket endpoint (downstream only) ------------------------------------------------


def subscribe_updates(pipeline: Pipeline) -> Subscription:
    """What this page shows, off the pipeline's bus. Opened *before* the first
    send, so nothing published in between is lost."""
    return pipeline.bus.subscribe(
        PmuFrame, PlayerStatus, FrameStatsResult, StreamChanged, ErrorEvent,
        overflow=Overflow.DROP_OLDEST, maxsize=64,
    )


async def push_until_switched(
    ws: WebSocket, pipeline: Pipeline, source: Source, available_now: list[Source],
    updates: Subscription, switched: asyncio.Event,
) -> None:
    """Push the state on every change until the client's source switches.

    Coalesces: when the reader wakes it drains whatever else is pending and
    sends **one** message built from the latest of each, so a socket that falls
    behind sees the newest state rather than a backlog.
    """
    while not switched.is_set():
        update = asyncio.ensure_future(updates.get())
        wait = asyncio.ensure_future(switched.wait())
        try:
            await asyncio.wait({update, wait}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (update, wait):
                task.cancel()
        if switched.is_set() or not update.done() or update.cancelled():
            continue
        while updates.get_nowait() is not None:
            pass
        await send_state(ws, state_message(pipeline, source, available_now))


async def serve_client(ws: WebSocket, client_id: str) -> None:
    """Follow the client's source: hold that pipeline and push its state until
    the source switches, then the next. A pipeline is held (a watcher in its
    registry) only while the client watches it."""
    switched = asyncio.Event()
    with SOCKETS.registered(client_id, switched):
        while True:
            switched.clear()
            source = source_of(client_id)
            async with REGISTRIES[source].session(pipeline_key(source, client_id)) as pipeline:
                available_now = sources_available()
                with subscribe_updates(pipeline) as updates:
                    await send_state(ws, state_message(pipeline, source, available_now))
                    await push_until_switched(ws, pipeline, source, available_now, updates, switched)


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    """One client's socket. No usable client id is closed *before* accepting
    (1008); at capacity it is accepted first and closed with 1013, because a
    close code only reaches the browser on an established connection, and the
    web client treats 1013 as terminal."""
    client_id = read_client_id(ws)
    if client_id is None:
        await ws.close(code=1008)  # policy violation
        return
    await ws.accept()
    logger.info("client %s: connected", client_id)
    server = asyncio.create_task(serve_client(ws, client_id))
    disconnected = asyncio.create_task(wait_for_disconnect(ws))
    try:
        await asyncio.wait({server, disconnected}, return_when=asyncio.FIRST_COMPLETED)
        if server.done() and not server.cancelled() and (error := server.exception()) is not None:
            if isinstance(error, CapacityError):
                logger.warning("refused client %s: %s", client_id, error)
                await ws.close(code=1013)  # try again later
            else:
                logger.error("client %s: pipeline failed: %r", client_id, error)
                await ws.close(code=1011)  # unexpected server error
    finally:
        for task in (server, disconnected):
            task.cancel()
        for task in (server, disconnected):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
    logger.info("client %s: disconnected", client_id)
