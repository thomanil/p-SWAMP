# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The PMU test streamer's backend: the web edge over one core pipeline per client.

This package is the thin slice of the target data architecture (see
``doc/server-data-architecture.md``). It owns nothing but the edge::

    sample_data.txt ──SampleRecordingClient (history)──┐
                                                        ├─ DataGateway ── Player ──▶ topic pmu-test-streamer.pmu.frame ──▶ FrameStatsModule
    the same rows, now ──LiveSyntheticClient (live)────┘        │                                                            │
                                                            this socket ◀── latest + player ◀── topic …frame.stats.result ◀──┘
    POST /playback/…  ──typed command, pipeline.dispatch──▶ topic …<command> ──▶ Player

Everything between the sources and this module is ``pswamp_core``; the pieces
this package adds are two providers (``sample_client.py``, the recording;
``live_client.py``, a synthetic live feed), one module (``stats_module.py``),
the family that names them (``family.py``), and the edge below: which state to
push down the socket, and which POSTs become which ``Command``.

Both providers are **sources of one gateway per client**, one of them active:
the recording is replayed (paced, seekable, looping), the live feed is tailed
(delivered as it arrives, no transport controls). ``POST /playback/source``
names the source to read; the page renders its source switch from
``PlayerStatus.sources`` and the transport controls or a red LIVE badge from
``PlayerStatus.mode`` and ``can_seek``.

Per client: one pipeline, built by ``REGISTRY`` on first connect and keyed by
the browser's client id, so every visitor replays from the start on their own
clock (the unit-of-isolation decision for a replay, STEP 3 §4.6). The registry
caps and idle-evicts exactly as the grid monitor's does.

**The stats module runs wherever the transport says**: in this process with
the in-memory transport (tests, CI's bare ``docker run``), in the
``module-worker`` with Kafka (compose, k8s). Nothing here changes between the
two; the player, and so every player command, stays in this process.

Commands come up over REST and state goes down over the socket, as everywhere
in this backend (AGENTS.md, doc/the-client-server-api.md). Each POST builds one
typed player command (``SeekCommand``, ``StepCommand``, ...) and hands it to
``shared.dispatch_command``: a command the player's current mode cannot apply
-- seek while live, a source that does not exist -- is a **409** before
anything is published, with the player's own reason. Otherwise the reply
acknowledges that it was *dispatched*, and the resulting status and frames
arrive on the socket like any other change.

server.py mounts this ``router`` under /api/pmu-test-streamer. Nothing here
knows about that prefix.
"""

from __future__ import annotations

import contextlib
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

from pswamp_core.messages import (
    Command,
    PauseCommand,
    PlayCommand,
    PlayerStatus,
    PmuFrame,
    SeekCommand,
    SpeedCommand,
    StepCommand,
    SwitchSourceCommand,
)
from pswamp_core.pipeline import Pipeline, PipelineRegistry

from .family import FAMILY
from .stats_module import FrameStatsResult

logger = get_logger("pmu")

MAX_PIPELINES = 8
IDLE_EVICT_SECONDS = 300.0


# --- the pipeline, per client -------------------------------------------------


def build_pipeline(client_id: str) -> Pipeline:
    """One client's pipeline over the family, with a player that loops its
    replay. Called by the registry, never directly."""
    return Pipeline(client_id, FAMILY, transport(), loop=True)


REGISTRY: PipelineRegistry[Pipeline] = PipelineRegistry(
    build_pipeline, max_pipelines=MAX_PIPELINES, idle_seconds=IDLE_EVICT_SECONDS
)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """The registry bound, errors forwarded, and (in one process) the stats
    module hosted, for as long as the server is up."""
    async with serve_family(FAMILY, REGISTRY):
        yield


# --- the socket message ---------------------------------------------------------


class PmuStreamState(BaseModel):
    """The one message pushed on connect and on every change.

    A declared model, because this IS the downstream half of the published
    contract: api_contract.py collects it via this package's WS_MESSAGE export.
    Its parts are core messages carried as they are -- the browser's types for
    ``PmuFrame`` (and the ``PmuHeader`` inside it), ``PlayerStatus`` and
    ``FrameStatsResult`` are generated from these very classes, and nothing
    renames a field on the way. The channel layout comes with every frame, as
    ``frame.header``; there is no separate header message.
    """

    type: Literal["state"] = "state"
    frame: PmuFrame | None = Field(
        description="The frame at the cursor, with its channel layout, once one has played."
    )
    player: PlayerStatus = Field(description="Where the replay is and which controls apply.")
    stats: FrameStatsResult | None = Field(description="The stats module's latest result.")
    frame_index: int | None = Field(description="0-based position of the cursor in the recording.")
    frame_count: int | None = Field(description="How many frames the recording holds.")


def _interval(frame: PmuFrame | None, status: PlayerStatus) -> float | None:
    """Seconds between frames: the layout's declared rate first, since it is
    exact, where the player's measured interval carries whatever jitter the
    last stream had."""
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
    """The current state: a live snapshot of the player, the frame it last
    played on the stream that is open, and the module's result for that frame."""
    latest = pipeline.latest
    # The player's status, not the last *published* one: the cursor moves with
    # every frame, and only control changes publish a PlayerStatus.
    status = pipeline.player.status()
    # The player's last frame, not the newest one published: it forgets it on a
    # stream switch, so a page never shows the live feed's values under a
    # "recorded, paused" badge (or the reverse). The stats result is kept only
    # if it belongs to that frame's stream and instant -- or to the instant one
    # frame before it: the module's result comes back over the transport a
    # little after the frame (measured: ~5 ms median over Kafka on a laptop),
    # and this message is pushed for the frame first. Without the
    # one-frame grace the stats line would blank and refill twenty times a
    # second. A stream switch still clears it: the last frame is reset, and a
    # result from the other stream has the other mRID and a distant timestamp.
    frame = pipeline.player.last_frame
    if not isinstance(frame, PmuFrame):
        frame = None
    stats = latest.get(FrameStatsResult)
    if frame is None or not _current(stats, frame, status):
        stats = None
    index, count = _position(status, frame)
    return PmuStreamState(
        frame=frame,
        player=status,
        stats=stats,
        frame_index=index,
        frame_count=count,
    )


def _current(stats: FrameStatsResult | None, frame: PmuFrame, status: PlayerStatus) -> bool:
    """Whether ``stats`` is for ``frame``, or for the frame just before it on
    the same stream (the one-frame grace described in ``state_message``)."""
    if stats is None:
        return False
    if stats.timestamp == frame.timestamp:
        return True
    if stats.mRID not in (None, frame.mRID):
        return False
    interval = _interval(frame, status)
    if not interval:
        return False
    behind = (frame.timestamp - stats.timestamp).total_seconds()
    return 0 < behind <= interval * 1.5


# --- logging -------------------------------------------------------------------


def roster_table(acting_id: str | None = None) -> str:
    """Every live pipeline: where its replay is and whether it is playing."""
    keys = sorted(REGISTRY.keys(), key=lambda k: int(k) if k.isdigit() else k)
    if not keys:
        return "    (no pipelines live)"
    headers = ("", "CLIENT", "SOCKETS", "CURSOR", "STATE")
    rows = [headers]
    for key in keys:
        pipeline = REGISTRY.peek(key)
        if pipeline is None:
            continue
        status = pipeline.player.status()
        if status.mode == "live":
            offset = "live"
        elif status.cursor is None or status.coverage_start is None:
            offset = "-"
        else:
            offset = f"{(status.cursor - status.coverage_start).total_seconds():.2f}s"
        rows.append(
            (
                "->" if key == acting_id else "",
                key,
                str(REGISTRY.watchers(key)),
                offset,
                "paused" if status.paused else f"playing x{status.speed:g}",
            )
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(headers))]

    def fmt(row: tuple[str, ...]) -> str:
        return "    " + "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))

    rule = "    " + "-" * (sum(widths) + 2 * (len(widths) - 1))
    return "\n".join([fmt(headers), rule, *(fmt(row) for row in rows[1:])])


def log_event(action: str, client_id: str) -> None:
    logger.info("client %s: %s\n\n%s\n", client_id, action, roster_table(client_id))


# --- REST commands ----------------------------------------------------------------
#
# One POST per operation, each building one typed player command. The routing,
# the 404 (no pipeline) and the 409 (the player's mode refuses it) are
# shared.dispatch_command's; the effect arrives on the socket.

router = APIRouter()


def dispatch(command: Command) -> CommandAck:
    """Dispatch one command into its client's pipeline, then log the roster."""
    ack = dispatch_command(REGISTRY, command, logger)
    log_event(f"{command.name} (request {command.request_id})", command.client_id or "?")
    return ack


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


class SourceBody(BaseModel):
    name: str = Field(description="The source to read: one of PlayerStatus.sources.")


@router.post("/playback/source", operation_id="pmu_test_streamer_source", responses=COMMAND_RESPONSES)
async def source(client_id: ClientId, body: SourceBody) -> CommandAck:
    """Switch this client to another source: a recording lands paused at its
    start, a live feed plays from now with no transport controls. 409 when no
    source has that name."""
    return dispatch(SwitchSourceCommand(client_id=client_id, source=body.name))


# --- websocket endpoint (downstream only) ---------------------------------------


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_pipeline(ws, REGISTRY) as pipeline:
        if pipeline is None:
            return
        log_event("connected", pipeline.key)
        await push_changes(ws, pipeline, lambda: state_message(pipeline))
    log_event("disconnected", pipeline.key)
