# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The PMU test streamer's backend: the web edge over one core pipeline per client.

This package is the thin slice of the target data architecture (see
``doc/server-data-architecture.md``). Where it used to read a text file into a
list and tick an index, it now owns nothing but the edge::

    sample_data.txt ──SampleRecordingClient (history)──┐
                                                        ├─ DataGateway ──Player──▶ bus ──▶ this socket
    the same rows, now ──LiveSyntheticClient (live)────┘             ├──FrameStatsModule──▶ bus ──▶ this socket
    POST /playback/…  ──Command on the bus──▶ Player

Everything between the sources and this module is ``pswamp_core``; the pieces
this package adds are two providers (``sample_client.py``, the recording;
``live_client.py``, a synthetic live feed), one module (``stats_module.py``),
and the edge below: which messages to forward down the socket, and which POSTs
become which ``Command``.

Both providers sit in **one gateway per client**, and the player's mode is which
of the two streams is open: a *replay* of the recording (paced, seekable,
looping) or the *live* tail (delivered as it arrives, no transport controls).
``POST /playback/live`` and ``POST /playback/replay`` switch between them; the
page renders the six transport controls or a red LIVE badge from
``PlayerStatus.mode`` and ``can_seek`` / ``can_go_live``.

Per client: one pipeline, built by ``REGISTRY`` on first connect and keyed by
the browser's client id, so every visitor replays from the start on their own
clock (the unit-of-isolation decision for a replay, STEP 3 §4.6). The registry
caps and idle-evicts exactly as the grid monitor's does.

**The stats module runs here or as its own service, and nothing else changes.**
With ``PMU_TEST_STREAMER_MODULE_TRANSPORT`` unset the pipeline's module list
holds the ``FrameStatsModule`` itself. Set to a transport spec (the Kafka one
in ``pswamp_core.transport.kafka``, or the portless in-memory one), the list
holds a ``RemoteModule`` standing in for it: it publishes this pipeline's
frames (and its header) on the module's input topic under this client's key
and puts the results it tails back on this bus, where the socket below reads
them exactly as before. ``worker.py`` beside this file is the other side --
the same module code, one instance per client key, in its own container. See
"Running a module as a separate service" in ``doc/server-data-architecture.md``.
The player, and so every command, stays in this process either way.

Commands come up over REST and state goes down over the socket, as everywhere
in this backend (AGENTS.md, doc/the-client-server-api.md). A command's reply is
an acknowledgement that it was *dispatched* -- published on the client's bus,
where the player applies it on its next turn; the resulting ``PlayerStatus``
and frames arrive on the socket like any other change. A command the current
mode cannot apply -- seek while live, live without a live source -- is refused
here with a **409** before anything is published, so the ack never claims a
command the player would only log and drop. The player's own refusal remains
the backstop for the race between that check and the apply.

server.py mounts this ``router`` under /api/pmu-test-streamer. Nothing here
knows about that prefix.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Literal

from fastapi import APIRouter, FastAPI, HTTPException, WebSocket
from pydantic import BaseModel, Field
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
from pswamp_core.messages import Command, PlayerStatus, PmuFrame, PmuHeader, StreamChanged
from pswamp_core.modules import Module
from pswamp_core.pipeline import CapacityError, Pipeline, PipelineRegistry
from pswamp_core.remote import RemoteModule
from pswamp_core.transport import Transport, transport_from_env

from .stats_module import FrameStatsModule, FrameStatsResult

logger = get_logger("pmu")

#: The providers a deployment gets unless PSWAMP_DATA_CLIENTS names others: the
#: recording (history, and the header) plus the synthetic live feed.
DEFAULT_DATA_CLIENTS = (
    "sample:pmu_test_streamer.sample_client:SampleRecordingClient,"
    "live:pmu_test_streamer.live_client:LiveSyntheticClient"
)

#: A transport spec, e.g. ``kafka:pswamp_core.transport.kafka:KafkaTransport``
#: (with ``KAFKA_BOOTSTRAP_SERVERS`` beside it): the stats module then runs in
#: the worker (``worker.py``), reached over that transport. Unset, it runs in
#: this process. The worker reads the same variable, so the two sides cannot
#: be configured apart.
MODULE_TRANSPORT_VARIABLE = "PMU_TEST_STREAMER_MODULE_TRANSPORT"

MAX_PIPELINES = 8
IDLE_EVICT_SECONDS = 300.0


# --- the pipeline, per client -------------------------------------------------

#: The process's one transport to the worker, built on the first pipeline that
#: needs it and closed by ``lifespan``; ``None`` while the module runs here.
TRANSPORT: Transport | None = None


def module_transport() -> Transport | None:
    """The transport the environment names, built once per process."""
    global TRANSPORT
    if TRANSPORT is None:
        TRANSPORT = transport_from_env(MODULE_TRANSPORT_VARIABLE)
    return TRANSPORT


def stats_modules(key: str) -> list[Module]:
    """The pipeline's module list: the stats module itself, or its stand-in
    when the environment sends it to the worker."""
    transport = module_transport()
    if transport is None:
        return [FrameStatsModule()]
    return [RemoteModule(FrameStatsModule, transport, key)]


async def build_pipeline(client_id: str) -> Pipeline:
    """One client's pipeline: the configured providers, a bus, a player that
    loops its replay, and the stats module -- here, or behind the transport.
    Called by the registry, never directly."""
    gateway: DataGateway = gateway_from_env(DEFAULT_DATA_CLIENTS)
    bus = InProcessBus()
    player = Player(gateway, bus, model=PmuFrame, loop=True)
    modules = stats_modules(client_id)
    logger.info("pipeline %s: %s runs %s", client_id, modules[0].name,
                "as its own service" if isinstance(modules[0], RemoteModule) else "in-process")
    # Plus the forwarder that copies this pipeline's ErrorEvents to the layout's
    # error tray, tagged with this app's slug.
    modules.append(ErrorForwarderModule(client_id, "pmu-test-streamer"))
    return Pipeline(client_id, gateway, bus, player, modules)


REGISTRY: PipelineRegistry[Pipeline] = PipelineRegistry(
    build_pipeline, max_pipelines=MAX_PIPELINES, idle_seconds=IDLE_EVICT_SECONDS
)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """Bind the registry to the loop for as long as the server is up; drain it
    on shutdown. An idle server runs no pipeline at all."""
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


class PmuStreamState(BaseModel):
    """The one message pushed on connect and on every change.

    A declared model, because this IS the downstream half of the published
    contract: api_contract.py collects it via this package's WS_MESSAGE export.
    Its parts are core messages carried as they are -- the browser's types for
    ``PmuHeader``, ``PmuFrame``, ``PlayerStatus`` and ``FrameStatsResult`` are
    generated from these very classes, and nothing renames a field on the way.
    """

    type: Literal["state"] = "state"
    header: PmuHeader | None = Field(
        description="The channel layout. Sent on the first message only; null afterwards."
    )
    frame: PmuFrame | None = Field(description="The frame at the cursor, once one has played.")
    player: PlayerStatus = Field(description="Where the replay is and which controls apply.")
    stats: FrameStatsResult | None = Field(description="The stats module's latest result.")
    frame_index: int | None = Field(description="0-based position of the cursor in the recording.")
    frame_count: int | None = Field(description="How many frames the recording holds.")


def _position(status: PlayerStatus, header: PmuHeader | None) -> tuple[int | None, int | None]:
    """The cursor as an index into the recording -- meaningless while live."""
    if status.mode == "live":
        return None, None
    # The header's declared rate first: it is exact, where the player's measured
    # interval carries whatever jitter the last stream had.
    interval = (1.0 / header.data_rate) if header else status.frame_interval_s
    if not interval or status.coverage_start is None:
        return None, None
    count = None
    if status.coverage_end is not None:
        count = round((status.coverage_end - status.coverage_start).total_seconds() / interval)
    index = None
    if status.cursor is not None:
        index = round((status.cursor - status.coverage_start).total_seconds() / interval)
    return index, count


def state_message(pipeline: Pipeline, header: PmuHeader | None, *, first: bool) -> PmuStreamState:
    """The current state: a live snapshot of the player, the frame it last
    played on the stream that is open, and the module's result for that frame."""
    latest = pipeline.latest
    # The player's status, not the last *published* one: the cursor moves with
    # every frame, and only control changes publish a PlayerStatus.
    status = pipeline.player.status()
    # The player's last frame, not the bus's newest: the player forgets it on a
    # stream switch, so a page never shows the live feed's values under a
    # "recorded, paused" badge (or the reverse). The stats result is kept only
    # if it belongs to that frame's stream and instant -- or to the instant one
    # frame before it: when the module runs in the worker its result lands a
    # few milliseconds after the frame (measured: ~5 ms median over Kafka on a
    # laptop), and this message is pushed for the frame first. Without the
    # one-frame grace the stats line would blank and refill twenty times a
    # second. A stream switch still clears it: the last frame is reset, and a
    # result from the other stream has the other mRID and a distant timestamp.
    frame = pipeline.player.last_frame
    stats = latest.get(FrameStatsResult) if latest else None
    if not isinstance(frame, PmuFrame) or not _current(stats, frame, header, status):
        stats = None
    index, count = _position(status, header)
    return PmuStreamState(
        header=header if first else None,
        frame=frame if isinstance(frame, PmuFrame) else None,
        player=status,
        stats=stats,
        frame_index=index,
        frame_count=count,
    )


def _current(
    stats: FrameStatsResult | None, frame: PmuFrame, header: PmuHeader | None, status: PlayerStatus
) -> bool:
    """Whether ``stats`` is for ``frame``, or for the frame just before it on
    the same stream (the one-frame grace described in ``state_message``)."""
    if stats is None:
        return False
    if stats.timestamp == frame.timestamp:
        return True
    if stats.mRID not in (None, frame.mRID):
        return False
    interval = (1.0 / header.data_rate) if header else status.frame_interval_s
    if not interval:
        return False
    behind = (frame.timestamp - stats.timestamp).total_seconds()
    return 0 < behind <= interval * 1.5


async def stream_header(gateway: DataGateway) -> PmuHeader | None:
    """The stream's layout, asked of the gateway like any other range query."""
    header: PmuHeader | None = None
    async for message in gateway.consume(PmuHeader):
        header = message  # the newest wins
    return header


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
# One POST per operation. Each finds the client's live pipeline (a command never
# builds one), checks the command applies in the player's current mode (409 if
# not), publishes a Command on that pipeline's bus and acknowledges. The player
# picks the command up there; the effect arrives on the socket.

router = APIRouter()

#: The verbs that reposition or pace the replay; none of them applies while live.
_TRANSPORT = frozenset({"play", "stop", "step", "seek", "speed"})

#: Documents the refusal on every command, so the contract carries it.
_REFUSED = {409: {"description": "The command does not apply in the player's current mode."}}


def refusal(status: PlayerStatus, verb: str) -> str | None:
    """Why ``verb`` cannot be applied right now, or ``None`` if it can."""
    if verb in _TRANSPORT and status.mode == "live":
        return f"{verb} does not apply in live mode; switch to the recording first"
    if verb == "live" and not status.can_go_live:
        return "no live source is configured for this pipeline"
    if verb == "replay" and status.coverage_start is None:
        return "no recording to replay: the pipeline has no history source"
    return None


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


async def dispatch(client_id: str, verb: str, **args: object) -> CommandAck:
    """Publish one command on the client's bus and acknowledge it -- or refuse
    it with a 409 when the player's mode cannot apply it."""
    pipeline = live_pipeline(client_id)
    reason = refusal(pipeline.player.status(), verb)
    if reason is not None:
        logger.info("client %s: refused %s: %s", client_id, verb, reason)
        raise HTTPException(status_code=409, detail=reason)
    command = Command(client_id=client_id, verb=verb, args=dict(args))
    pipeline.bus.publish(command)
    log_event(f"{verb} {args or ''} (request {command.request_id})", client_id)
    return CommandAck(applied=verb)


@router.post("/playback/play", operation_id="pmu_test_streamer_play", responses=_REFUSED)
async def play(client_id: ClientId) -> CommandAck:
    """Start (or resume) this client's replay."""
    return await dispatch(client_id, "play")


@router.post("/playback/stop", operation_id="pmu_test_streamer_stop", responses=_REFUSED)
async def stop(client_id: ClientId) -> CommandAck:
    """Pause this client's replay where it is."""
    return await dispatch(client_id, "stop")


@router.post("/playback/forward", operation_id="pmu_test_streamer_forward", responses=_REFUSED)
async def forward(client_id: ClientId) -> CommandAck:
    """Play one frame, independently of the play/pause state."""
    return await dispatch(client_id, "step", n=1)


@router.post("/playback/back", operation_id="pmu_test_streamer_back", responses=_REFUSED)
async def back(client_id: ClientId) -> CommandAck:
    """Step one frame back: the player reopens its stream one interval earlier."""
    return await dispatch(client_id, "step", n=-1)


class SeekBody(BaseModel):
    offset_s: float = Field(ge=0, description="Seconds from the start of the recording.")


@router.post("/playback/seek", operation_id="pmu_test_streamer_seek", responses=_REFUSED)
async def seek(client_id: ClientId, body: SeekBody) -> CommandAck:
    """Jump the replay to an offset into the recording. 409 while live."""
    return await dispatch(client_id, "seek", offset_s=body.offset_s)


class SpeedBody(BaseModel):
    speed: float = Field(gt=0, le=10, description="Replay speed multiplier; 1 is real time.")


@router.post("/playback/speed", operation_id="pmu_test_streamer_speed", responses=_REFUSED)
async def speed(client_id: ClientId, body: SpeedBody) -> CommandAck:
    """Change the replay speed. 409 while live."""
    return await dispatch(client_id, "speed", speed=body.speed)


@router.post("/playback/live", operation_id="pmu_test_streamer_live", responses=_REFUSED)
async def live(client_id: ClientId) -> CommandAck:
    """Switch this client to the live feed: frames from now, as they arrive, no
    transport controls. 409 when no live source is configured."""
    return await dispatch(client_id, "live")


@router.post("/playback/replay", operation_id="pmu_test_streamer_replay", responses=_REFUSED)
async def replay(client_id: ClientId) -> CommandAck:
    """Switch this client back to the recording, paused at its start. 409 when
    no history source is configured."""
    return await dispatch(client_id, "replay")


# --- websocket endpoint (downstream only) ---------------------------------------


@contextlib.asynccontextmanager
async def connected_pipeline(ws: WebSocket) -> AsyncIterator[Pipeline | None]:
    """Accept one socket and hold its client's pipeline for as long as it lives.

    Yields ``None`` when the connection was refused. Two refusals, and the
    ordering of ``accept`` between them is the point: no usable client id is
    closed *before* accepting (1008); at capacity the socket is accepted first
    and then closed with 1013, because a code only reaches the browser on an
    established connection, and the web client treats 1013 as terminal.

    (A local copy of ``pswamp_web.hub.connected_hub`` over the core registry;
    the two unify when the grid monitor is re-pointed at the core.)
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
    """What this page shows, off the client's bus. Opened by the endpoint
    *before* its first send, so nothing published in between is lost."""
    return pipeline.bus.subscribe(
        PmuFrame, PlayerStatus, FrameStatsResult, StreamChanged,
        overflow=Overflow.DROP_OLDEST, maxsize=64,
    )


async def serve_stream(
    ws: WebSocket, pipeline: Pipeline, header: PmuHeader | None, updates: Subscription
) -> None:
    """Push the state on every change until the client disconnects.

    Drains the page's subscription and coalesces: when the reader wakes it
    drains whatever else is pending and sends **one** message built from the
    latest of each, so a socket that falls behind sees the newest state rather
    than a backlog of stale ones.
    """

    async def push() -> None:
        async for _ in updates:
            while updates.get_nowait() is not None:
                pass
            await send_state(ws, state_message(pipeline, header, first=False))

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
        log_event("connected", ws.query_params.get("client_id", "?"))
        # Subscribe first, then snapshot, then serve: a command landing between
        # the snapshot and the subscription would otherwise publish its status
        # to nobody, and a paused player sends nothing later to make up for it.
        with subscribe_updates(pipeline) as updates:
            header = await stream_header(pipeline.gateway)
            await send_state(ws, state_message(pipeline, header, first=True))
            await serve_stream(ws, pipeline, header, updates)
    log_event("disconnected", ws.query_params.get("client_id", "?"))
