"""The PMU test streamer's backend: a per-client replay over the data gateway.

The thin slice of the data-integration architecture (STEP-4). What changed from
the ticker-over-a-text-file it replaced, and what stayed:

**Stayed** -- the browser edge. Commands come up as POSTs under
``/api/pmu-test-streamer/``, state goes down one socket at ``/ws``, every
message is the model this package exports as ``WS_MESSAGE``, and the client id
is the routing key. The reasoning is in AGENTS.md and doc/the-client-server-api.md.

**Changed** -- everything behind it. This package no longer opens a file: it
asks the process's gateway for ``Sample`` rows of one stream and does not know
or care which client answers (``pmu_data`` decides that, from configuration).
Each browser gets its own :class:`~pswamp.data.Replay` -- a paced, seekable
cursor over that stream -- which is STEP3 §8.2's *"replay pipeline per client"*
in its smallest form: one async generator per viewer rather than a copy of the
data. Play, stop, speed, step and seek are that replay's controls, exposed one
POST each (STEP3 §8.3). ``mode`` on the wire is what the source reports:
``replay`` for this recording, ``live`` if a deployment plugs in a live feed,
which is how the client knows whether to show transport controls at all.

**Lifecycle.** A client's *state* (position, playing, speed) outlives its
sockets and is never evicted, exactly as before -- so a reload resumes where it
was. A client's *replay* lives only while it has a socket open: started on the
first connect, stopped on the last disconnect, and rebuilt from the retained
position next time. Nothing is persisted; a restart puts everyone at the start.

Everything here runs on the one event loop, so no locking is needed.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from fastapi import APIRouter, HTTPException, WebSocket
from pmu_data import STREAM_ID, coverage_of, gateway, stream_header
from pswamp.data import Coverage, Replay, Sample, StreamHeader
from pydantic import BaseModel, Field
from shared import (
    ClientId,
    CommandAck,
    SocketRegistry,
    get_logger,
    read_client_id,
    send_state,
    wait_for_disconnect,
)

# Discrete client events only -- never the per-sample push.
logger = get_logger("pmu")

sockets = SocketRegistry()
router = APIRouter()

MIN_SPEED = 0.1
MAX_SPEED = 100.0


# --- authoritative in-memory state ------------------------------------------


@dataclass
class ClientState:
    """One client's replay: what outlives the socket, and what does not."""

    # Retained across disconnects: where this client is and how it plays.
    position: datetime | None = None
    playing: bool = False
    speed: float = 1.0
    passes: int = 0

    # Live only while at least one socket is open.
    header: StreamHeader | None = None
    coverage: Coverage | None = None
    replay: Replay | None = None
    pusher: asyncio.Task | None = None


states: dict[str, ClientState] = {}


def get_state(client_id: str) -> ClientState:
    """The single place per-client state is born; called on connect and on every
    command, so a command can never hit a missing client."""
    state = states.get(client_id)
    if state is None:
        state = states[client_id] = ClientState()
    return state


# --- the wire ---------------------------------------------------------------


class PmuStreamState(BaseModel):
    """The single message shape pushed to a client on connect and every change.

    A sample message carries the row that just played; a control message (after
    play/stop/speed) carries none, and the page keeps showing the last one. The
    header rides on the opening message only.
    """

    type: Literal["state"] = "state"
    header: StreamHeader | None = Field(
        default=None, description="Channel table; sent on the opening message only."
    )
    sample: Sample | None = Field(
        default=None, description="The row that just played; null on a control-only update."
    )
    mode: Literal["live", "replay"] = Field(
        description="What the source can do; transport controls only make sense for replay."
    )
    index: int = Field(description="0-based sample ordinal within the current pass.")
    total: int = Field(description="How many samples one pass of the source holds.")
    position_s: float | None = Field(description="Seconds into the source; null before the first sample.")
    duration_s: float = Field(description="Length of one pass of the source, in seconds.")
    playing: bool
    speed: float = Field(description="Replay speed factor; 1.0 is as recorded.")
    passes: int = Field(description="How many times this client's replay has looped.")


def state_message(state: ClientState, sample: Sample | None, *, opening: bool = False) -> PmuStreamState:
    header, coverage = state.header, state.coverage
    assert header is not None and coverage is not None, "the pipeline must be started first"
    start, end = coverage.range.start, coverage.range.end
    assert start is not None and end is not None
    duration = (end - start).total_seconds()
    position = None if state.position is None else (state.position - start).total_seconds()
    return PmuStreamState(
        header=header if opening else None,
        sample=sample,
        mode="live" if coverage.live else "replay",
        index=-1 if position is None else round(position * header.data_rate),
        total=round(duration * header.data_rate) + 1,  # fence posts: 2.95 s at 20 Hz is 60 samples
        position_s=position,
        duration_s=round(duration, 6),
        playing=state.playing,
        speed=state.speed,
        passes=state.passes,
    )


# --- the per-client replay pipeline -----------------------------------------


async def start_pipeline(client_id: str, state: ClientState) -> None:
    """Open this client's replay at its retained position and start pushing."""
    state.header = await stream_header(STREAM_ID)
    state.coverage = await coverage_of(Sample, STREAM_ID)
    if state.coverage is None:
        raise RuntimeError(f"no client serves Sample for stream {STREAM_ID!r}")
    state.replay = Replay(
        gateway(),
        Sample,
        mRID=STREAM_ID,
        start=state.position,
        speed=state.speed,
        paused=not state.playing,
        loop=True,
    )
    if not state.playing:
        # A paused replay emits nothing on its own; show the row it stands on.
        state.replay.step(1)
    state.pusher = asyncio.create_task(push_samples(client_id, state))


async def push_samples(client_id: str, state: ClientState) -> None:
    """Fan every sample the replay releases out to this client's sockets."""
    assert state.replay is not None
    try:
        async for sample in state.replay:
            state.position = sample.timestamp
            state.passes = state.replay.passes
            await sockets.send_to_client(client_id, state_message(state, sample))
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("client %s: replay pusher died", client_id)


async def stop_pipeline(state: ClientState) -> None:
    """Tear the replay down; the retained position lets the next connect resume."""
    task, replay = state.pusher, state.replay
    state.pusher = state.replay = None
    if task is not None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    if replay is not None:
        await replay.aclose()


# --- logging ----------------------------------------------------------------


def roster_table(acting_id: str | None = None) -> str:
    """Every connected client, its position and whether it plays."""
    ids = sorted(sockets.clients(), key=int)
    if not ids:
        return "    (no clients connected)"
    rows = [("", "CLIENT", "POSITION", "SPEED", "STATE")]
    for cid in ids:
        state = states[cid]
        start = state.coverage.range.start if state.coverage else None
        at = "-" if state.position is None or start is None else f"{(state.position - start).total_seconds():.2f}s"
        rows.append(
            (
                "->" if cid == acting_id else "",
                cid,
                at,
                f"x{state.speed:g}",
                "playing" if state.playing else "paused",
            )
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]

    def fmt(row: tuple[str, ...]) -> str:
        return "    " + "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))

    rule = "    " + "-" * (sum(widths) + 2 * (len(widths) - 1))
    return "\n".join([fmt(rows[0]), rule, *(fmt(row) for row in rows[1:])])


def log_event(action: str, client_id: str) -> None:
    logger.info("client %s: %s\n\n%s\n", client_id, action, roster_table(client_id))


# --- REST commands ----------------------------------------------------------
#
# One POST per operation. Play, stop and speed change retained state and push a
# control message so the page's badge updates even when nothing is flowing;
# forward, back and seek drive the replay, whose next emission is the update.


class SetSpeed(BaseModel):
    speed: float = Field(ge=MIN_SPEED, le=MAX_SPEED, description="Replay speed factor.")


class Seek(BaseModel):
    position_s: float = Field(ge=0, description="Seconds into the source to continue from.")


async def applied(client_id: str, state: ClientState, action: str, *, push: bool) -> CommandAck:
    log_event(action, client_id)
    if push and state.replay is not None:
        await sockets.send_to_client(client_id, state_message(state, None))
    return CommandAck(applied=action)


def running_replay(client_id: str) -> tuple[ClientState, Replay]:
    """The client's live replay, or a 404: stepping needs a stream to step."""
    state = states.get(client_id)
    if state is None or state.replay is None or state.coverage is None:
        raise HTTPException(
            status_code=404,
            detail=f"client {client_id} has no replay running; open the page first",
        )
    return state, state.replay


@router.post("/playback/play", operation_id="pmu_test_streamer_play")
async def play(client_id: ClientId) -> CommandAck:
    """Start advancing this client through the recorded stream."""
    state = get_state(client_id)
    state.playing = True
    if state.replay is not None:
        state.replay.play()
    return await applied(client_id, state, "play", push=True)


@router.post("/playback/stop", operation_id="pmu_test_streamer_stop")
async def stop(client_id: ClientId) -> CommandAck:
    """Pause this client where it is in the stream."""
    state = get_state(client_id)
    state.playing = False
    if state.replay is not None:
        state.replay.pause()
    return await applied(client_id, state, "stop", push=True)


@router.post("/playback/speed", operation_id="pmu_test_streamer_speed")
async def speed(client_id: ClientId, body: SetSpeed) -> CommandAck:
    """Change how fast this client's replay runs relative to real time."""
    state = get_state(client_id)
    state.speed = body.speed
    if state.replay is not None:
        state.replay.set_speed(body.speed)
    return await applied(client_id, state, f"speed x{body.speed:g}", push=True)


@router.post("/playback/forward", operation_id="pmu_test_streamer_forward")
async def forward(client_id: ClientId) -> CommandAck:
    """Release the next sample now, playing or paused."""
    state, replay = running_replay(client_id)
    replay.step(1)
    return await applied(client_id, state, "forward", push=False)


@router.post("/playback/back", operation_id="pmu_test_streamer_back")
async def back(client_id: ClientId) -> CommandAck:
    """Jump one sample back: a seek to the previous instant."""
    state, replay = running_replay(client_id)
    assert state.header is not None and state.coverage is not None
    start = state.coverage.range.start
    if state.position is None or start is None or state.header.data_rate <= 0:
        replay.seek(None)
    else:
        target = state.position - timedelta(seconds=1 / state.header.data_rate)
        replay.seek(max(target, start))
    return await applied(client_id, state, "back", push=False)


@router.post("/playback/seek", operation_id="pmu_test_streamer_seek")
async def seek(client_id: ClientId, body: Seek) -> CommandAck:
    """Continue from an absolute position in the source."""
    state, replay = running_replay(client_id)
    assert state.coverage is not None
    start, end = state.coverage.range.start, state.coverage.range.end
    assert start is not None and end is not None
    target = min(start + timedelta(seconds=body.position_s), end)
    replay.seek(target)
    return await applied(client_id, state, f"seek {body.position_s:.2f}s", push=False)


# --- websocket endpoint (downstream only) -----------------------------------


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    client_id = read_client_id(ws)
    if client_id is None:
        await ws.close(code=1008)  # policy violation
        return

    known = client_id in states
    async with sockets.connected(ws, client_id):
        state = get_state(client_id)
        first_socket = state.replay is None
        if first_socket:
            await start_pipeline(client_id, state)
        log_event("reconnected" if known else "connected", client_id)
        # The opening message goes straight down this socket: it carries the
        # header and the retained position; samples follow from the pusher.
        await send_state(ws, state_message(state, None, opening=True))
        await wait_for_disconnect(ws)
    # The socket is out of the registry now; if it was the last one, the replay
    # goes with it and the position stays behind for the next connect.
    if not sockets.of(client_id):
        await stop_pipeline(state)
    log_event("disconnected", client_id)
