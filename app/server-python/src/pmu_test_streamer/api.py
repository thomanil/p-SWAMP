"""The PMU test streamer's backend: WebSocket api, per-client position, ticker.

Streams sample grid records line by line, keeping per-client state keyed by the
client id. There is nothing to pick: one data file, one stream.

Commands come up over REST and state goes down over the socket; the reasoning is
in AGENTS.md and doc/the-client-server-api.md.

server.py mounts this `router` under /api/pmu-test-streamer, so the endpoint below
is reachable at /api/pmu-test-streamer/ws. Nothing here knows about that prefix.

All state is in memory and dies with the process, and everything
runs on the one asyncio event loop — the WS handlers, the request handlers, the
ticker, and broadcasts are cooperatively scheduled and never truly parallel, so no
locking is needed.
"""

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Literal

from fastapi import APIRouter, FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from shared import (
    ClientId,
    CommandAck,
    SocketRegistry,
    get_logger,
    read_client_id,
    send_state,
)

from .model import LINES, TICKS_PER_SECOND, PmuStreamModel

# Discrete client events only — never the ticker's auto-advance, which fires
# TICKS_PER_SECOND times a second per playing client.
logger = get_logger("pmu")


# --- authoritative in-memory state ------------------------------------------
#
# One position + play flag per client seed (the ?client_id= URL param), kept across
# reconnects so a dropped client resumes mid-stream, and never evicted (a bounded,
# acceptable leak for a local dev demo). This dict is the only store; nothing is
# persisted, so a restart puts every client back at the first record.


@dataclass
class ClientState:
    model: PmuStreamModel = field(default_factory=PmuStreamModel)
    playing: bool = False


states: dict[str, ClientState] = {}


def get_state(client_id: str) -> ClientState:
    """The single place per-client state is born; called on connect and on every
    command, so a command can never hit a missing client."""
    state = states.get(client_id)
    if state is None:
        state = states[client_id] = ClientState()
    return state


class PmuRecord(BaseModel):
    """One record in the visible window: a 1-based line number and its text."""

    line_number: int = Field(description="1-based, matching how `wc -l` counts.")
    text: str = Field(description="The raw record, verbatim from sample_data.txt.")


class PmuStreamState(BaseModel):
    """The single message shape pushed to a client on connect and every change.

    A declared model rather than a loose dict, because this IS the downstream half
    of the published contract: api_contract.py collects it via this package's
    WS_MESSAGE export, and a bare dict would silently drop the app out of it.

    `total_lines` lets the client show "record N of M" — which is also how the
    wrap-around at the end of the file becomes visible in the UI.
    """

    type: Literal["state"] = "state"
    window: list[PmuRecord | None] = Field(
        description="Records around the cursor; null where it runs off an end.",
    )
    index: int = Field(description="0-based cursor into the sample file.")
    total_lines: int = Field(description="How many records the sample file holds.")
    playing: bool = Field(description="Whether the server is advancing this client.")


def state_message(state: ClientState) -> PmuStreamState:
    """The single message shape pushed to a client on connect and every change."""
    return PmuStreamState(
        window=state.model.visible_window(),
        index=state.model.index,
        total_lines=len(LINES),
        playing=state.playing,
    )


def roster_table(acting_id: str | None = None) -> str:
    """An aligned table of every currently connected client: where it is in the
    stream and whether it's playing. Disconnected-but-remembered seeds are excluded
    — this is the live roster, not the state table. The client that triggered the
    current event is flagged with an arrow."""
    ids = sorted(sockets.clients(), key=int)
    if not ids:
        return "    (no clients connected)"
    headers = ("", "CLIENT", "RECORD", "STATE")
    rows = [headers]
    for cid in ids:
        state = states[cid]
        rows.append(
            (
                "->" if cid == acting_id else "",
                str(cid),
                f"{state.model.index + 1}/{len(LINES)}",
                "playing" if state.playing else "paused",
            )
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(headers))]

    def fmt(row: tuple[str, ...]) -> str:
        return "    " + "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))

    rule = "    " + "-" * (sum(widths) + 2 * (len(widths) - 1))
    return "\n".join([fmt(headers), rule, *(fmt(row) for row in rows[1:])])


def log_event(action: str, client_id: str) -> None:
    """The single logging entry point: the triggering client + action, then the full
    live roster, so the console always shows the complete picture after any
    operation."""
    logger.info("client %s: %s\n\n%s\n", client_id, action, roster_table(client_id))


# --- connection tracking ----------------------------------------------------
#
# Transport bookkeeping only, so it comes from shared.py; this app's own state lives
# in `states` above and deliberately outlives a disconnect.

sockets = SocketRegistry()


# --- server-side playback ticker -------------------------------------------


async def ticker() -> None:
    """One driver for every client: each tick, advance only the clients that are
    currently playing and push each its own updated state.

    Paced against a monotonic deadline rather than `sleep(interval)`, because the
    latter waits interval *plus* the time the tick's own work took — a 5% shortfall
    at this app's 100 ticks/s, which would compound over a long replay and quietly
    make "real time" a lie. If a tick ever overruns by more than one interval (a
    stalled client, a throttled CPU) the deadline is reset to now instead of firing
    a catch-up burst: better to drop time than to flood the socket.

    Iterate a snapshot of `states` because a connect/disconnect can mutate it
    across the `await`.
    """
    interval = 1 / TICKS_PER_SECOND
    next_tick = time.monotonic()
    while True:
        next_tick += interval
        now = time.monotonic()
        if now > next_tick + interval:
            next_tick = now
        await asyncio.sleep(max(0.0, next_tick - now))
        for client_id, state in list(states.items()):
            if state.playing:
                state.model.step_forward()
                await sockets.send_to_client(client_id, state_message(state))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """This app's slice of the process lifespan: run the streaming ticker for as
    long as the server is up. server.py composes it with the other app packages'
    lifespans (see APPS there)."""
    task = asyncio.create_task(ticker())
    try:
        yield
    finally:
        task.cancel()


# --- REST commands ----------------------------------------------------------
#
# One POST per operation; see doc/the-client-server-api.md for why the upstream
# half is HTTP and the downstream half is not.
#
# Paths are relative to wherever server.py mounts this router
# (/api/pmu-test-streamer), so "/playback/play" is served as
# /api/pmu-test-streamer/playback/play.

router = APIRouter()


async def applied(client_id: str, state: ClientState, action: str) -> CommandAck:
    """Log the command, push this client its new state, acknowledge the request."""
    log_event(action, client_id)
    await sockets.send_to_client(client_id, state_message(state))
    return CommandAck(applied=action)


@router.post("/playback/play", operation_id="pmu_test_streamer_play")
async def play(client_id: ClientId) -> CommandAck:
    """Start advancing this client through the recorded stream."""
    state = get_state(client_id)
    state.playing = True
    return await applied(client_id, state, "play")


@router.post("/playback/stop", operation_id="pmu_test_streamer_stop")
async def stop(client_id: ClientId) -> CommandAck:
    """Pause this client where it is in the stream."""
    state = get_state(client_id)
    state.playing = False
    return await applied(client_id, state, "stop")


@router.post("/playback/forward", operation_id="pmu_test_streamer_forward")
async def forward(client_id: ClientId) -> CommandAck:
    """Step one record forward, independently of the play/pause flag."""
    state = get_state(client_id)
    state.model.step_forward()
    return await applied(client_id, state, "forward")


@router.post("/playback/back", operation_id="pmu_test_streamer_back")
async def back(client_id: ClientId) -> CommandAck:
    """Step one record back, independently of the play/pause flag."""
    state = get_state(client_id)
    state.model.step_back()
    return await applied(client_id, state, "back")


# --- websocket endpoint (downstream only) -----------------------------------


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    # The client identifies itself with a numeric seed in the URL
    # (ws://.../api/pmu-test-streamer/ws?client_id=<seed>); reject a connection
    # without a valid one. `read_client_id` applies the very rule the `ClientId`
    # query parameter enforces, so a page's socket and its commands can never
    # address different state.
    client_id = read_client_id(ws)
    if client_id is None:
        await ws.close(code=1008)  # policy violation
        return

    # Resuming an existing seed vs. a brand-new one changes the connect message.
    known = client_id in states
    async with sockets.connected(ws, client_id):
        state = get_state(client_id)  # born here so it shows in the roster below
        log_event("reconnected" if known else "connected", client_id)
        try:
            # Straight down this socket, not through the registry: the opening
            # message is for the connection that just arrived (and resumes its
            # prior position if known), while a command's result goes to every
            # socket the client has open.
            await send_state(ws, state_message(state))
            # Nothing is sent up this socket; the receive loop is what surfaces a
            # disconnect. Without it a closed socket is only noticed on the next
            # send, so a paused client would linger for ever.
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
    # Outside the block, so the socket is already out of the registry and the
    # roster this logs shows who is left rather than who is leaving.
    log_event("disconnected", client_id)
