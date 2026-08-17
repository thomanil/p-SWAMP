"""The timeline app's backend: WebSocket api, in-memory state, playback ticker.

Single source of truth: this module owns the timeline state in memory
(per-sequence indices, the active sequence, and the play/pause flag) and runs the
playback ticker. The client is a thin renderer that streams commands in and state
out over one WebSocket.

This is one app package among (eventually) several — see src/server.py, which
assembles the process and mounts this `router` under /api/timeline, so the
endpoint below is reachable at /api/timeline/ws. Nothing here knows or cares about
that prefix, the health probe, or the web client's static assets.

The two names server.py needs are re-exported from __init__.py: `router` (the
endpoints) and `lifespan` (the ticker's start/stop). A new app package copies that
pair.

The app is entirely stateless in the persistence sense: there is no database and
nothing is written to disk. All state lives in this process and is gone when it
exits — a restart or redeploy resets every client to the start of the timeline.

Everything here runs on a single asyncio event loop: the WebSocket handler
coroutines, the ticker task, and broadcasts are cooperatively scheduled and
never truly parallel, so the shared `model`/`playing` state needs no locks.
"""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from fastapi import APIRouter, FastAPI, WebSocket, WebSocketDisconnect
from shared import ConnectionManager, make_logger

from .model import SEQUENCES, TICKS_PER_SECOND, TimelineModel

# --- logging -----------------------------------------------------------------
#
# We log discrete client events (connects, playback commands) but deliberately NOT
# the ticker's auto-advance, which fires TICKS_PER_SECOND times a second per
# playing client.

logger = make_logger("timeline")

# --- authoritative in-memory state ------------------------------------------
#
# State is per client, not global. Each client process generates an integer
# seed (its client id, sent as the ?client_id= URL param) and the server keeps
# one timeline + play flag per seed. State is keyed by seed and survives across
# reconnects, so a dropped-and-reopened client resumes where it left off; it is
# never evicted (a bounded, acceptable leak for a local dev demo).
#
# This dict is the ONLY store — there is no database and nothing is written to
# disk, so all of it dies with the process. That's deliberate: a restart or
# redeploy resets every client, and the single-replica rule in the k8s manifest
# is what keeps the one copy authoritative.


@dataclass
class ClientState:
    model: TimelineModel = field(default_factory=TimelineModel)
    playing: bool = False


states: dict[int, ClientState] = {}


def get_state(client_id: int) -> ClientState:
    """The single place per-client state is born; called on connect and on every
    command, so a command can never hit a missing client."""
    state = states.get(client_id)
    if state is None:
        state = states[client_id] = ClientState()
    return state


def state_message(state: ClientState) -> dict:
    """The single message shape pushed to a client on connect and every change."""
    return {
        "type": "state",
        "window": state.model.visible_window(),
        "sequence_name": state.model.sequence_name,
        "sequences": list(SEQUENCES.keys()),
        "playing": state.playing,
    }


def roster_table(acting_id: int | None = None) -> str:
    """An aligned, human-readable table of every currently connected client and
    its state: sequence picked, place in the timeline (index + value there), and
    play/pause. Disconnected-but-remembered seeds are excluded -- this is the
    live roster, not the state table. The client that triggered the current
    event (if any) is flagged with an arrow."""
    ids = sorted(manager.conns)
    if not ids:
        return "    (no clients connected)"
    headers = ("", "CLIENT", "SEQUENCE", "INDEX", "VALUE", "STATE")
    rows = [headers]
    for cid in ids:
        m = states[cid].model
        value = m.value_at(m.index)
        rows.append(
            (
                "->" if cid == acting_id else "",
                str(cid),
                m.sequence_name,
                str(m.index),
                "-" if value is None else str(value),
                "playing" if states[cid].playing else "paused",
            )
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(headers))]

    def fmt(row: tuple[str, ...]) -> str:
        return "    " + "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))

    rule = "    " + "-" * (sum(widths) + 2 * (len(widths) - 1))
    return "\n".join([fmt(headers), rule, *(fmt(row) for row in rows[1:])])


def log_event(action: str, client_id: int) -> None:
    """The single logging entry point for every interesting event. Emits the
    triggering client + action, then dumps the full live roster so the console
    always shows the complete picture after any operation (connect, disconnect,
    play, stop, forward, back, set_sequence)."""
    logger.info("client %s: %s\n\n%s\n", client_id, action, roster_table(client_id))


# --- connection tracking ----------------------------------------------------
#
# Transport bookkeeping only, so it comes from shared.py; this app's own state
# lives in `states` above. `states[client_id]` deliberately outlives a disconnect,
# so a client resumes its timeline when it reconnects with the same seed.

manager = ConnectionManager()


# --- server-side playback ticker -------------------------------------------


async def ticker() -> None:
    """One driver for every client: each tick, advance only the clients that are
    currently playing and push each its own updated state.

    `model.step_forward()` writes `_indices[sequence_name]`, so a client's
    inactive sequences stay frozen at their last position. Iterate a snapshot of
    `states` because a connect/disconnect can mutate it across the `await`.
    """
    interval = 1 / TICKS_PER_SECOND
    while True:
        await asyncio.sleep(interval)
        for client_id, state in list(states.items()):
            if state.playing:
                state.model.step_forward()
                await manager.send_to_client(client_id, state_message(state))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """This app's slice of the process lifespan: run the playback ticker for as
    long as the server is up. server.py composes this with any other app
    package's lifespan (see APPS there) into the one FastAPI lifespan."""
    task = asyncio.create_task(ticker())
    try:
        yield
    finally:
        task.cancel()


# --- command dispatch + websocket endpoint ---------------------------------
#
# Paths here are relative to wherever server.py mounts this router
# (/api/timeline), so "/ws" is served as /api/timeline/ws.

router = APIRouter()


async def handle_command(client_id: int, msg: dict) -> None:
    state = get_state(client_id)
    action = msg.get("action")
    m = state.model
    if action == "forward":
        m.step_forward()
    elif action == "back":
        m.step_back()
    elif action == "play":
        state.playing = True
    elif action == "stop":
        state.playing = False
    elif action == "set_sequence":
        name = msg.get("name")
        if name not in SEQUENCES:
            logger.warning("client %s set_sequence rejected: %r", client_id, name)
            return
        m.set_sequence(name)
        action = f"set_sequence {name}"
    else:
        logger.warning("client %s unknown action: %r", client_id, action)
        return  # unknown action -> no state change, no send
    log_event(action, client_id)
    await manager.send_to_client(client_id, state_message(state))


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    # The client identifies itself with an integer seed in the URL
    # (ws://.../api/timeline/ws?client_id=<seed>); reject a connection without a
    # valid one.
    try:
        client_id = int(ws.query_params.get("client_id"))
    except (TypeError, ValueError):
        await ws.close(code=1008)  # policy violation
        return

    # Resuming an existing seed vs. a brand-new one changes the connect message.
    known = client_id in states
    await manager.connect(ws, client_id)
    state = get_state(client_id)  # born here so it shows in the roster below
    log_event("reconnected" if known else "connected", client_id)
    try:
        # Initial full state for this client (resumes prior state if seed is known).
        await ws.send_json(state_message(state))
        while True:
            msg = await ws.receive_json()
            await handle_command(client_id, msg)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        manager.disconnect(ws, client_id)
        log_event("disconnected", client_id)
