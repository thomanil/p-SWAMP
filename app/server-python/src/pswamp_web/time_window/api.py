# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""A live view of this client's measurement window.

The web counterpart of p-SWAMP's ``visualization.time_window_plot_v2``.

The Qt plot has an advantage this one does not: it holds a reference to the very
``TimeWindow`` the application thread writes into, so "sending" a window costs
nothing. Here the same data has to cross a socket, and doing it naively is the
difference between a page that works and one that does not -- a 30 s window of 8
channels at 50 Hz is 12,000 numbers, and re-sending it ten times a second is on
the order of a megabyte per second, per client.

So the window is sent once, and after that only the samples that are new. At
10 Hz that is five rows per channel per message rather than fifteen hundred, and
the cost stops depending on how much history the page shows.

The read itself is the deliberate mirror of what the Qt widget does: poll the
window on a timer, under the lock the window already has. No queue and no
bus, which is what keeps the 50 Hz sample stream from becoming 50 event-loop
callbacks a second.
"""

import functools
from dataclasses import dataclass, field

from fastapi import APIRouter, HTTPException, WebSocket
from pydantic import BaseModel, Field

from .. import channels as channel_utils
from ..hub import Hub, connected_hub, live_hub
from ..pump import serve_ticks
from ..replay import load_recording
from ..sessions import SessionRegistry
from ..wire import (
    ChannelCatalogue,
    ClientId,
    CommandAck,
    TimeWindowSlice,
    read_client_id,
    series,
)

# How often a client is updated. Fast enough to read as live, and slow enough
# that each message carries a handful of samples rather than one.
PUSH_HZ = 10

router = APIRouter()


@dataclass
class ClientState:
    """Per-connection *view* state. The data lives in the client's own Hub;
    what varies between clients is only what they are looking at.

    Published in SESSIONS below for the life of the socket, so the commands
    that change it -- HTTP requests, on no connection at all -- can find it.
    """

    selection: list[int] = field(default_factory=list)
    # Value of the window's append counter at the last message, which is how the
    # next message knows how many rows are new.
    last_appended: int = 0
    seq: int = 0
    needs_full: bool = True


# The open views of this endpoint, so POST /selection can find the one (or more)
# a browser has on screen. See ../sessions.py.
SESSIONS: SessionRegistry[ClientState] = SessionRegistry()


@functools.lru_cache(maxsize=1)
def _channels() -> tuple:
    """The flattened channel list, shared by every pipeline.

    Cached because rebuilding 700 objects on every tick of every client would be
    the most expensive thing this module does. Read from the *recording* rather
    than from a live Hub: every pipeline decodes the same file with no channel
    selection, so this is the same header its measurement window carries, and
    taking it from a Hub would outlive the client that happened to connect first.
    """
    return tuple(channel_utils.describe(load_recording().header))


def build_message(hub: Hub, state: ClientState) -> TimeWindowSlice | None:
    """Read this client's window and produce its next message.

    Returns None when there is nothing new, so an idle client costs nothing.
    """
    tw = hub.store_app.tw
    appended, times, data = tw.snapshot(state.selection)

    new_rows = appended - state.last_appended
    state.last_appended = appended

    # A client that has fallen behind by more than the window no longer shares
    # any samples with it, so an append would splice unrelated data onto what it
    # already has. Start it over instead.
    full = state.needs_full or new_rows >= tw.n_samples
    if not full and new_rows <= 0:
        return None

    if full:
        state.needs_full = False
        rows = slice(None)
    else:
        rows = slice(-new_rows, None)

    state.seq += 1
    described = None
    if full:
        by_idx = {c.idx: c for c in _channels()}
        described = [by_idx[idx] for idx in state.selection]

    return TimeWindowSlice(
        mode="full" if full else "append",
        seq=state.seq,
        # Time stamps are epoch seconds; three decimals is a millisecond, which
        # is finer than the 50 Hz sampling and keeps each one short on the wire.
        t=series(times[rows], ndigits=3),
        # Column-major: one list per selected channel.
        series=[
            series(data[rows, position]) for position in range(len(state.selection))
        ],
        channels=described,
        n_samples=tw.n_samples if full else None,
        sampling_rate=hub.recording.data_rate if full else None,
    )


# --- REST commands ----------------------------------------------------------
#
# Neither of these pushes anything itself. Both only set state the pusher task
# below already reads on its next tick, so a selection change lands within a
# tenth of a second and the send path stays in exactly one place.


class ChannelSelection(BaseModel):
    """Body of POST /selection."""

    channels: list[int] = Field(
        description="Channel indices, as listed by GET /channels.",
        examples=[[3, 17, 204]],
    )


def _sessions_or_404(client_id: str) -> list[ClientState]:
    """This browser's open views of this endpoint.

    A command with no view open is a 404: there is no selection to change, and
    silently accepting it would report success for something that did nothing.
    """
    sessions = SESSIONS.of(client_id)
    if not sessions:
        raise HTTPException(
            status_code=404,
            detail=f"client {client_id} has no open time-window view",
        )
    return sessions


@router.post("/selection", operation_id="time_window_select_channels")
async def select_channels(client_id: ClientId, body: ChannelSelection) -> CommandAck:
    """Choose which channels this browser's measurement view plots."""
    hub = live_hub(client_id)
    sessions = _sessions_or_404(client_id)

    selection = channel_utils.sanitise(body.channels, hub.store_app.tw.n_cols)
    if not selection:
        raise HTTPException(
            status_code=422,
            detail="no usable channel indices in the request",
        )

    for state in sessions:
        state.selection = selection
        # The client's existing traces are for different channels entirely, so
        # the next message has to be a full one.
        state.needs_full = True
    return CommandAck(applied=f"select_channels ({len(selection)})")


@router.post("/resync", operation_id="time_window_resync")
async def resync(client_id: ClientId) -> CommandAck:
    """Ask for the whole window again rather than the next delta.

    The escape hatch for a client whose buffer no longer matches what the server
    thinks it has -- a dropped append, a chart remounted mid-stream.
    """
    for state in _sessions_or_404(client_id):
        state.needs_full = True
    return CommandAck(applied="resync")


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_hub(ws) as hub:
        if hub is None:
            return

        # connected_hub has already validated this, so it cannot be None here;
        # we need the value itself to publish the session under it.
        client_id = read_client_id(ws)

        state = ClientState(selection=channel_utils.default_selection(_channels()))
        # The first read establishes the baseline for the append counter; without
        # it the opening message would claim every sample ever appended is new.
        # Note this is *this client's* window, which on a fresh pipeline has just
        # been prefilled and is at the start of the recording.
        state.last_appended = hub.store_app.tw.n_appended

        # build_message returns None when the window has not advanced, so an idle
        # client costs a dict lookup per tick and no traffic.
        with SESSIONS.registered(client_id, state):
            await serve_ticks(ws, PUSH_HZ, lambda: build_message(hub, state))


@router.get("/channels", operation_id="time_window_channels")
async def list_channels() -> ChannelCatalogue:
    """Every selectable channel, for the picker. Static for the process, so it is
    a plain GET rather than part of the socket protocol."""
    return ChannelCatalogue(channels=_channels())
