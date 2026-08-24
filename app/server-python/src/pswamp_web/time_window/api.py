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

import asyncio
import contextlib
import functools
from dataclasses import dataclass, field

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from .. import channels as channel_utils
from ..hub import Hub, connected_hub
from ..replay import load_recording
from ..wire import TimeWindowSlice, send_state, series

# How often a client is updated. Fast enough to read as live, and slow enough
# that each message carries a handful of samples rather than one.
PUSH_HZ = 10

router = APIRouter()


@dataclass
class ClientState:
    """Per-connection *view* state. The data lives in the client's own Hub;
    what varies between clients is only what they are looking at."""

    selection: list[int] = field(default_factory=list)
    # Value of the window's append counter at the last message, which is how the
    # next message knows how many rows are new.
    last_appended: int = 0
    seq: int = 0
    needs_full: bool = True


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


async def handle_command(hub: Hub, state: ClientState, message: dict) -> None:
    action = message.get("action")
    if action == "select_channels":
        selection = channel_utils.sanitise(
            message.get("channels"), hub.store_app.tw.n_cols
        )
        if selection:
            state.selection = selection
            # The client's existing traces are for different channels entirely,
            # so the next message has to be a full one.
            state.needs_full = True
    elif action == "resync":
        state.needs_full = True


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_hub(ws) as hub:
        if hub is None:
            return

        all_channels = _channels()
        state = ClientState(selection=channel_utils.default_selection(all_channels))
        # The first read establishes the baseline for the append counter; without
        # it the opening message would claim every sample ever appended is new.
        # Note this is *this client's* window, which on a fresh pipeline has just
        # been prefilled and is at the start of the recording.
        state.last_appended = hub.store_app.tw.n_appended

        async def push() -> None:
            interval = 1 / PUSH_HZ
            while True:
                message = build_message(hub, state)
                if message is not None:
                    await send_state(ws, message)
                await asyncio.sleep(interval)

        pusher = asyncio.create_task(push())
        try:
            while True:
                await handle_command(hub, state, await ws.receive_json())
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            pusher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pusher


@router.get("/channels")
async def list_channels() -> dict:
    """Every selectable channel, for the picker. Static for the process, so it is
    a plain GET rather than part of the socket protocol."""
    return {"channels": [c.model_dump() for c in _channels()]}
