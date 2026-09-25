"""Helpers shared by the app packages under src/.

Not an app package: it exposes no `router` and is never listed in `APPS` — it is
just the home for pieces every app would otherwise copy. Keep it strictly
domain-free; anything that knows about timelines, PMU records, or any one app's
state belongs in that app's own package.

Most of what an app package needs is *defined* one level down, in `pswamp_web/`,
and re-exported here. That looks backwards and is not:

`pswamp_web/` is kept self-contained — nothing inside it may import from the
rest of this backend — because the repo's two Python projects are meant to become
one, and the direction depends on how long the Qt desktop path lives: either that
package moves in under `pswamp/`, or — once Qt is gone — the root package moves in
here (§7 of `doc/WIP-context-port-from-qt-to-web-frontend.md`). Self-containment
is what keeps either move cheap. The rule is one-way, though — it says nothing
about importing *inward*, which is what this module does, and which stays legal
whichever way the consolidation goes.

Getting that direction right is what removed four "change one, change the other"
duplicates: `ClientId`, `CommandAck`, the client-id parser, and the stdout
logger were each declared twice, and ~90 lines in `api_contract.py` existed to
reunify two of them in the published contract. One definition each now.

So an app package imports from here and needs to know nothing about the layout:

    from shared import ClientId, CommandAck, SocketRegistry, get_logger

What is genuinely defined here is `SocketRegistry` — the scaffold apps' socket
bookkeeping, which `pswamp_web/` has no use for because its pages push from their
own per-connection task rather than fanning out to a client's sockets.

`event_queue` and `serve_updates` are the grid monitor's event-driven push loop
(`pswamp_web/pump.py`), re-exported because they serve a core pipeline
unchanged: the core bus kept `add_listener(topic, fn)`, with a message class as
the topic, so a page over `pswamp_core` wakes on its result class the same way
a monitor page wakes on a store topic.

`dispatch_command` is the one way an app over a core pipeline turns a POST
into a command: find the client's pipeline (404 without one -- a command never
builds a pipeline), `pipeline.dispatch` it (409 when its receiver refuses it
now), acknowledge. `COMMAND_RESPONSES` documents those two answers on the
route, so the published contract carries them.

`ErrorForwarderModule` and `HUB` come from the `errors` app package: every app
that builds a core pipeline appends one forwarder to its module list, so a
pipeline's `ErrorEvent`s reach the layout's error tray with the app's slug on
them. Re-exported here so those apps import it from the one place they already
import from; `errors` itself never imports `shared` (see its docstring), which
is what keeps this from being a cycle.
"""

import contextlib
import logging
from collections.abc import AsyncIterator

from fastapi import HTTPException, WebSocket
from pydantic import BaseModel

from errors.forwarder import ErrorForwarderModule
from errors.hub import HUB
from pswamp_web.log import get_logger
from pswamp_web.pump import event_queue, serve_updates, wait_for_disconnect
from pswamp_web.sessions import SessionRegistry
from pswamp_web.wire import (
    CLIENT_ID_PATTERN,
    ClientId,
    CommandAck,
    read_client_id,
    send_state,
)

from pswamp_core.command_routing import CommandRefused
from pswamp_core.messages import Command
from pswamp_core.pipeline import PipelineRegistry

__all__ = [
    "CLIENT_ID_PATTERN",
    "COMMAND_RESPONSES",
    "HUB",
    "ClientId",
    "CommandAck",
    "ErrorForwarderModule",
    "SocketRegistry",
    "dispatch_command",
    "event_queue",
    "get_logger",
    "read_client_id",
    "send_state",
    "serve_updates",
    "wait_for_disconnect",
]


class SocketRegistry(SessionRegistry[WebSocket]):
    """This app's live sockets, per client id.

    A `SessionRegistry` (see `pswamp_web/sessions.py`) whose session *is* the
    socket, which is what a scaffold app's command needs to reach: it changes
    per-client state and the result has to go down whatever sockets that client
    has open. The page packages register something else in the same structure --
    a channel selection, a wake-up queue -- because their pushing is done by a
    task that already holds the socket.

    Pure transport: it knows nothing about what is being sent, so every app keeps
    its own instance beside its own state.

    One client may briefly hold several sockets -- two tabs, or a reconnect
    overlapping the socket it replaces -- so a message goes to all of them, and
    registration is scoped to the connection rather than to the client. The app's
    own per-client state deliberately outlives that: a client that comes back
    resumes where it was.
    """

    @contextlib.asynccontextmanager
    async def connected(self, ws: WebSocket, client_id: str) -> AsyncIterator[None]:
        """Accept one socket and hold it in the registry for as long as it lives.

        The scaffold apps' counterpart to `pswamp_web.hub.connected_hub`, minus
        the pipeline: accept, register, and unregister on the way out however the
        handler ends.
        """
        await ws.accept()
        with self.registered(client_id, ws):
            yield

    async def send_to_client(self, client_id: str, message: BaseModel) -> None:
        """Push one message to every live socket this client holds.

        A pydantic model, not a dict, and sent through `send_state` -- the one
        serialiser in the backend. Two reasons, and the second is the one that
        bites:

        1. The model IS the published schema. Every socket payload in this repo
           is declared as a model and picked up by api_contract.py from its
           package's `WS_MESSAGE`, so a message built as a loose dict would be
           absent from the contract the web client generates its types from.
        2. `send_json` routes through `json.dumps`, which emits bare `NaN` and
           `Infinity` tokens that `JSON.parse` rejects outright. pydantic's
           serialiser does not, given the declared field types.

        A socket that fails mid-send is left to its own handler: the receive loop
        there raises on the next turn and unregisters it. So one dead connection
        cannot break delivery to this client's other sockets, and there is only
        one place a socket is removed from the registry.
        """
        for ws in self.of(client_id):
            with contextlib.suppress(Exception):
                await send_state(ws, message)


#: The answers a command route gives besides its ack; pass as ``responses=``.
COMMAND_RESPONSES: dict[int | str, dict] = {
    404: {"description": "The client has no live pipeline: its page is not open."},
    409: {"description": "The command does not apply in the pipeline's current state."},
}


def dispatch_command(
    registry: PipelineRegistry, command: Command, logger: logging.Logger
) -> CommandAck:
    """Send one command into its client's pipeline and acknowledge it.

    404 when the client has no pipeline (a command never builds one); 409 when
    the receiver refuses it in its current state, with its reason as the
    detail. Otherwise the command is on the pipeline's bus and the ack says so:
    the effect arrives on the socket, never in this reply.
    """
    client_id = command.client_id or ""
    pipeline = registry.peek(client_id)
    if pipeline is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"no live pipeline for client {client_id}; "
                "open the page (and its WebSocket) before sending commands"
            ),
        )
    try:
        pipeline.dispatch(command)
    except CommandRefused as refused:
        logger.info("client %s: refused %s: %s", client_id, command.name, refused)
        raise HTTPException(status_code=409, detail=str(refused)) from refused
    logger.info("client %s: %s (request %s)", client_id, command.name, command.request_id)
    return CommandAck(applied=command.name)
