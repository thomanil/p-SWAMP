"""Helpers shared by the app packages under src/.

Not an app package: it exposes no `router` and is never listed in `APPS` — it is
just the home for pieces every app would otherwise copy. Keep it strictly
domain-free; anything that knows about timelines, PMU records, or any one app's
state belongs in that app's own package.

Most of what an app package needs is *defined* one level down, in `pswamp_web/`,
and re-exported here. That looks backwards and is not:

`pswamp_web/` is written to move into the desktop package as `pswamp/web/`, so
nothing inside it may import from the rest of this backend. The rule is one-way,
though — it says nothing about importing *inward*, which is what this module
does, and which stays legal after the move (the web backend already depends on
`p-swamp`, so the import becomes `from pswamp.web.wire import ...` and nothing
else changes).

Getting that direction right is what removed four "change one, change the other"
duplicates: `ClientId`, `CommandAck`, the client-id parser, and the stdout
logger were each declared twice, and ~90 lines in `api_contract.py` existed to
reunify two of them in the published contract. One definition each now.

So an app package imports from here and needs to know nothing about the layout:

    from shared import ClientId, CommandAck, SocketRegistry, get_logger

What is genuinely defined here is `SocketRegistry` — the scaffold apps' socket
bookkeeping, which `pswamp_web/` has no use for because its pages push from their
own per-connection task rather than fanning out to a client's sockets.
"""

import contextlib
from collections.abc import AsyncIterator

from fastapi import WebSocket
from pydantic import BaseModel

from pswamp_web.log import get_logger
from pswamp_web.pump import wait_for_disconnect
from pswamp_web.sessions import SessionRegistry
from pswamp_web.wire import (
    CLIENT_ID_PATTERN,
    ClientId,
    CommandAck,
    read_client_id,
    send_state,
)

__all__ = [
    "CLIENT_ID_PATTERN",
    "ClientId",
    "CommandAck",
    "SocketRegistry",
    "get_logger",
    "read_client_id",
    "send_state",
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
