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

    from shared import ClientId, CommandAck, ConnectionManager, get_logger

What is genuinely defined here is `ConnectionManager` — the scaffold apps' socket
bookkeeping, which `pswamp_web/` has no use for because its pages push from their
own per-connection task rather than fanning out to a client's sockets.
"""

from fastapi import WebSocket
from pydantic import BaseModel

from pswamp_web.log import get_logger
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
    "ConnectionManager",
    "get_logger",
    "read_client_id",
    "send_state",
]


class ConnectionManager:
    """Live sockets per client id. Pure transport: it knows nothing about what is
    being sent, so every app gets its own instance and keeps its own state
    alongside it."""

    def __init__(self) -> None:
        # One client (seed) may briefly have several live sockets — e.g. a
        # reconnect that overlaps the dying old one — so map each id to a set.
        self.conns: dict[str, set[WebSocket]] = {}

    async def connect(self, ws: WebSocket, client_id: str) -> None:
        await ws.accept()
        self.conns.setdefault(client_id, set()).add(ws)

    def disconnect(self, ws: WebSocket, client_id: str) -> None:
        # Drop only the live socket. The caller's per-client state is deliberately
        # left alone, so the client resumes where it was when it reconnects with
        # the same seed.
        sockets = self.conns.get(client_id)
        if sockets is None:
            return
        sockets.discard(ws)
        if not sockets:
            del self.conns[client_id]

    async def send_to_client(self, client_id: str, message: BaseModel) -> None:
        """Push one message to every live socket this client holds.

        A pydantic model, not a dict, and serialised through `send_state` rather
        than `ws.send_json`. Two reasons, and the second is the one that bites:

        1. The model IS the published schema. Every socket payload in this repo
           is declared as a model and picked up by api_contract.py from its
           package's `WS_MESSAGE`, so a message built as a loose dict would be
           absent from the contract the web client generates its types from.
        2. `send_json` routes through `json.dumps`, which emits bare `NaN` and
           `Infinity` tokens that `JSON.parse` rejects outright. pydantic's
           serialiser does not, given the declared field types.
        """
        # Iterate a snapshot; drop any socket that fails mid-send so one dead
        # connection can't break delivery to this client's other sockets.
        dead: list[WebSocket] = []
        for ws in list(self.conns.get(client_id, set())):
            try:
                await send_state(ws, message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws, client_id)
