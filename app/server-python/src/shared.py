"""Helpers shared by the app packages under src/.

Not an app package: it exposes no `router` and is never listed in `APPS` — it is
just the home for pieces every app would otherwise copy. Keep it strictly
domain-free; anything that knows about timelines, PMU records, or any one app's
state belongs in that app's own package.

Currently: WebSocket connection bookkeeping, the stdout logger setup, and the
two pieces every REST command endpoint needs (`ClientId` and `CommandAck`).
"""

import logging
import sys
from typing import Annotated, Literal

from fastapi import Query, WebSocket
from pydantic import BaseModel


def make_logger(name: str) -> logging.Logger:
    """A logger writing to stdout, which Docker/k8s capture verbatim (`docker
    logs`, `kubectl logs`).

    It owns its own handler and does not propagate, so it behaves the same however
    the app is launched (uvicorn programmatically from server.py, `uvicorn
    server:app`, or `uv run`) and never double-prints through uvicorn's root
    config. Idempotent: called again with the same name, it returns the configured
    logger untouched, so an import cycle or a module reload can't stack handlers
    and print every line twice.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                f"%(asctime)s %(levelname)s [{name}] %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


class ConnectionManager:
    """Live sockets per client id. Pure transport: it knows nothing about what is
    being sent, so every app gets its own instance and keeps its own state
    alongside it."""

    def __init__(self) -> None:
        # One client (seed) may briefly have several live sockets — e.g. a
        # reconnect that overlaps the dying old one — so map each id to a set.
        self.conns: dict[int, set[WebSocket]] = {}

    async def connect(self, ws: WebSocket, client_id: int) -> None:
        await ws.accept()
        self.conns.setdefault(client_id, set()).add(ws)

    def disconnect(self, ws: WebSocket, client_id: int) -> None:
        # Drop only the live socket. The caller's per-client state is deliberately
        # left alone, so the client resumes where it was when it reconnects with
        # the same seed.
        sockets = self.conns.get(client_id)
        if sockets is None:
            return
        sockets.discard(ws)
        if not sockets:
            del self.conns[client_id]

    async def send_to_client(self, client_id: int, message: dict) -> None:
        # Iterate a snapshot; drop any socket that fails mid-send so one dead
        # connection can't break delivery to this client's other sockets.
        dead: list[WebSocket] = []
        for ws in list(self.conns.get(client_id, set())):
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws, client_id)


# --- REST command plumbing --------------------------------------------------
#
# Commands travel upstream as POSTs; state comes back down the socket. See the
# "commands up, state down" invariant in AGENTS.md for why the two directions are
# split across two transports.
#
# Both names below exist so that every command endpoint declares its caller and
# its reply the same way -- which is also what makes the whole command surface
# describable, when the OpenAPI/Swagger layer goes in on top of this.
#
# The pswamp_web package deliberately keeps its own twin of both in
# pswamp_web/wire.py: it may not import anything from the rest of the web backend,
# because it is written to move into the desktop package as pswamp/web/. Change
# one of these and change the other.


ClientId = Annotated[
    int,
    Query(
        alias="client_id",
        ge=1,
        description=(
            "The browser's client id -- the same value its WebSocket sends, "
            "resolved once per browser profile in app/client-web/src/lib/clientId.ts."
        ),
    ),
]
"""The caller's identity, as a validated query parameter.

Deliberately the same `?client_id=` the sockets already carry, rather than a
header or a body field: it mirrors the WebSocket convention exactly, and it lands
in the access log, which is half the reason these commands became HTTP requests.

Not authentication and not pretending to be -- supply someone else's id and you
drive their state. FastAPI rejects a missing or non-numeric one with a 422 before
any handler runs, which is the whole validation story.
"""


class CommandAck(BaseModel):
    """The reply to every command POST.

    Deliberately NOT the resulting state. That arrives on the socket, on the
    server's own schedule, so there is exactly one path for state and no ordering
    for a client to reconcile between two of them. What this carries is
    only "the command was understood and applied, and here is what it was" --
    enough to log and to assert on, and nothing a page should render.
    """

    status: Literal["ok"] = "ok"
    applied: str
