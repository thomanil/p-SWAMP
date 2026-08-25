"""Helpers shared by the app packages under src/.

Not an app package: it exposes no `router` and is never listed in `APPS` — it is
just the home for pieces every app would otherwise copy. Keep it strictly
domain-free; anything that knows about timelines, PMU records, or any one app's
state belongs in that app's own package.

Currently: WebSocket connection bookkeeping, the stdout logger setup, and the
pieces every REST command endpoint needs (`ClientId`, `CommandAck`, and the
`CLIENT_ID_PATTERN` / `read_client_id` pair the sockets validate with).
"""

import logging
import sys
from typing import Annotated, Literal

from fastapi import Query, WebSocket
from pydantic import BaseModel, ConfigDict


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

        A pydantic model, not a dict, and serialised with pydantic rather than
        `ws.send_json`. Two reasons, and the second is the one that bites:

        1. The model IS the published schema. Every socket payload in this repo
           is declared as a model and picked up by api_contract.py from its
           package's `WS_MESSAGE`, so a message built as a loose dict would be
           absent from the contract the web client generates its types from.
        2. `send_json` routes through `json.dumps`, which emits bare `NaN` and
           `Infinity` tokens that `JSON.parse` rejects outright. pydantic's
           serialiser does not, given the declared field types. This mirrors
           `pswamp_web/wire.py:send_state`, which exists for exactly that reason.
        """
        payload = message.model_dump_json()
        # Iterate a snapshot; drop any socket that fails mid-send so one dead
        # connection can't break delivery to this client's other sockets.
        dead: list[WebSocket] = []
        for ws in list(self.conns.get(client_id, set())):
            try:
                await ws.send_text(payload)
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


# The one spelling of a client id, shared by the query parameter below and the
# socket parser beside it. Numeric and bounded: the value ends up in log lines,
# and in pswamp_web also in thread names. Kept character-for-character identical
# to the twin in pswamp_web/wire.py -- see the note above.
CLIENT_ID_PATTERN = r"^\d{1,20}$"


def read_client_id(ws: WebSocket) -> str | None:
    """The client id from a socket's ``?client_id=``, or None if unusable.

    The exact rule :data:`ClientId` applies to a command's query parameter, so a
    page's socket and its commands can never address different state. The twin
    for the p-SWAMP pipelines is `pswamp_web/hub.py:read_client_id`.
    """
    raw = ws.query_params.get("client_id")
    if raw is None:
        return None
    raw = raw.strip()
    if not raw.isdigit() or len(raw) > 20:
        return None
    return raw


ClientId = Annotated[
    str,
    Query(
        alias="client_id",
        pattern=CLIENT_ID_PATTERN,
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

A *string* matching :data:`CLIENT_ID_PATTERN`, identical to the twin in
`pswamp_web/wire.py`. These used to disagree -- `int` with `ge=1` here, `str` with
this pattern there -- which published two different schemas for one browser-wide
identity, and disagreed on whether "0" was valid. One identity, one type: the
generated contract now says the same thing about `client_id` whichever app you
read.

Not authentication and not pretending to be -- supply someone else's id and you
drive their state. FastAPI rejects a missing or malformed one with a 422 before
any handler runs, which is the whole validation story.
"""


class CommandAck(BaseModel):
    """The reply to every command POST.

    Deliberately NOT the resulting state. That arrives on the socket, on the
    server's own schedule, so there is exactly one path for state and no ordering
    for a client to reconcile between two of them. What this carries is
    only "the command was understood and applied, and here is what it was" --
    enough to log and to assert on, and nothing a page should render.

    The explicit title is what keeps the deliberate twin in `pswamp_web/wire.py`
    from colliding with this one in the generated contract: two classes of the
    same name would otherwise be disambiguated into machine-chosen spellings that
    change whenever the module layout does.
    """

    model_config = ConfigDict(title="CommandAck")

    status: Literal["ok"] = "ok"
    applied: str
