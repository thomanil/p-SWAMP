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

Then what every app over a core pipeline shares, which is all of its plumbing:

- `transport()` is the process's one transport (`PSWAMP_TRANSPORT`; unset, the
  in-memory one), closed by this module's `lifespan`, which `server.py` enters
  as a service before every app and leaves after them.
- `serve_family(family, registry)` is an app's lifespan: it binds the
  registry, forwards the family's `ErrorEvent`s to the layout's error tray
  (`errors.forward_errors`), and -- when the transport is in-memory -- hosts the
  family's modules right here, in this process.
- `connected_pipeline(ws, registry)` and `push_changes(ws, pipeline, build)`
  are a socket endpoint: accept and hold the client's pipeline, then send
  `build()` on connect and on every change, coalesced.
- `dispatch_command` is the one way a POST becomes a command: find the client's
  pipeline (404 without one -- a command never builds a pipeline),
  `pipeline.dispatch` it (409 when the player refuses it now), acknowledge.
  `COMMAND_RESPONSES` documents those two answers on the route, so the
  published contract carries them.

`errors` itself never imports `shared` (see its docstring): the transport is
handed to its forwarder, which is what keeps this from being a cycle.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from errors.forwarder import forward_errors
from errors.hub import HUB
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

from pswamp_core.command_routing import CommandRefused
from pswamp_core.host import hosts_for, serve_hosts
from pswamp_core.messages import Command
from pswamp_core.pipeline import CapacityError, Pipeline, PipelineFamily, PipelineRegistry
from pswamp_core.transport import Transport, transport_from_env
from pswamp_core.util.tasks import cancel_and_wait

__all__ = [
    "CLIENT_ID_PATTERN",
    "COMMAND_RESPONSES",
    "HUB",
    "ClientId",
    "CommandAck",
    "SocketRegistry",
    "connected_pipeline",
    "dispatch_command",
    "get_logger",
    "lifespan",
    "push_changes",
    "read_client_id",
    "send_state",
    "serve_family",
    "transport",
    "wait_for_disconnect",
]

logger = get_logger("shared")


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
    detail. Otherwise the command is on its topic and the ack says so: the
    effect arrives on the socket, never in this reply.
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


# --- the transport, one per process -----------------------------------------------

_TRANSPORT: Transport | None = None


def transport() -> Transport:
    """The process's transport, built on first use from ``PSWAMP_TRANSPORT``."""
    global _TRANSPORT
    if _TRANSPORT is None:
        _TRANSPORT = transport_from_env()
        logger.info(
            "transport: %s (%s)", _TRANSPORT.name,
            "modules hosted in this process" if _TRANSPORT.in_process else "modules hosted by workers",
        )
    return _TRANSPORT


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """Close the transport once every app is done with it."""
    global _TRANSPORT
    try:
        yield
    finally:
        closing, _TRANSPORT = _TRANSPORT, None
        if closing is not None:
            await closing.close()


@contextlib.asynccontextmanager
async def serve_family(family: PipelineFamily, registry: PipelineRegistry) -> AsyncIterator[None]:
    """An app's lifespan: its registry bound to the loop, its errors forwarded
    to the tray, and -- with the in-memory transport -- its modules hosted here.
    On the way out every pipeline stops (each saying ``PipelineClosed``)."""
    registry.bind(asyncio.get_running_loop())
    link = transport()
    tasks = [asyncio.create_task(forward_errors(link, family.app), name=f"{family.app}.errors")]
    if link.in_process:
        tasks.append(asyncio.create_task(serve_hosts(hosts_for(family, link)), name=f"{family.app}.hosts"))
    elif family.modules:
        logger.info(
            "%s: modules %s are hosted by a worker", family.app, ", ".join(m.name for m in family.modules)
        )
    try:
        yield
    finally:
        await registry.stop_all()
        registry.bind(None)
        await cancel_and_wait(*tasks, ignore=(Exception,))


# --- a socket over a pipeline ------------------------------------------------------


@contextlib.asynccontextmanager
async def connected_pipeline(ws: WebSocket, registry: PipelineRegistry) -> AsyncIterator[Pipeline | None]:
    """Accept one socket and hold its client's pipeline for as long as it lives.

    Yields ``None`` when the connection was refused. The ordering of ``accept``
    is the point: no usable client id is closed *before* accepting (1008); at
    capacity the socket is accepted first and then closed with 1013, because a
    code only reaches the browser on an established connection, and the web
    client treats 1013 as terminal; a pipeline that fails to start is 1011.
    """
    client_id = read_client_id(ws)
    if client_id is None:
        await ws.close(code=1008)  # policy violation
        yield None
        return
    await ws.accept()
    try:
        pipeline = await registry.acquire(client_id)
    except CapacityError:
        logger.warning("refused client %s: all %s pipelines in use", client_id, registry.max_pipelines)
        await ws.close(code=1013)  # try again later
        yield None
        return
    except Exception:
        logger.exception("failed to start pipeline for client %s", client_id)
        await ws.close(code=1011)  # unexpected server error
        yield None
        return
    try:
        yield pipeline
    finally:
        registry.release(client_id)


async def push_changes(
    ws: WebSocket,
    pipeline: Pipeline,
    build: Callable[[], BaseModel],
    *,
    min_interval: float = 0.0,
    tick: float | None = None,
) -> None:
    """Send ``build()`` now and on every change of ``pipeline`` until the
    client disconnects.

    Coalesces: a wake-up means "something changed", and the message is built
    from the pipeline's current state, so however much arrived meanwhile one
    message goes. ``min_interval`` caps the rate (a 50x replay changes 2500
    times a second); ``tick`` sends anyway after that long without a change
    (readings that move with the clock, not with a message). A client that is
    gone by the time a message is sent ends the connection, quietly: that is a
    disconnect, not an error.
    """
    with pipeline.changes() as changes:
        try:
            await send_state(ws, build())
        except WebSocketDisconnect:
            return

        async def push() -> None:
            while True:
                await changes.wait(tick)
                await send_state(ws, build())
                if min_interval:
                    await asyncio.sleep(min_interval)

        pusher = asyncio.create_task(push())
        try:
            await wait_for_disconnect(ws)
        finally:
            await cancel_and_wait(pusher, ignore=(WebSocketDisconnect,))
