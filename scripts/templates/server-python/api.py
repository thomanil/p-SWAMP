"""Backend for the __LABEL__ app.

Scaffolded by scripts/generate-new-subapp.sh: a per-client counter to bump and
reset, the smallest thing that proves the wiring. Replace it with what this app
really does, keeping the shape — state per client_id, in memory only, and one
message pushed on connect and after every change.

server.py owns the prefix, so the "/ws" below is served at:

    /api/__SLUG__/ws
"""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from shared import ConnectionManager, make_logger

from .model import __NAME__Model

logger = make_logger("__SLUG__")
manager = ConnectionManager()
router = APIRouter()

# One model per client seed (the ?client_id= URL param), kept across reconnects
# and never evicted. Nothing is persisted, so a restart resets every client.
states: dict[int, __NAME__Model] = {}


def get_state(client_id: int) -> __NAME__Model:
    if client_id not in states:
        states[client_id] = __NAME__Model()
    return states[client_id]


def state_message(client_id: int, model: __NAME__Model) -> dict:
    """The one message shape this app pushes. Keys are snake_case on the wire and
    the page's hook maps them to camelCase, so changing one means changing both."""
    return {"type": "state", "count": model.count, "client_id": client_id}


async def handle_command(client_id: int, msg: dict) -> None:
    model = get_state(client_id)
    action = msg.get("action")
    if action == "bump":
        model.bump()
    elif action == "reset":
        model.reset()
    else:
        logger.warning("client %s unknown action: %r", client_id, action)
        return  # unknown action -> no state change, no send
    logger.info("client %s: %s (count=%s)", client_id, action, model.count)
    await manager.send_to_client(client_id, state_message(client_id, model))


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    # The client identifies itself with an integer seed: ?client_id=<seed>.
    try:
        client_id = int(ws.query_params.get("client_id"))
    except (TypeError, ValueError):
        await ws.close(code=1008)  # policy violation
        return

    await manager.connect(ws, client_id)
    logger.info("client %s: connected", client_id)
    try:
        await ws.send_json(state_message(client_id, get_state(client_id)))
        while True:
            await handle_command(client_id, await ws.receive_json())
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        manager.disconnect(ws, client_id)
        logger.info("client %s: disconnected", client_id)
