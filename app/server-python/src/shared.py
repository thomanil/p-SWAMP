"""Helpers shared by the app packages under src/.

Not an app package: it exposes no `router` and is never listed in `APPS` — it is
just the home for pieces every app would otherwise copy. Keep it strictly
domain-free; anything that knows about timelines, PMU records, or any one app's
state belongs in that app's own package.

Currently: WebSocket connection bookkeeping and the stdout logger setup.
"""

import logging
import sys

from fastapi import WebSocket


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
