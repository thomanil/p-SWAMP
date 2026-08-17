"""The __LABEL__ app.

Exposes `router`, which server.py mounts under /api/__SLUG__. Add a `lifespan`
asynccontextmanager here too if this app ever needs startup/shutdown work — see
src/timeline/__init__.py.
"""

from .api import router

__all__ = ["router"]
