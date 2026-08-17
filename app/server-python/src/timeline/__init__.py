"""The timeline app: a scrolling number sequence with playback controls.

An app package's public surface is exactly these two names, and src/server.py
uses nothing else:

  router    the endpoints, mounted by server.py under this app's /api/<app> prefix
  lifespan  optional; startup/shutdown work (here: the playback ticker task)

Keeping the surface that small is what lets server.py stay a plain registry —
copy this file when adding an app package.
"""

from .api import lifespan, router

__all__ = ["lifespan", "router"]
