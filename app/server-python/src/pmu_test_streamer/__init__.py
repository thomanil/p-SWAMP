"""The PMU test streamer app: streams sample grid records line by line.

Same public surface as every app package — exactly these two names, and src/server.py
uses nothing else:

  router    the endpoints, mounted by server.py under this app's /api/<app> prefix
  lifespan  optional; startup/shutdown work (here: the streaming ticker task)

Note the spelling difference: this directory is `pmu_test_streamer` because
server.py imports it as a Python module, while its URL prefix is the hyphenated
`/api/pmu-test-streamer`, matching the web client's route.
"""

from .api import lifespan, router

__all__ = ["lifespan", "router"]
