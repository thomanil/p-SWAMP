"""The PMU test streamer: the worked example of the server data architecture.

A recorded and a live PMU feed through one core pipeline per client, a stats
module on the bus (in-process or in a worker), the replay commands and a module
command going up as POSTs, the state coming down one socket. See
``doc/server-data-architecture.md``. The pieces:

  sample_client.py, live_client.py   two providers, written outside the core
  stats_module.py                    the module, and its ResetStatsCommand
  pipeline.py                        the pipeline definition
  worker.py                          the module as its own service
  api.py                             the web edge

Same public surface as every app package — exactly these three names, and
src/server.py uses nothing else:

  router      the endpoints, mounted by server.py under this app's /api/<app> prefix
  lifespan    optional; startup/shutdown work (here: the pipeline registry)
  WS_MESSAGE  optional; the model this app pushes down its socket

Note the spelling difference: this directory is `pmu_test_streamer` because
server.py imports it as a Python module, while its URL prefix is the hyphenated
`/api/pmu-test-streamer`, matching the web client's route.
"""

from .api import PmuStreamState, lifespan, router

WS_MESSAGE = PmuStreamState

__all__ = ["WS_MESSAGE", "PmuStreamState", "lifespan", "router"]
