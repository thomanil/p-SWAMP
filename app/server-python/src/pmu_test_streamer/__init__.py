# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The PMU test streamer: the thin slice of the target data architecture.

The committed sample recording and a synthetic live feed, both behind the
core's provider → gateway → player → bus chain, one pipeline per client, with
the replay commands and the recorded/live switch going up as POSTs and the
state coming down one socket. See ``doc/server-data-architecture.md`` for the
architecture and ``STEP4-WIP-data-integration-impl-for-single-module.md`` for
what this slice covers and what it leaves open.

Same public surface as every app package -- exactly these three names, and
src/server.py uses nothing else:

  router      the endpoints, mounted by server.py under this app's /api/<app> prefix
  lifespan    binds the pipeline registry to the loop; drains it on shutdown
  WS_MESSAGE  the model this app pushes down its socket

Note the spelling difference: this directory is `pmu_test_streamer` because
server.py imports it as a Python module, while its URL prefix is the hyphenated
`/api/pmu-test-streamer`, matching the web client's route.
"""

from .api import PmuStreamState, lifespan, router

WS_MESSAGE = PmuStreamState

__all__ = ["WS_MESSAGE", "PmuStreamState", "lifespan", "router"]
