"""The PMU streamer app: retransmit a live broker topic over a WebSocket.

Part of the Kafka-vs-NATS experiment (see brokers/ and producer.py): a separate
producer loops sample_data.txt into both a Kafka topic and a NATS subject, and
this app consumes from whichever pipe the client selected.

Same public surface as every app package — exactly these three names, and
src/server.py uses nothing else:

  router      the endpoints, mounted by server.py under this app's /api/<app> prefix
  lifespan    optional; here it stops any live consumers on shutdown
  WS_MESSAGE  the pydantic model this app pushes down its socket

Note the spelling difference: this directory is `pmu_test_streamer` because
server.py imports it as a Python module, while its URL prefix is the hyphenated
`/api/pmu-test-streamer`, matching the web client's route.
"""

from .api import PmuStreamState, lifespan, router

WS_MESSAGE = PmuStreamState

__all__ = ["WS_MESSAGE", "PmuStreamState", "lifespan", "router"]
