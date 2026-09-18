# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The stats module as its own service: ``python -m pmu_test_streamer.worker``.

The same image as the server, a different command, no port. A ``ModuleHost``
(``pswamp_core.remote``) tails the module's input topic, runs one
``FrameStatsModule`` per client key it sees -- primed by the header the server
side sent ahead -- and publishes each result on the output topic under that
key, where the client's pipeline picks it up. The module code is
``stats_module.py``, unchanged.

Reads the transport from the same variable the server does::

    PMU_TEST_STREAMER_MODULE_TRANSPORT=kafka:pswamp_core.transport.kafka:KafkaTransport \\
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092  python -m pmu_test_streamer.worker

Run from the server's ``src/`` directory, as the image does. Stops on SIGINT or
SIGTERM; exits 2 when the variable is unset.
"""

from pswamp_core.remote import main

from .api import IDLE_EVICT_SECONDS, MODULE_TRANSPORT_VARIABLE
from .stats_module import FrameStatsModule

if __name__ == "__main__":
    raise SystemExit(main(FrameStatsModule, MODULE_TRANSPORT_VARIABLE, idle_seconds=IDLE_EVICT_SECONDS))
