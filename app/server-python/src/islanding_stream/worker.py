# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The islanding module as its own service: ``python -m islanding_stream.worker``.

The same image as the server, a different command, no port. A ``ModuleHost``
(``pswamp_core.remote``) tails ``pmu.frame``, runs one ``IslandingModule`` per
client key it sees, and publishes each result on ``islanding.stream.result``
under that key -- and each ``ErrorEvent`` the module raises, keep-up reports
included, on ``error.event``, where the client's pipeline picks it up for the
error tray. The module code is ``islanding_module.py``, unchanged.

Reads the transport from the same variable the server does::

    ISLANDING_STREAM_MODULE_TRANSPORT=islanding:pswamp_core.transport.kafka:KafkaTransport \\
    ISLANDING_BOOTSTRAP_SERVERS=kafka:9092 ISLANDING_TOPIC_PREFIX=islanding-stream \\
    python -m islanding_stream.worker

The prefix keeps this worker's topics apart from the streamer's stats-worker,
which tails the unprefixed ``pmu.frame`` under the same client ids.

Run from the server's ``src/`` directory, as the image does. Stops on SIGINT or
SIGTERM; exits 2 when the variable is unset.

The package's ``__init__`` imports ``api.py``, so this process loads the web
stack too, exactly as the streamer's worker does; only the module and the
core run.
"""

from pswamp_core.remote import main

from .api import IDLE_EVICT_SECONDS, MODULE_TRANSPORT_VARIABLE
from .islanding_module import IslandingModule

if __name__ == "__main__":
    raise SystemExit(main(IslandingModule, MODULE_TRANSPORT_VARIABLE, idle_seconds=IDLE_EVICT_SECONDS))
