# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The N4SID module as its own service: ``python -m mode_estimation.worker``.

The same image as the server, a different command, no port. A ``ModuleHost``
(``pswamp_core.remote``) tails ``pmu.frame`` under this app's topic prefix,
runs one ``N4SIDModule`` per client key it sees, and publishes each result on
``mode.estimation.result`` under that key -- and each ``ErrorEvent`` the module
raises, keep-up and skipped-identification reports included, on
``error.event``. The module code is ``n4sid_module.py``, unchanged; how it
runs its identifications is ``MODE_ESTIMATION_EXECUTION`` (inline, thread,
process) and ``MODE_ESTIMATION_POOL_SIZE``, read here.

Reads the transport from the same variable the server does::

    MODE_ESTIMATION_MODULE_TRANSPORT=modes:pswamp_core.transport.kafka:KafkaTransport \\
    MODES_BOOTSTRAP_SERVERS=kafka:9092 MODES_TOPIC_PREFIX=mode-estimation \\
    python -m mode_estimation.worker

Run from the server's ``src/`` directory, as the image does. Stops on SIGINT or
SIGTERM; exits 2 when the variable is unset. The package's ``__init__``
imports ``api.py``, so this process loads the web stack too, as the other
workers do.
"""

from pswamp_core.remote import main

from .api import IDLE_EVICT_SECONDS, MODULE_TRANSPORT_VARIABLE
from .n4sid_module import N4SIDModule

if __name__ == "__main__":
    raise SystemExit(main(N4SIDModule, MODULE_TRANSPORT_VARIABLE, idle_seconds=IDLE_EVICT_SECONDS))
