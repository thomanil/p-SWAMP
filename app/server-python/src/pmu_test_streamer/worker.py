# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The stats module as its own service: ``python -m pmu_test_streamer.worker``.

The same image as the server, a different command, no port. A ``ModuleHost``
tails the module's input topic, runs one ``FrameStatsModule`` per pipeline key
it sees, and publishes each result under that key, where the pipeline's
``RemoteModule`` picks it up. The module code is ``stats_module.py``, unchanged.

Reads the transport from the same variable the server does::

    PMU_TEST_STREAMER_MODULE_TRANSPORT=kafka:pswamp_core.transport.kafka:KafkaTransport \\
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092  python -m pmu_test_streamer.worker

Run from the server's ``src/`` directory, as the image does.
"""

from pswamp_core.remote import main

from .stats_module import FrameStatsModule

#: Names the transport, on both sides: set, the server's pipeline holds a
#: RemoteModule and this worker runs the module; unset, it runs in-process.
MODULE_TRANSPORT_VARIABLE = "PMU_TEST_STREAMER_MODULE_TRANSPORT"

if __name__ == "__main__":
    raise SystemExit(main(FrameStatsModule, MODULE_TRANSPORT_VARIABLE))
