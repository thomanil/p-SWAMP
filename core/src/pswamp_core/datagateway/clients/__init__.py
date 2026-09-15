# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The data clients shipped with the core.

The reference in-memory client, its broker stand-in (``InMemoryBroker``, a
broker with no port, for tests and portless configurations), and -- behind
the ``kafka`` extra, so it is never imported here -- ``KafkaClient`` in
:mod:`pswamp_core.datagateway.clients.kafka`, named by dotted path in a
provider spec (``bus:pswamp_core.datagateway.clients.kafka:KafkaClient``).
The draft's ``CsvClient`` is still deferred (see STEP 4); a provider outside
the core -- ``pmu_test_streamer.sample_client.SampleRecordingClient`` is the
worked example -- imports :mod:`pswamp_core.datagateway` and nothing else.
"""

from .in_memory import InMemoryBroker, InMemoryClient

__all__ = ["InMemoryBroker", "InMemoryClient"]
