# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The data clients shipped with the core.

Two: the reference ``InMemoryClient`` (re-exported here), and
``RemoteDataClient`` in ``remote_data`` -- a deployment's own data service
behind a REST api, answering with a streamed response -- which is *not* re-exported,
so that importing this package pulls in nothing the ``remote-data`` extra
provides; name it by module in a ``PSWAMP_DATA_CLIENTS`` spec. The draft's
``CsvClient`` and a broker-as-history ``KafkaClient`` are still deferred (see
STEP 4); a provider outside the core --
``pmu_test_streamer.sample_client.SampleRecordingClient`` is the worked
example -- imports :mod:`pswamp_core.datagateway` and nothing else.
"""

from .in_memory import InMemoryClient

__all__ = ["InMemoryClient"]
