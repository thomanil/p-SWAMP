# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The data clients shipped with the core.

Only the reference in-memory client today. The draft's ``CsvClient`` and
``KafkaClient`` are deferred (see STEP 4); a provider outside the core --
``pmu_test_streamer.sample_client.SampleRecordingClient`` is the worked
example -- imports :mod:`pswamp_core.datagateway` and nothing else.
"""

from .in_memory import InMemoryClient

__all__ = ["InMemoryClient"]
