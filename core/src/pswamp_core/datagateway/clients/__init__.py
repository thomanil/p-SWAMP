# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The data clients shipped with the core: today the reference ``InMemoryClient``.

A provider outside the core imports :mod:`pswamp_core.datagateway` and nothing
else; ``pmu_test_streamer.sample_client.SampleRecordingClient`` is the example.
"""

from .in_memory import InMemoryClient

__all__ = ["InMemoryClient"]
