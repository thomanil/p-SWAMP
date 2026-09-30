# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""What every PMU test streamer pipeline is made of.

The app's topics are ``pmu-test-streamer.*``; its gateway holds the two
sources, the recording first (so a pipeline starts on it) and the synthetic
live feed; its one module is the frame statistics. The server builds a
pipeline per client from this; whoever hosts the modules -- the server itself
with the in-memory transport, a worker with a broker
(``PSWAMP_WORKER_FAMILIES=pmu_test_streamer.family:FAMILY``) -- hosts them
from this too.
"""

from __future__ import annotations

from pswamp_core.datagateway import DataGateway, gateway_from_env
from pswamp_core.pipeline import PipelineFamily

from .stats_module import FrameStatsModule

APP = "pmu-test-streamer"

#: The sources a deployment gets unless PSWAMP_DATA_CLIENTS names others: the
#: recording (history, and the initial source, being named first) plus the
#: synthetic live feed.
DEFAULT_DATA_CLIENTS = (
    "sample:pmu_test_streamer.sample_client:SampleRecordingClient,"
    "live:pmu_test_streamer.live_client:LiveSyntheticClient"
)


def gateway() -> DataGateway:
    return gateway_from_env(DEFAULT_DATA_CLIENTS)


FAMILY = PipelineFamily(APP, gateway, (FrameStatsModule,))
