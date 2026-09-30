# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""What every Frequency peek pipeline is made of: the ``frequency-peek.*``
topics, a gateway whose first source is the live feed (so a pipeline tails it
from the start), and the frequency module. A worker hosts it with
``PSWAMP_WORKER_FAMILIES=frequency_peek.family:FAMILY``."""

from __future__ import annotations

from pswamp_core.datagateway import DataGateway, gateway_from_env
from pswamp_core.pipeline import PipelineFamily

from .frequency_module import FrequencyModule

APP = "frequency-peek"

#: The sources a deployment gets unless FREQUENCY_PEEK_DATA_CLIENTS names
#: others: the live feed the page shows -- first, so it is the initial source --
#: and the recording beside it.
DEFAULT_DATA_CLIENTS = (
    "live:pmu_test_streamer.live_client:LiveSyntheticClient,"
    "sample:pmu_test_streamer.sample_client:SampleRecordingClient"
)
DATA_CLIENTS_VARIABLE = "FREQUENCY_PEEK_DATA_CLIENTS"


def gateway() -> DataGateway:
    return gateway_from_env(DEFAULT_DATA_CLIENTS, variable=DATA_CLIENTS_VARIABLE)


FAMILY = PipelineFamily(APP, gateway, (FrequencyModule,))
