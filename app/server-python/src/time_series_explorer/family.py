# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""What every Timeseries Db Explorer pipeline is made of: the
``time-series-explorer.*`` topics, a gateway over the configured history
source, and the row-count module -- which reads a gateway built by this same
factory wherever it is hosted, so a worker hosting it
(``PSWAMP_WORKER_FAMILIES=time_series_explorer.family:FAMILY``) needs the same
``TIME_SERIES_EXPLORER_DATA_CLIENTS`` block as the server."""

from __future__ import annotations

from pswamp_core.datagateway import DataGateway, gateway_from_env
from pswamp_core.pipeline import PipelineFamily

from .row_count_module import RowCountModule

APP = "time-series-explorer"

#: The sources a deployment gets unless TIME_SERIES_EXPLORER_DATA_CLIENTS names
#: others: the streamer's sample recording, history only. Compose and k8s
#: replace it with the Remote Data Client over the stub service.
DEFAULT_DATA_CLIENTS = "sample:pmu_test_streamer.sample_client:SampleRecordingClient"
DATA_CLIENTS_VARIABLE = "TIME_SERIES_EXPLORER_DATA_CLIENTS"


def gateway() -> DataGateway:
    return gateway_from_env(DEFAULT_DATA_CLIENTS, variable=DATA_CLIENTS_VARIABLE)


FAMILY = PipelineFamily(APP, gateway, (RowCountModule,))
