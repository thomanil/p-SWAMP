# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""What every Mode estimation pipeline is made of: the ``mode-estimation.*``
topics, a gateway over the N44 recording -- through the islanding stream's
provider, named by its spec string -- and the N4SID module. It gets a worker
of its own in compose and k8s
(``PSWAMP_WORKER_FAMILIES=mode_estimation.family:FAMILY``), with its own CPU
limit and one BLAS thread per identification."""

from __future__ import annotations

from pswamp_core.datagateway import DataGateway, gateway_from_env
from pswamp_core.pipeline import PipelineFamily

from .n4sid_module import N4SIDModule

APP = "mode-estimation"

#: The providers a deployment gets unless MODE_ESTIMATION_DATA_CLIENTS names others:
#: the islanding stream's N44 provider, by spec string, under a name of its own
#: so its settings (``MODES_N44_MEASUREMENTS``) are not the islanding app's.
DEFAULT_DATA_CLIENTS = "modes_n44:islanding_stream.n44_client:N44RecordingClient"
DATA_CLIENTS_VARIABLE = "MODE_ESTIMATION_DATA_CLIENTS"


def gateway() -> DataGateway:
    return gateway_from_env(DEFAULT_DATA_CLIENTS, variable=DATA_CLIENTS_VARIABLE)


FAMILY = PipelineFamily(APP, gateway, (N4SIDModule,))
