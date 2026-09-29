# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Providers, the gateway over them, and the player that paces a stream.

The concrete clients are *not* re-exported here -- they live in ``.clients`` --
so importing the contract pulls in nothing a provider does not need.
"""

from ..settings import EnvSetting, MissingSettingError
from .config import DATA_CLIENTS_VARIABLE, gateway_from_env
from .data_client_model import Capability, DataClient, ModelSelector, MRIDFilter
from .data_gateway import DataGateway
from .stream import DataStream
from .time_range import Coverage, TimeRange

__all__ = [
    "DATA_CLIENTS_VARIABLE",
    "Capability",
    "Coverage",
    "DataClient",
    "DataGateway",
    "DataStream",
    "EnvSetting",
    "MRIDFilter",
    "MissingSettingError",
    "ModelSelector",
    "TimeRange",
    "gateway_from_env",
]
