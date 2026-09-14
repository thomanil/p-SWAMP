# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Providers, the gateway over them, and the player that paces a stream.

The provider layer is the test_pswamp draft, lifted (see each module's
docstring for what was adapted). The concrete clients are *not* re-exported
here -- they live one level down in ``.clients`` -- so importing the contract
pulls in nothing a provider does not need. ``Player`` and ``conformance`` are
new in this repo.
"""

from .config import (
    DATA_CLIENTS_VARIABLE,
    EnvSetting,
    MissingSettingError,
    env_key,
    gateway_from_env,
)
from .data_client_model import (
    Capability,
    DataClient,
    MRIDFilter,
    ModelSelector,
)
from .data_gateway import DataGateway, ProduceError
from .planner import DataGapError, GapPolicy, Segment, SegmentPlanner
from .player import Player
from .stream import DataStream
from .time_range import Coverage, TimeRange

__all__ = [
    "DATA_CLIENTS_VARIABLE",
    "Capability",
    "Coverage",
    "DataClient",
    "DataGapError",
    "DataGateway",
    "DataStream",
    "EnvSetting",
    "GapPolicy",
    "MRIDFilter",
    "MissingSettingError",
    "ModelSelector",
    "Player",
    "ProduceError",
    "Segment",
    "SegmentPlanner",
    "TimeRange",
    "env_key",
    "gateway_from_env",
]
