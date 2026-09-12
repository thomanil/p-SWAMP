# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Contract 2: time-aware data access stitched across heterogeneous backends."""

from .client import (
    Capability,
    DataClient,
    ModelSelector,
    MRIDFilter,
    normalise_models,
    normalise_mrid_filter,
)
from .config import (
    CLIENTS_SETTING,
    EnvSetting,
    MissingSettingError,
    build_gateway_from_env,
    env_key,
    resolve_client_type,
)
from .gateway import DataGateway
from .jobs import new_job_id, run_batch_job
from .pacing import Pacer, PacerInterrupted
from .planner import DataGapError, GapPolicy, Segment, SegmentPlanner
from .replay import Replay
from .stream import DataStream
from .time_range import Coverage, TimeRange

__all__ = [
    "CLIENTS_SETTING",
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
    "Pacer",
    "PacerInterrupted",
    "Replay",
    "Segment",
    "SegmentPlanner",
    "TimeRange",
    "build_gateway_from_env",
    "env_key",
    "new_job_id",
    "normalise_models",
    "normalise_mrid_filter",
    "resolve_client_type",
    "run_batch_job",
]
