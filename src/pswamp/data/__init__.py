# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The data layer of p-SWAMP: messages, providers, gateway, bus.

Contracts 1–3 of ``STEP3-WIP-data-integration-propose-full-architecture.md``,
landed as the thin slice described in
``STEP-4-WIP-data-integration-sample-slice-implementation.md``. The provider and
gateway half is Louis Pauchet's design (SINTEF, ``../test_pswamp``), lifted
largely as written; see each module's header for what changed.

Rules this package keeps, and why:

- **It is movable.** Relative imports inside, nothing imported from outside
  ``pswamp``, no FastAPI — the same rule ``pswamp_web`` obeys, so where the
  shared Python finally lives (port document §7) stays an open question that
  costs one ``git mv`` either way.
- **Nothing in it needs a broker, a database or a second process.** The bus is
  the in-memory client; anything else is a client a deployment registers.
- **It is provable without a web server.** Every piece has hermetic tests under
  ``app/server-python/tests/`` (where the locked pytest lives) that import only
  this package.

Import the public surface from here::

    from pswamp.data import DataGateway, InMemoryClient, Sample, StreamHeader, Replay
"""

from .clients import BUILTIN_CLIENTS, BUS_CAPABILITIES, InMemoryClient
from .conformance import check_client
from .gateway import (
    CLIENTS_SETTING,
    Capability,
    Coverage,
    DataClient,
    DataGapError,
    DataGateway,
    DataStream,
    EnvSetting,
    MissingSettingError,
    MRIDFilter,
    Pacer,
    PacerInterrupted,
    Replay,
    TimeRange,
    build_gateway_from_env,
    env_key,
    new_job_id,
    resolve_client_type,
    run_batch_job,
)
from .models import (
    DEFAULT_NAMESPACE,
    StreamChannel,
    DataModel,
    JobAck,
    ModuleRef,
    Report,
    Result,
    Sample,
    StreamHeader,
    Unit,
    Value,
    catalogue,
)
from .time import ensure_utc, utcnow

__all__ = [
    "BUILTIN_CLIENTS",
    "BUS_CAPABILITIES",
    "CLIENTS_SETTING",
    "DEFAULT_NAMESPACE",
    "Capability",
    "StreamChannel",
    "Coverage",
    "DataClient",
    "DataGapError",
    "DataGateway",
    "DataModel",
    "DataStream",
    "EnvSetting",
    "InMemoryClient",
    "JobAck",
    "MRIDFilter",
    "MissingSettingError",
    "ModuleRef",
    "Pacer",
    "PacerInterrupted",
    "Replay",
    "Report",
    "Result",
    "Sample",
    "StreamHeader",
    "TimeRange",
    "Unit",
    "Value",
    "build_gateway_from_env",
    "catalogue",
    "check_client",
    "ensure_utc",
    "env_key",
    "new_job_id",
    "resolve_client_type",
    "run_batch_job",
    "utcnow",
]
