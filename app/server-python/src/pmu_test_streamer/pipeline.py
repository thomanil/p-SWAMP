# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The streamer's pipeline definitions: which providers, which enrichers, which modules.

``build_pipeline(key, source)`` is the factory a ``PipelineRegistry`` calls once
per key. Defining a pipeline is writing it: everything it builds is built fresh
for that key. The streamer has one pipeline per **source**, and a source is
nothing but the providers its variable names:

    source  providers (variable, default)                              keyed by
    local   PMU_TEST_STREAMER_LOCAL_CLIENTS: the recording in the image  the client:  "local-<id>"
    live    PMU_TEST_STREAMER_LIVE_CLIENTS:  the synthetic live feed     the stream:  "live", shared

Every pipeline gets the stub CIM reference enricher (PMU_TEST_STREAMER_CIM_REFERENCE,
"none" to switch off) and the stats module -- in-process, or, with
PMU_TEST_STREAMER_MODULE_TRANSPORT set, a RemoteModule standing in for it while
worker.py runs it. The pipeline key is the worker's key too, so two pipelines
never share a module instance.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from typing import Literal

from pswamp_core.bus import InProcessBus
from pswamp_core.datagateway import (
    CimReferenceEnricher,
    Enricher,
    MissingSettingError,
    Player,
    gateway_from_env,
)
from pswamp_core.messages import PmuFrame
from pswamp_core.modules import Module
from pswamp_core.pipeline import Pipeline
from pswamp_core.remote import RemoteModule
from pswamp_core.transport import Transport, transport_from_env

from .stats_module import FrameStatsModule

__all__ = [
    "IDLE_EVICT_SECONDS",
    "LIVE_STREAM",
    "MAX_PIPELINES",
    "MODULE_TRANSPORT_VARIABLE",
    "SOURCES",
    "Source",
    "available",
    "build_pipeline",
    "cim_reference_enrichers",
    "close_module_transport",
    "pipeline_key",
    "stats_modules",
]

Source = Literal["local", "live"]

#: Per source: the variable naming its providers, and what it names when unset.
#: Set a variable to ``none`` to switch that source off.
SOURCES: dict[Source, tuple[str, str]] = {
    "local": (
        "PMU_TEST_STREAMER_LOCAL_CLIENTS",
        "sample:pmu_test_streamer.sample_client:SampleRecordingClient",
    ),
    "live": (
        "PMU_TEST_STREAMER_LIVE_CLIENTS",
        "live:pmu_test_streamer.live_client:LiveSyntheticClient",
    ),
}

#: The key of the one shared live pipeline: a stream name, not a client id.
LIVE_STREAM = "live"

#: The placeholder ``cimReferenceId`` the stub enricher stamps on every frame.
DEFAULT_CIM_REFERENCE = "n44-stub"
CIM_REFERENCE_VARIABLE = "PMU_TEST_STREAMER_CIM_REFERENCE"

#: A transport spec (``kafka:pswamp_core.transport.kafka:KafkaTransport`` plus
#: ``KAFKA_BOOTSTRAP_SERVERS``): the stats module then runs in ``worker.py``.
#: Unset, it runs in this process. The worker reads the same variable.
MODULE_TRANSPORT_VARIABLE = "PMU_TEST_STREAMER_MODULE_TRANSPORT"

#: The registry's bounds; the worker evicts an idle key after the same time.
MAX_PIPELINES = 8
IDLE_EVICT_SECONDS = 300.0

#: The process's one transport to the worker, built on first use.
_TRANSPORT: Transport | None = None


def cim_reference_enrichers() -> list[Enricher]:
    """The gateway's stub CIM reference enricher, unless switched off with ``none``."""
    reference = os.environ.get(CIM_REFERENCE_VARIABLE, "").strip() or DEFAULT_CIM_REFERENCE
    return [] if reference.lower() == "none" else [CimReferenceEnricher(reference)]


def stats_modules(key: str) -> list[Module]:
    """The module list: the stats module itself, or its stand-in when the
    environment sends it to the worker. The only line that differs."""
    global _TRANSPORT
    if _TRANSPORT is None:
        _TRANSPORT = transport_from_env(MODULE_TRANSPORT_VARIABLE)
    if _TRANSPORT is None:
        return [FrameStatsModule()]
    return [RemoteModule(FrameStatsModule, _TRANSPORT, key)]


async def close_module_transport() -> None:
    """Close the transport to the worker, if one was opened; on shutdown."""
    global _TRANSPORT
    transport, _TRANSPORT = _TRANSPORT, None
    if transport is not None:
        await transport.close()


def pipeline_key(source: Source, client_id: str) -> str:
    """Recordings are per client; the live stream is one, whoever watches."""
    return LIVE_STREAM if source == "live" else f"{source}-{client_id}"


def available(source: Source) -> bool:
    """Whether ``source``'s providers are configured and can be built."""
    variable, default = SOURCES[source]
    if os.environ.get(variable, "").strip().lower() == "none":
        return False
    try:
        return bool(gateway_from_env(default, variable=variable).clients)
    except MissingSettingError:
        return False


def build_pipeline(key: str, source: Source = "local", modules: Sequence[Module] | None = None) -> Pipeline:
    """One pipeline for ``key`` over ``source``'s providers: its own gateway,
    bus, player and modules. A live-only gateway makes the player start live."""
    variable, default = SOURCES[source]
    gateway = gateway_from_env(default, variable=variable, enrichers=cim_reference_enrichers())
    bus = InProcessBus()
    player = Player(gateway, bus, model=PmuFrame, loop=True)
    return Pipeline(key, gateway, bus, player, stats_modules(key) if modules is None else modules)
