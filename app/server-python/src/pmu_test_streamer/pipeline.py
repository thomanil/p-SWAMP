# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The streamer's pipeline definition: which providers, which enrichers, which modules.

``build_pipeline(key)`` is the factory a ``PipelineRegistry`` calls once per
key. Defining a pipeline is writing this function: everything it builds is
built fresh for that key.

    providers  ── PSWAMP_DATA_CLIENTS, defaulting to the recording + the synthetic live feed
    enrichers  ── the stub CIM reference (PMU_TEST_STREAMER_CIM_REFERENCE, "none" to switch off)
    player     ── loops the recording; "live" switches to the feed
    modules    ── FrameStatsModule, in-process -- or, with PMU_TEST_STREAMER_MODULE_TRANSPORT
                  set, a RemoteModule standing in for it while worker.py runs it
"""

from __future__ import annotations

import os
from collections.abc import Sequence

from pswamp_core.bus import InProcessBus
from pswamp_core.datagateway import CimReferenceEnricher, Enricher, Player, gateway_from_env
from pswamp_core.messages import PmuFrame
from pswamp_core.modules import Module
from pswamp_core.pipeline import Pipeline
from pswamp_core.remote import RemoteModule
from pswamp_core.transport import Transport, transport_from_env

from .average_module import RangeAverageModule
from .stats_module import FrameStatsModule

__all__ = [
    "DEFAULT_DATA_CLIENTS",
    "IDLE_EVICT_SECONDS",
    "MAX_PIPELINES",
    "MODULE_TRANSPORT_VARIABLE",
    "build_pipeline",
    "cim_reference_enrichers",
    "close_module_transport",
    "stats_modules",
]

#: The providers when ``PSWAMP_DATA_CLIENTS`` is unset: the committed recording
#: (history) and a synthetic live feed of the same rows.
DEFAULT_DATA_CLIENTS = (
    "sample:pmu_test_streamer.sample_client:SampleRecordingClient,"
    "live:pmu_test_streamer.live_client:LiveSyntheticClient"
)

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


def recording_modules(key: str) -> list[Module]:
    """A recording's modules: the stats module (here or in the worker) and the
    batch average (always here: it reads the gateway)."""
    return [*stats_modules(key), RangeAverageModule()]


async def close_module_transport() -> None:
    """Close the transport to the worker, if one was opened; on shutdown."""
    global _TRANSPORT
    transport, _TRANSPORT = _TRANSPORT, None
    if transport is not None:
        await transport.close()


def build_pipeline(key: str, modules: Sequence[Module] | None = None) -> Pipeline:
    """One pipeline for ``key``: its own gateway, bus, player and modules."""
    gateway = gateway_from_env(DEFAULT_DATA_CLIENTS, enrichers=cim_reference_enrichers())
    bus = InProcessBus()
    player = Player(gateway, bus, model=PmuFrame, loop=True)
    return Pipeline(key, gateway, bus, player, recording_modules(key) if modules is None else modules)
