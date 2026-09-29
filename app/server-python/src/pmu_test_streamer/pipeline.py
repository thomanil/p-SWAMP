# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The streamer's pipeline definition: which providers, which enrichers, which modules.

``build_pipeline(key)`` is the factory a ``PipelineRegistry`` calls once per
key. Defining a pipeline is writing this function: everything it builds is
built fresh for that key.

    providers  ── PSWAMP_DATA_CLIENTS, defaulting to the recording + the synthetic live feed
    enrichers  ── the stub CIM reference (PMU_TEST_STREAMER_CIM_REFERENCE, "none" to switch off)
    player     ── loops the recording; "live" switches to the feed
    modules    ── FrameStatsModule
"""

from __future__ import annotations

import os
from collections.abc import Sequence

from pswamp_core.bus import InProcessBus
from pswamp_core.datagateway import CimReferenceEnricher, Enricher, Player, gateway_from_env
from pswamp_core.messages import PmuFrame
from pswamp_core.modules import Module
from pswamp_core.pipeline import Pipeline

from .stats_module import FrameStatsModule

__all__ = ["DEFAULT_DATA_CLIENTS", "build_pipeline", "cim_reference_enrichers"]

#: The providers when ``PSWAMP_DATA_CLIENTS`` is unset: the committed recording
#: (history) and a synthetic live feed of the same rows.
DEFAULT_DATA_CLIENTS = (
    "sample:pmu_test_streamer.sample_client:SampleRecordingClient,"
    "live:pmu_test_streamer.live_client:LiveSyntheticClient"
)

#: The placeholder ``cimReferenceId`` the stub enricher stamps on every frame.
DEFAULT_CIM_REFERENCE = "n44-stub"
CIM_REFERENCE_VARIABLE = "PMU_TEST_STREAMER_CIM_REFERENCE"


def cim_reference_enrichers() -> list[Enricher]:
    """The gateway's stub CIM reference enricher, unless switched off with ``none``."""
    reference = os.environ.get(CIM_REFERENCE_VARIABLE, "").strip() or DEFAULT_CIM_REFERENCE
    return [] if reference.lower() == "none" else [CimReferenceEnricher(reference)]


def build_pipeline(key: str, modules: Sequence[Module] | None = None) -> Pipeline:
    """One pipeline for ``key``: its own gateway, bus, player and modules."""
    gateway = gateway_from_env(DEFAULT_DATA_CLIENTS, enrichers=cim_reference_enrichers())
    bus = InProcessBus()
    player = Player(gateway, bus, model=PmuFrame, loop=True)
    return Pipeline(key, gateway, bus, player, [FrameStatsModule()] if modules is None else modules)
