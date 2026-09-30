# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""What every Islanding stream pipeline is made of: the ``islanding-stream.*``
topics, a gateway over the N44 recording with the stub CIM reference enricher,
and the islanding module. A worker hosts it with
``PSWAMP_WORKER_FAMILIES=islanding_stream.family:FAMILY``; the reference rides
in the frame, so that worker needs no enrichment configuration of its own."""

from __future__ import annotations

import os

from pswamp_core.datagateway import CimReferenceEnricher, DataGateway, gateway_from_env
from pswamp_core.pipeline import PipelineFamily

from .islanding_module import IslandingModule

APP = "islanding-stream"

#: The providers a deployment gets unless ISLANDING_STREAM_DATA_CLIENTS names others.
DEFAULT_DATA_CLIENTS = "n44:islanding_stream.n44_client:N44RecordingClient"
DATA_CLIENTS_VARIABLE = "ISLANDING_STREAM_DATA_CLIENTS"

#: The CIM reference the gateway's stub enricher stamps on every frame's header
#: (``PmuHeader.cimReferenceId``) -- a placeholder until a real one exists. Set
#: ``ISLANDING_STREAM_CIM_REFERENCE`` to change it, or to ``none`` for no
#: enrichment.
DEFAULT_CIM_REFERENCE = "n44-stub"
CIM_REFERENCE_VARIABLE = "ISLANDING_STREAM_CIM_REFERENCE"


def cim_reference_enrichers() -> list[CimReferenceEnricher]:
    """The gateway's stub CIM reference enricher, unless switched off."""
    reference = os.environ.get(CIM_REFERENCE_VARIABLE, "").strip() or DEFAULT_CIM_REFERENCE
    return [] if reference.lower() == "none" else [CimReferenceEnricher(reference)]


def gateway() -> DataGateway:
    return gateway_from_env(
        DEFAULT_DATA_CLIENTS, variable=DATA_CLIENTS_VARIABLE, enrichers=cim_reference_enrichers()
    )


FAMILY = PipelineFamily(APP, gateway, (IslandingModule,))
