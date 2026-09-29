# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""A gateway composed from the environment.

A deployment names *which* providers make up a gateway with one variable, and
configures each with its own ``{NAME}_{SETTING}`` block::

    PSWAMP_DATA_CLIENTS="live:acme_tso.pmu:KafkaFeed,history:acme_tso.pmu:TimescaleClient"
    HISTORY_DSN=postgres://...

That is the whole mechanism by which a deployment plugs in its own provider:
an image with one extra package, and one variable naming it.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from ..settings import MissingSettingError, load_class, parse_spec

if TYPE_CHECKING:
    from .data_gateway import DataGateway

__all__ = ["DATA_CLIENTS_VARIABLE", "gateway_from_env", "parse_client_specs"]

#: The default variable naming the clients a gateway is built from.
DATA_CLIENTS_VARIABLE = "PSWAMP_DATA_CLIENTS"


def parse_client_specs(spec: str, variable: str = DATA_CLIENTS_VARIABLE) -> list[tuple[str, str, str]]:
    """Split a comma-separated ``name:module.path:ClassName`` list into triples."""
    specs = [parse_spec(variable, entry) for entry in spec.split(",") if entry.strip()]
    if not specs:
        raise MissingSettingError(f"{variable} names no clients")
    return specs


def gateway_from_env(
    default: str | None = None,
    *,
    variable: str = DATA_CLIENTS_VARIABLE,
    **gateway_options: Any,
) -> DataGateway:
    """Build a ``DataGateway`` from the clients the environment names.

    Reads ``variable``, falling back to ``default`` when it is unset, and builds
    each named client with its own ``from_env(name)``. ``gateway_options`` go to
    ``DataGateway``.

    Raises:
        MissingSettingError: When neither names a client, or a client cannot be
            imported or configured.
    """
    from .data_client_model import DataClient
    from .data_gateway import DataGateway

    spec = os.environ.get(variable, "").strip() or default
    if not spec:
        raise MissingSettingError(f"{variable} is unset and no default was given")
    clients = [
        load_class(variable, module_path, class_name, DataClient).from_env(name)
        for name, module_path, class_name in parse_client_specs(spec, variable)
    ]
    return DataGateway(clients, **gateway_options)
