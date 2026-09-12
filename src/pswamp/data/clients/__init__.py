# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Contract 3: clients shipped with the repo. The bus is one of them."""

from .in_memory import BUS_CAPABILITIES, InMemoryClient

#: Built-in short names a ``<NAME>_TYPE`` setting may use.
BUILTIN_CLIENTS = {"in_memory": InMemoryClient}

__all__ = ["BUILTIN_CLIENTS", "BUS_CAPABILITIES", "InMemoryClient"]
