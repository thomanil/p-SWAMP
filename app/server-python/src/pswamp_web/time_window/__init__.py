# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

from ..wire import TimeWindowSlice
from .api import router

# The model this app pushes down its socket, collected into the published api
# contract by api_contract.py -- see timeline/__init__.py for the mechanism.
WS_MESSAGE = TimeWindowSlice

__all__ = ["WS_MESSAGE", "router"]
