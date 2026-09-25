# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Fixtures for the core tests. Helpers live in support.py."""

from __future__ import annotations

import pytest

from support import HISTORY, Measurement, measurements

from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.messages import DataModel


@pytest.fixture
def model() -> type[DataModel]:
    return Measurement


@pytest.fixture
def history_client() -> InMemoryClient:
    """Ten measurements one second apart, history only."""
    return InMemoryClient("history", Measurement, measurements(10), capabilities=HISTORY)
