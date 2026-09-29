# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The reference client passes the provider conformance suite.

Also the worked example of how a provider author runs it: inherit, supply the
three fixtures, done. Twice: once as a history client, once as a client that
can only tail, which is what proves the suite's capability guards -- the
history cases skip, the live cases run, and nothing tails forever.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from support import HISTORY, Measurement, measurements

from pswamp_core.datagateway import Capability, Coverage, TimeRange
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.datagateway.conformance import DataClientConformance
from pswamp_core.util.time import utcnow


class TestInMemoryClientConformance(DataClientConformance):
    @pytest.fixture
    def client_under_test(self):
        return InMemoryClient("memory", Measurement, measurements(6), capabilities=HISTORY)

    @pytest.fixture
    def conformance_model(self):
        return Measurement

    @pytest.fixture
    def conformance_records(self, client_under_test):
        return list(client_under_test.records)


class TestInMemoryLiveOnlyConformance(DataClientConformance):
    """A tail-only client: no records to list, a now-relative live window."""

    @pytest.fixture
    def client_under_test(self):
        return InMemoryClient(
            "tail",
            Measurement,
            capabilities=Capability.LIVE_CONSUME,
            coverage_fn=lambda: Coverage(
                TimeRange(utcnow() - timedelta(seconds=1), None), live=True
            ),
        )

    @pytest.fixture
    def conformance_model(self):
        return Measurement

    @pytest.fixture
    def conformance_records(self):
        return []
