# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The provider conformance suite: what any ``DataClient`` must do.

A provider author -- a TSO wiring up their own data store, or this repo's
own sample recording -- proves their client against the contract by inheriting
the suite and supplying three fixtures::

    # conftest.py or the test module
    import pytest
    from pswamp_core.datagateway.conformance import DataClientConformance

    class TestMyClient(DataClientConformance):
        @pytest.fixture
        def client_under_test(self):
            return MyClient("mine", ...)          # holds conformance_records

        @pytest.fixture
        def conformance_model(self):
            return PmuFrame                       # the class the client serves

        @pytest.fixture
        def conformance_records(self, client_under_test):
            return list(client_under_test.frames) # exactly what it holds, in order

The cases follow the client's **declared capabilities**: the history cases
(every record in order, half-open windows, seek, range query) run for a
``HISTORY_CONSUME`` client and skip otherwise; the live cases (now-relative
live coverage, a bounded tail that terminates) run for a ``LIVE_CONSUME``
client and skip otherwise. A client that can only tail has nothing to list, so
its ``conformance_records`` fixture returns ``[]``. What the suite cannot do is
make a live client *produce* anything -- a provider's own test asserts that its
tail carries data; the suite asserts only that a tail behaves.

The history cases are the test_pswamp draft's ``test_data_gateway.py``
generalised over any client and any model; cases that need *several* clients
(stitching, gaps, live hand-off) stay in ``core/tests/test_data_gateway.py``.
Every case goes through a ``DataGateway``, so what is checked is what the core
will ask of the client, not the client in isolation.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from ..messages.data_model import DataModel
from ..util.time import utcnow
from .data_client_model import Capability, DataClient
from .data_gateway import DataGateway


__all__ = ["DataClientConformance"]

#: How long a bounded live tail is opened for, and how long the suite waits
#: for it to close. A tail that has not returned by then is not bounded.
_LIVE_WINDOW = timedelta(seconds=0.3)
_LIVE_TIMEOUT = 3.0


def _identity(record: DataModel) -> tuple:
    return (record.timestamp, record.mRID)


def _needs(client: DataClient, capability: Capability) -> None:
    """Skip the case unless the client declared ``capability``."""
    if capability not in client.capabilities:
        pytest.skip(f"{client.name} does not declare {capability.name}")


def _assert_utc(record: DataModel) -> None:
    assert record.timestamp is not None
    assert record.timestamp.tzinfo is not None
    assert record.timestamp.utcoffset() == timedelta(0)


class DataClientConformance:
    """Inherit, supply the three fixtures, and pytest runs the cases."""

    # -- what the client declares ---------------------------------------------

    async def test_declares_a_consume_capability(self, client_under_test: DataClient):
        """A provider is a source: it must be readable one way or the other."""
        assert client_under_test.capabilities & (
            Capability.HISTORY_CONSUME | Capability.LIVE_CONSUME
        )

    async def test_supports_the_model(self, client_under_test, conformance_model):
        assert client_under_test.supports(conformance_model)
        for capability in (Capability.HISTORY_CONSUME, Capability.LIVE_CONSUME):
            if capability in client_under_test.capabilities:
                assert client_under_test.supports(conformance_model, capability)

    async def test_records_are_timestamped_utc_and_ordered(
        self, client_under_test, conformance_records
    ):
        _needs(client_under_test, Capability.HISTORY_CONSUME)
        assert conformance_records, "a history client's fixture must hold at least one record"
        previous = None
        for record in conformance_records:
            _assert_utc(record)
            if previous is not None:
                assert record.timestamp >= previous
            previous = record.timestamp

    # -- coverage --------------------------------------------------------------

    async def test_coverage_spans_the_records(
        self, client_under_test, conformance_model, conformance_records
    ):
        _needs(client_under_test, Capability.HISTORY_CONSUME)
        coverage = await client_under_test.coverage(conformance_model)
        assert coverage is not None
        assert coverage.range.contains(conformance_records[0].timestamp)
        assert coverage.range.contains(conformance_records[-1].timestamp)
        if Capability.LIVE_CONSUME not in client_under_test.capabilities:
            assert coverage.live is False

    async def test_coverage_is_recomputed_on_every_call(
        self, client_under_test, conformance_model
    ):
        first = await client_under_test.coverage(conformance_model)
        second = await client_under_test.coverage(conformance_model)
        assert first is not None and second is not None
        if Capability.HISTORY_CONSUME in client_under_test.capabilities:
            assert first == second
        else:
            # A tail-only client's window is now-relative: it may move, never back.
            assert first.live and second.live
            assert second.range.start is None or first.range.start is None or (
                second.range.start >= first.range.start
            )

    # -- live: tailing through the gateway ------------------------------------

    async def test_live_coverage_is_live_and_now_relative(
        self, client_under_test, conformance_model
    ):
        _needs(client_under_test, Capability.LIVE_CONSUME)
        coverage = await client_under_test.coverage(conformance_model)
        assert coverage is not None
        assert coverage.live is True
        now = utcnow()
        assert coverage.range.contains(now)
        assert coverage.range.end is None or coverage.range.end > now

    async def test_bounded_live_consume_terminates_inside_the_window(
        self, client_under_test, conformance_model
    ):
        """A tail with an end returns on its own once that end has passed, and
        everything it yielded lies inside the window, in order. It may be
        empty: the suite cannot make the source produce."""
        _needs(client_under_test, Capability.LIVE_CONSUME)
        gateway = DataGateway([client_under_test])
        start = utcnow()
        end = start + _LIVE_WINDOW

        async def drain() -> list[DataModel]:
            return [r async for r in gateway.consume(conformance_model, start, end)]

        got = await asyncio.wait_for(drain(), timeout=_LIVE_TIMEOUT)
        previous = None
        for record in got:
            _assert_utc(record)
            assert start <= record.timestamp < end
            if previous is not None:
                assert record.timestamp >= previous
            previous = record.timestamp

    async def test_live_stream_can_be_closed_early_and_reopened(
        self, client_under_test, conformance_model
    ):
        _needs(client_under_test, Capability.LIVE_CONSUME)
        gateway = DataGateway([client_under_test])
        stream = gateway.consume(conformance_model, utcnow(), None)
        try:
            await asyncio.wait_for(stream.__anext__(), timeout=0.1)
        except (TimeoutError, StopAsyncIteration):
            pass
        await stream.aclose()

        start = utcnow()

        async def drain() -> list[DataModel]:
            return [r async for r in gateway.consume(conformance_model, start, start + _LIVE_WINDOW)]

        await asyncio.wait_for(drain(), timeout=_LIVE_TIMEOUT)

    # -- history: consuming through the gateway --------------------------------

    async def test_consume_yields_every_record_in_order(
        self, client_under_test, conformance_model, conformance_records
    ):
        _needs(client_under_test, Capability.HISTORY_CONSUME)
        gateway = DataGateway([client_under_test])
        got = [record async for record in gateway.consume(conformance_model)]
        assert [_identity(r) for r in got] == [_identity(r) for r in conformance_records]

    async def test_window_is_half_open(
        self, client_under_test, conformance_model, conformance_records
    ):
        _needs(client_under_test, Capability.HISTORY_CONSUME)
        if len(conformance_records) < 4:
            pytest.skip("needs at least four records")
        gateway = DataGateway([client_under_test])
        start = conformance_records[1].timestamp
        end = conformance_records[3].timestamp
        got = [record async for record in gateway.consume(conformance_model, start, end)]
        expected = [r for r in conformance_records if start <= r.timestamp < end]
        assert [_identity(r) for r in got] == [_identity(r) for r in expected]

    async def test_seek_by_start_lands_on_that_record(
        self, client_under_test, conformance_model, conformance_records
    ):
        _needs(client_under_test, Capability.HISTORY_CONSUME)
        if len(conformance_records) < 3:
            pytest.skip("needs at least three records")
        gateway = DataGateway([client_under_test])
        target = conformance_records[2]
        stream = gateway.consume(conformance_model, target.timestamp, None)
        first = await stream.__anext__()
        await stream.aclose()
        assert _identity(first) == _identity(target)

    async def test_range_query_returns_only_the_chunk(
        self, client_under_test, conformance_model, conformance_records
    ):
        _needs(client_under_test, Capability.HISTORY_CONSUME)
        if len(conformance_records) < 3:
            pytest.skip("needs at least three records")
        gateway = DataGateway([client_under_test])
        start = conformance_records[1].timestamp
        end = conformance_records[2].timestamp
        got = [record async for record in gateway.consume(conformance_model, start, end)]
        assert got, "the chunk must not be empty"
        assert all(start <= r.timestamp < end for r in got)

    async def test_identifier_filter(
        self, client_under_test, conformance_model, conformance_records
    ):
        _needs(client_under_test, Capability.HISTORY_CONSUME)
        ids = {r.mRID for r in conformance_records if r.mRID is not None}
        if len(ids) < 2:
            pytest.skip("needs records with at least two distinct mRIDs")
        wanted = sorted(ids)[0]
        gateway = DataGateway([client_under_test])
        got = [r async for r in gateway.consume(conformance_model, mRID=wanted)]
        assert got
        assert all(r.mRID == wanted for r in got)

    async def test_stream_can_be_closed_early_and_reopened(
        self, client_under_test, conformance_model, conformance_records
    ):
        _needs(client_under_test, Capability.HISTORY_CONSUME)
        gateway = DataGateway([client_under_test])
        stream = gateway.consume(conformance_model)
        async for _ in stream:
            break
        await stream.aclose()
        again = [r async for r in gateway.consume(conformance_model)]
        assert len(again) == len(conformance_records)
