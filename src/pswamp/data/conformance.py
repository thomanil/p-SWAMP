# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The provider conformance check a deployment runs against its own client.

STEP3 §5.6, first cut: one coroutine that drives a client through the rules the
gateway depends on and raises ``AssertionError`` with a plain message on the
first one broken. The repo's own tests call it on every shipped client; a TSO
writing a ``TimescaleClient`` calls it from their test suite with nothing but
``pswamp.data`` imported.

Rules checked:

1. ``coverage()`` reports a concrete window.
2. ``consume()`` over that window yields at least one payload, every payload is
   an instance of the model, carries a timestamp, lies inside the window, and
   timestamps never decrease.
3. A bounded sub-range stops at its ``end`` and returns exactly the payloads
   that fall inside it.
4. With ``PRODUCE`` and a ``sample`` to write, a produced payload can be read
   back at its timestamp.
"""

from __future__ import annotations

from collections.abc import Sequence

from .gateway.client import Capability, DataClient, MRIDFilter
from .gateway.time_range import TimeRange
from .models.base import DataModel

__all__ = ["check_client"]


async def check_client(
    client: DataClient,
    model: type[DataModel],
    *,
    mRID: MRIDFilter = None,
    sample: DataModel | None = None,
) -> Sequence[DataModel]:
    """Assert ``client`` honours the provider contract for ``model``; return what it yielded."""
    coverage = await client.coverage(model, mRID)
    assert coverage is not None, f"{client.name}: coverage() reported nothing for {model.__name__}"
    window = coverage.range
    assert window.start is not None and window.end is not None, (
        f"{client.name}: coverage must have concrete bounds, got {window}"
    )

    payloads = [payload async for payload in client.consume(model, window, mRID)]
    assert payloads, f"{client.name}: consume() over its own coverage yielded nothing"

    previous = None
    for payload in payloads:
        assert isinstance(payload, model), (
            f"{client.name}: yielded a {type(payload).__name__}, not a {model.__name__}"
        )
        assert payload.timestamp is not None, f"{client.name}: yielded a payload without timestamp"
        assert window.contains(payload.timestamp), (
            f"{client.name}: {payload.timestamp.isoformat()} is outside coverage {window}"
        )
        assert previous is None or payload.timestamp >= previous, (
            f"{client.name}: timestamps decreased ({previous} -> {payload.timestamp})"
        )
        previous = payload.timestamp

    if len(payloads) >= 2:
        cut = payloads[len(payloads) // 2].timestamp
        expected = [payload for payload in payloads if payload.timestamp < cut]
        sub = TimeRange(window.start, cut)
        got = [payload async for payload in client.consume(model, sub, mRID)]
        assert len(got) == len(expected), (
            f"{client.name}: sub-range {sub} returned {len(got)} payloads, expected {len(expected)}"
        )
        assert all(payload.timestamp < cut for payload in got), (
            f"{client.name}: a sub-range yielded a payload at or past its end"
        )

    if sample is not None and Capability.PRODUCE in client.capabilities:
        assert sample.timestamp is not None, "the sample to produce must be timestamped"
        await client.produce(sample)
        around = TimeRange(sample.timestamp, sample.timestamp + (window.end - window.start))
        read_back = [payload async for payload in client.consume(type(sample), around, sample.mRID)]
        assert any(payload.timestamp == sample.timestamp for payload in read_back), (
            f"{client.name}: a produced payload could not be read back"
        )

    return payloads
