# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

from datetime import datetime, timedelta, timezone

from pswamp_core.util.time import UTC, ensure_utc, utcnow


def test_naive_is_taken_as_utc():
    assert ensure_utc(datetime(2026, 1, 1)) == datetime(2026, 1, 1, tzinfo=UTC)


def test_aware_is_converted():
    oslo = timezone(timedelta(hours=1))
    assert ensure_utc(datetime(2026, 1, 1, 1, tzinfo=oslo)) == datetime(2026, 1, 1, tzinfo=UTC)


def test_utcnow_is_aware():
    assert utcnow().tzinfo is UTC
