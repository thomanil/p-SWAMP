# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The wire shapes a remote time-series store speaks: the query going up and
the result envelope coming down, and the error event any pipeline may raise."""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from support import Measurement, at, measurement

from pswamp_core.messages import ErrorEvent, TimeSeriesQuery, TimeSeriesResult


def test_query_round_trips_as_json_with_utc_bounds():
    query = TimeSeriesQuery(query_id="q1", model="pmu.frame", start=at(1), end=at(2), mrid=["a"])
    again = TimeSeriesQuery.model_validate_json(query.model_dump_json())
    assert again == query
    assert again.start.tzinfo is not None and again.start.utcoffset().total_seconds() == 0
    assert TimeSeriesQuery(query_id="q", model="pmu.frame").start is None


def test_result_envelope_carries_a_record_as_json_and_round_trips():
    record = measurement(3, at(3))
    result = TimeSeriesResult.for_record("q1", 0, record, timestamp=at(10))
    again = TimeSeriesResult.model_validate_json(result.model_dump_json())
    assert again.kind == "record" and again.model == "measurement" and again.query_id == "q1"
    assert Measurement.model_validate(again.record) == record
    assert TimeSeriesResult.topic == "time.series.result"


def test_end_and_error_envelopes():
    end = TimeSeriesResult.ended("q1", 7, 7, timestamp=at(10))
    assert end.kind == "end" and end.count == 7 and end.record is None
    failed = TimeSeriesResult.failed("q1", 0, "no such model", timestamp=at(10))
    assert failed.kind == "error" and failed.error == "no such model"


@pytest.mark.parametrize(
    "fields",
    [
        {"kind": "record", "model": "measurement"},  # no record
        {"kind": "record", "record": {}},  # no model
        {"kind": "end"},  # no count
        {"kind": "error"},  # no error text
        {"kind": "error", "error": ""},
    ],
)
def test_an_envelope_must_match_its_kind(fields):
    with pytest.raises(ValidationError):
        TimeSeriesResult(timestamp=at(0), query_id="q", seq=0, **fields)


def test_error_event_topic_and_shape():
    event = ErrorEvent(timestamp=at(0), source="player", message="stopped", detail="TimeoutError: x")
    assert ErrorEvent.topic == "error.event"
    assert ErrorEvent.model_validate_json(event.model_dump_json()) == event
    assert event.request_id is None
