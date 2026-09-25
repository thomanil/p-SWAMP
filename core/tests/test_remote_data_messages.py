# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The wire shapes a remote data service speaks: the query going up and
the line envelope coming down, and the error event any pipeline may raise."""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from support import Measurement, at, measurement

from pswamp_core.messages import ErrorEvent, RemoteDataQuery, RemoteDataResult


def test_query_round_trips_as_json_with_utc_bounds():
    query = RemoteDataQuery(query_id="q1", model="pmu.frame", start=at(1), end=at(2), mrid=["a"])
    again = RemoteDataQuery.model_validate_json(query.model_dump_json())
    assert again == query
    assert again.start.tzinfo is not None and again.start.utcoffset().total_seconds() == 0
    assert RemoteDataQuery(query_id="q", model="pmu.frame").start is None


def test_result_line_carries_a_record_as_json_and_round_trips():
    record = measurement(3, at(3))
    line = RemoteDataResult.for_record(record).to_line()
    assert line.endswith(b"\n") and line.count(b"\n") == 1
    again = RemoteDataResult.model_validate_json(line)
    assert again.kind == "record" and again.model == "measurement"
    assert Measurement.model_validate(again.record) == record


def test_end_and_error_lines_carry_only_their_own_fields():
    end = RemoteDataResult.ended(7)
    assert end.kind == "end" and end.count == 7 and end.record is None
    assert end.to_line() == b'{"kind":"end","count":7}\n'
    failed = RemoteDataResult.failed("no such model")
    assert failed.kind == "error" and failed.error == "no such model"
    assert failed.to_line() == b'{"kind":"error","error":"no such model"}\n'


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
        RemoteDataResult(**fields)


def test_error_event_topic_and_shape():
    event = ErrorEvent(timestamp=at(0), source="player", message="stopped", detail="TimeoutError: x")
    assert ErrorEvent.topic == "error.event"
    assert ErrorEvent.model_validate_json(event.model_dump_json()) == event
    assert event.request_id is None
