# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The message base: topics, versions, UTC timestamps, the JSON codec."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import ClassVar, Literal

import pytest
from pydantic import ValidationError
from support import Measurement, NumberResult, at

from pswamp_core.messages import Command, DataModel, PmuFrame, PmuHeader, PlayerStatus
from pswamp_core.messages.data_model import topic_from_name
from pswamp_core.messages.pmu import header_id_of


@pytest.mark.parametrize(
    ("name", "topic"),
    [
        ("PmuFrame", "pmu.frame"),
        ("PmuHeader", "pmu.header"),
        ("FrameStatsResult", "frame.stats.result"),
        ("MeasurementPMUVoltage", "measurement.pmu.voltage"),
        ("N44Frame", "n.44.frame"),
        ("Command", "command"),
    ],
)
def test_topic_is_derived_from_the_class_name(name, topic):
    assert topic_from_name(name) == topic


def test_topic_on_class_and_instance():
    assert PmuFrame.topic == "pmu.frame"
    assert Measurement.topic == "measurement"
    assert Measurement(timestamp=at(0)).topic == "measurement"
    assert NumberResult.topic == "number.result"


def test_topic_can_be_overridden():
    class Odd(DataModel):
        version: Literal["v1"] = "v1"
        topic: ClassVar[str] = "custom.topic"

    class Odder(Odd):
        pass

    assert Odd.topic == "custom.topic"
    assert Odder.topic == "custom.topic"  # inherited override


def test_version_literal_rejects_other_versions():
    with pytest.raises(ValidationError):
        Measurement.model_validate({"version": "v2", "value": 1.0})


def test_naive_timestamps_are_taken_as_utc():
    m = Measurement(timestamp=datetime(2026, 1, 1, 12, 0, 0))
    assert m.timestamp.tzinfo is not None
    assert m.timestamp.utcoffset().total_seconds() == 0


def test_aware_timestamps_are_converted_to_utc():
    plus_two = timezone.utc.__class__(__import__("datetime").timedelta(hours=2))
    m = Measurement(timestamp=datetime(2026, 1, 1, 12, 0, 0, tzinfo=plus_two))
    assert m.timestamp.hour == 10
    assert m.timestamp.utcoffset().total_seconds() == 0


def test_json_round_trip_with_null_values():
    header = PmuHeader.build(
        timestamp=at(0),
        mRID="stream",
        station=["a", "a", "b"],
        channel=["V", "V", "f"],
        measurement=["V_Magnitude", "V_Angle", "f"],
        units=["kV", "deg", "Hz"],
        data_rate=20.0,
    )
    frame = PmuFrame(
        timestamp=at(0.05),
        mRID="stream",
        header_id=header.header_id,
        values=[1.0, None, float("nan")],
    )
    text = frame.model_dump_json()
    assert "NaN" not in text and "null" in text
    back = PmuFrame.model_validate_json(text)
    assert back.values == [1.0, None, None]
    assert back.timestamp == frame.timestamp


def test_header_id_is_a_content_hash():
    args = (["a"], ["V"], ["V_Magnitude"], ["kV"], 20.0)
    assert header_id_of(*args) == header_id_of(*args)
    assert header_id_of(*args) != header_id_of(["b"], ["V"], ["V_Magnitude"], ["kV"], 20.0)
    header = PmuHeader.build(
        timestamp=at(0), mRID="s", station=["a"], channel=["V"],
        measurement=["V_Magnitude"], units=["kV"], data_rate=20.0,
    )
    assert header.header_id == header_id_of(*args)


def test_header_rows_must_align():
    with pytest.raises(ValidationError):
        PmuHeader(
            timestamp=at(0), mRID="s", header_id="x",
            station=["a", "a"], channel=["V"], measurement=["V"], units=["kV"],
            data_rate=20.0,
        )


def test_header_column_lookup():
    header = PmuHeader.build(
        timestamp=at(0), mRID="s",
        station=["a", "a", "b", "b"], channel=["V", "V", "V", "V"],
        measurement=["V_Magnitude", "f", "V_Magnitude", "f"], units=["kV", "Hz"] * 2,
        data_rate=20.0,
    )
    assert header.columns(measurement="f") == [1, 3]
    assert header.columns(station="b", measurement="V_Magnitude") == [2]
    assert header.stations == ["a", "b"]


def test_command_gets_a_request_id():
    a, b = Command(verb="play"), Command(verb="play")
    assert a.request_id and a.request_id != b.request_id
    assert a.target is None


def test_player_status_serialises():
    status = PlayerStatus(
        timestamp=at(0), mode="replay", cursor=None, speed=1.0, paused=True, loop=False,
        ended=False, can_seek=True, coverage_start=at(0), coverage_end=at(3),
        frame_interval_s=None,
    )
    assert PlayerStatus.model_validate_json(status.model_dump_json()) == status
