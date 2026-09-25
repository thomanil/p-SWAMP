# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The message base: topics, versions, UTC timestamps, the JSON codec."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import ClassVar, Literal

import pytest
from pydantic import ValidationError
from support import Measurement, NumberResult, at

from pswamp_core.messages import (
    DataModel,
    GoLiveCommand,
    PlayCommand,
    PlayerStatus,
    PmuFrame,
    PmuHeader,
    ReplayCommand,
    SeekCommand,
    SpeedCommand,
)
from pswamp_core.messages.data_model import topic_from_name
from pswamp_core.messages.pmu import header_id_of


@pytest.mark.parametrize(
    ("name", "topic"),
    [
        ("PmuFrame", "pmu.frame"),
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
    header = PmuHeader(
        station=["a", "a", "b"],
        channel=["V", "V", "f"],
        measurement=["V_Magnitude", "V_Angle", "f"],
        units=["kV", "deg", "Hz"],
        data_rate=20.0,
    )
    frame = PmuFrame(
        timestamp=at(0.05),
        mRID="stream",
        header=header,
        values=[1.0, None, float("nan")],
    )
    text = frame.model_dump_json()
    assert "NaN" not in text and "null" in text
    back = PmuFrame.model_validate_json(text)
    assert back.values == [1.0, None, None]
    assert back.timestamp == frame.timestamp
    assert back.header == header and back.header.header_id == header.header_id


def test_a_frame_carries_its_layout_and_its_width_is_checked():
    header = PmuHeader(station=["a", "b"], channel=["f", "f"], measurement=["f", "f"], units=["Hz", "Hz"], data_rate=1.0)
    frame = PmuFrame(timestamp=at(0), mRID="s", header=header, values=[50.0, 50.1])
    assert frame.header is header and frame.header.columns(measurement="f") == [0, 1]
    with pytest.raises(ValidationError):
        PmuFrame(timestamp=at(0), mRID="s", header=header, values=[50.0])
    with pytest.raises(ValidationError):
        PmuFrame(timestamp=at(0), mRID="s", header=header, values=[50.0, 50.1], quality=[0])
    # The id is a serialised, cached property: in the JSON, and one object per instance.
    assert '"header_id":"' in frame.model_dump_json()
    assert header.header_id is header.header_id


def test_header_id_is_a_content_hash():
    args = (["a"], ["V"], ["V_Magnitude"], ["kV"], 20.0)
    assert header_id_of(*args) == header_id_of(*args)
    assert header_id_of(*args) != header_id_of(["b"], ["V"], ["V_Magnitude"], ["kV"], 20.0)
    header = PmuHeader(station=["a"], channel=["V"], measurement=["V_Magnitude"], units=["kV"], data_rate=20.0)
    assert header.header_id == header_id_of(*args)


def test_header_rows_must_align():
    with pytest.raises(ValidationError):
        PmuHeader(
            station=["a", "a"], channel=["V"], measurement=["V"], units=["kV"],
            data_rate=20.0,
        )


def test_header_column_lookup():
    header = PmuHeader(
        station=["a", "a", "b", "b"], channel=["V", "V", "V", "V"],
        measurement=["V_Magnitude", "f", "V_Magnitude", "f"], units=["kV", "Hz"] * 2,
        data_rate=20.0,
    )
    assert header.columns(measurement="f") == [1, 3]
    assert header.columns(station="b", measurement="V_Magnitude") == [2]
    assert header.stations == ["a", "b"]


def test_command_gets_a_request_id_and_a_name_from_its_class():
    a, b = PlayCommand(), PlayCommand()
    assert a.request_id and a.request_id != b.request_id
    assert a.target is None
    assert PlayCommand.name == a.name == "play"
    assert GoLiveCommand.name == "go.live" and GoLiveCommand.topic == "go.live.command"


def test_a_command_s_arguments_are_its_validated_fields():
    assert SeekCommand(offset_s=3).offset_s == 3
    for bad in ({}, {"offset_s": 1, "to": "2020-01-01T00:00:00Z"}, {"offset_s": -1}):
        with pytest.raises(ValidationError):
            SeekCommand(**bad)
    with pytest.raises(ValidationError):
        SpeedCommand(speed=0)
    with pytest.raises(ValidationError):
        ReplayCommand(start="2020-01-01T00:00:05Z", end="2020-01-01T00:00:05Z")
    naive = ReplayCommand(start="2020-01-01T00:00:00", end="2020-01-01T00:00:01")
    assert naive.start.tzinfo is not None and naive.end.tzinfo is not None


def test_player_status_serialises():
    status = PlayerStatus(
        timestamp=at(0), mode="replay", cursor=None, speed=1.0, paused=True, loop=False,
        ended=False, can_seek=True, coverage_start=at(0), coverage_end=at(3),
        frame_interval_s=None,
    )
    assert PlayerStatus.model_validate_json(status.model_dump_json()) == status
