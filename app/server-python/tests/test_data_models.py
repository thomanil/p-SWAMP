# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The message contract: derived topics, versions, the catalogue, NaN on the wire."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from typing import Literal

import pytest
from pydantic import ValidationError

from pswamp.data import (
    StreamChannel,
    DataModel,
    ModuleRef,
    Report,
    Sample,
    StreamHeader,
    catalogue,
)


class VoltageReport(Report):
    version: Literal["v1"] = "v1"
    mean_voltage: float


def test_topic_is_derived_from_the_class_name_and_namespace():
    assert VoltageReport.topic == "voltage.live.report"
    assert Sample.topic == "sample.live"
    assert StreamHeader.topic == "stream.live.header"

    report = VoltageReport(
        module=ModuleRef(name="x", uuid="1"), n_samples=0, mean_voltage=1.0, namespace="no"
    )
    assert report.topic == "voltage.no.report"


def test_version_is_pinned_per_subclass():
    with pytest.raises(ValidationError):
        VoltageReport.model_validate(
            {"version": "v2", "module": {"name": "x", "uuid": "1"}, "n_samples": 0, "mean_voltage": 1}
        )


def test_catalogue_lists_concrete_models_only():
    models = catalogue()
    assert VoltageReport in models
    assert Sample in models
    # Bases that pin no version are not publishable and stay out.
    assert Report not in models
    assert DataModel not in models
    assert [m.topic for m in models] == sorted(m.topic for m in models)


def test_timestamps_are_utc_aware_after_validation():
    naive = Sample(mRID="s", values=[], timestamp=datetime(2026, 1, 1, 12, 0, 0))
    assert naive.timestamp == datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def test_nan_and_infinity_become_null_on_the_wire():
    sample = Sample(mRID="s", values=[1.5, math.nan, math.inf, None], timestamp=datetime(2026, 1, 1))
    wire = json.loads(sample.model_dump_json())
    assert wire["values"] == [1.5, None, None, None]
    # In memory the NaN is untouched, so analysis can use it.
    assert math.isnan(sample.values[1])


def test_header_round_trips_through_json():
    header = StreamHeader(
        stream_id="n44",
        data_rate=20.0,
        channels=[StreamChannel(station="3000", channel="V_Magnitude", measurement="v_Magnitude", unit="V")],
        timestamp=datetime(2026, 1, 1),
    )
    again = StreamHeader.model_validate_json(header.model_dump_json())
    assert again == header
