# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The committed sample file as a provider: parsing, units, and the contract."""

from __future__ import annotations

from datetime import timedelta
from math import isclose, radians

from pmu_data.sample_file import DEFAULT_FILE, RECORDING_EPOCH, STREAM_ID, SampleFileClient, parse_records
from pswamp.data import Capability, DataGateway, Sample, StreamHeader, check_client

RAW = """\
t= 0.050s  PMU=3000  V= 419.95kV  ang=   -0.00deg  f= 49.9999Hz
t= 0.050s  PMU=3245  V= 419.97kV  ang=    7.11deg  f= 50.0000Hz
t= 0.100s  PMU=3000  V= 419.96kV  ang=   -0.10deg  f= 49.9998Hz
t= 0.100s  PMU=3245  V= 419.98kV  ang=    7.10deg  f= 50.0001Hz
"""


def test_parse_builds_a_header_and_one_sample_per_instant():
    header, samples = parse_records(RAW, source="raw")

    assert [c.station for c in header.channels] == ["3000", "3000", "3000", "3245", "3245", "3245"]
    assert [c.measurement for c in header.channels[:3]] == ["v_Magnitude", "v_Angle", "f"]
    assert header.data_rate == 20.0
    assert header.stream_id == STREAM_ID
    assert len(samples) == 2
    assert samples[0].timestamp == RECORDING_EPOCH + timedelta(seconds=0.05)
    # kV -> V, degrees -> radians, Hz as is: the provider converts, the modules never do.
    assert samples[0].values[0] == 419950.0
    assert isclose(samples[0].values[4], radians(7.11))
    assert samples[1].values[5] == 50.0001


def test_the_committed_file_parses_to_five_stations_at_20hz():
    client = SampleFileClient("source")

    assert len(client.header.channels) == 15
    assert client.header.data_rate == 20.0
    assert len(client.samples) == 60
    assert client.header.source == DEFAULT_FILE.name


async def test_it_honours_the_provider_contract():
    client = SampleFileClient("source")

    samples = await check_client(client, Sample, mRID=STREAM_ID)
    assert len(samples) == 60
    assert Capability.PRODUCE not in client.capabilities

    headers = [h async for h in client.consume(StreamHeader, (await client.coverage(StreamHeader)).range)]
    assert len(headers) == 1 and headers[0] is client.header


async def test_through_the_gateway_a_consumer_names_only_model_and_range():
    gateway = DataGateway([SampleFileClient("source")])
    origin = RECORDING_EPOCH

    window = [s async for s in gateway.consume(Sample, origin + timedelta(seconds=1), origin + timedelta(seconds=1.2))]

    assert [round((s.timestamp - origin).total_seconds(), 3) for s in window] == [1.0, 1.05, 1.1, 1.15]
    # A filter for a stream this provider does not serve routes nowhere.
    assert [s async for s in gateway.consume(Sample, mRID="some-other-stream")] == []


def test_from_env_reads_its_own_settings(monkeypatch, tmp_path):
    other = tmp_path / "other.txt"
    other.write_text(RAW)
    monkeypatch.setenv("SOURCE_PATH", str(other))
    monkeypatch.setenv("SOURCE_PRIORITY", "42")

    client = SampleFileClient.from_env("source", [Sample])

    assert client.priority == 42
    assert len(client.samples) == 2
