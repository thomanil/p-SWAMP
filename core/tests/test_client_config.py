# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""A provider configured from the environment: from_env, gateway_from_env."""

from __future__ import annotations

import pytest
from support import EnvTestClient, Measurement

from pswamp_core.datagateway import Capability, MissingSettingError, gateway_from_env
from pswamp_core.datagateway.config import parse_client_specs


def test_client_from_env(monkeypatch):
    monkeypatch.setenv("ARCHIVE_LABEL", "cold store")
    monkeypatch.setenv("ARCHIVE_CAPABILITIES", "history_consume,live_consume")
    monkeypatch.setenv("ARCHIVE_COUNT", "5")

    client = EnvTestClient.from_env("archive")

    assert client.label == "cold store"
    assert client.capabilities == Capability.HISTORY_CONSUME | Capability.LIVE_CONSUME
    assert len(client.records) == 5


def test_overrides_win_over_the_environment(monkeypatch):
    monkeypatch.setenv("ARCHIVE_LABEL", "x")
    monkeypatch.setenv("ARCHIVE_COUNT", "10")

    client = EnvTestClient.from_env("archive", count=2)

    assert len(client.records) == 2


def test_missing_required_setting_is_reported(monkeypatch):
    monkeypatch.delenv("ARCHIVE_LABEL", raising=False)

    with pytest.raises(MissingSettingError, match="ARCHIVE_LABEL"):
        EnvTestClient.from_env("archive")


def test_malformed_setting_is_reported(monkeypatch):
    monkeypatch.setenv("ARCHIVE_LABEL", "x")
    monkeypatch.setenv("ARCHIVE_COUNT", "high")

    with pytest.raises(MissingSettingError, match="ARCHIVE_COUNT"):
        EnvTestClient.from_env("archive")


def test_unknown_capability_is_reported(monkeypatch):
    monkeypatch.setenv("ARCHIVE_LABEL", "x")
    monkeypatch.setenv("ARCHIVE_CAPABILITIES", "TELEPORT")

    with pytest.raises(MissingSettingError, match="TELEPORT"):
        EnvTestClient.from_env("archive")


# --- composing a gateway from the environment ---------------------------------


def test_parse_client_specs():
    assert parse_client_specs("a:mod.path:Cls, b:other:Other") == [
        ("a", "mod.path", "Cls"),
        ("b", "other", "Other"),
    ]
    with pytest.raises(MissingSettingError):
        parse_client_specs("a:mod.path")
    with pytest.raises(MissingSettingError):
        parse_client_specs("")


async def test_gateway_from_env_builds_the_named_clients(monkeypatch):
    monkeypatch.setenv("PSWAMP_DATA_CLIENTS", "one:support:EnvTestClient,two:support:EnvTestClient")
    monkeypatch.setenv("ONE_LABEL", "first")
    monkeypatch.setenv("TWO_LABEL", "second")
    monkeypatch.setenv("TWO_CAPABILITIES", "live_consume")  # one per role

    gateway = gateway_from_env()

    assert set(gateway.clients) == {"one", "two"}
    assert gateway.clients["two"].capabilities == Capability.LIVE_CONSUME
    assert [m.mRID async for m in gateway.consume(Measurement)] == ["m0", "m1", "m2"]


def test_gateway_from_env_falls_back_to_the_default(monkeypatch):
    monkeypatch.delenv("PSWAMP_DATA_CLIENTS", raising=False)
    monkeypatch.setenv("SAMPLE_LABEL", "default")

    gateway = gateway_from_env("sample:support:EnvTestClient")

    assert list(gateway.clients) == ["sample"]


def test_gateway_from_env_reports_a_bad_class(monkeypatch):
    monkeypatch.setenv("PSWAMP_DATA_CLIENTS", "x:support:Measurement")
    with pytest.raises(MissingSettingError, match="not a DataClient"):
        gateway_from_env()

    monkeypatch.setenv("PSWAMP_DATA_CLIENTS", "x:no.such.module:Thing")
    with pytest.raises(MissingSettingError, match="cannot import"):
        gateway_from_env()

    monkeypatch.delenv("PSWAMP_DATA_CLIENTS", raising=False)
    with pytest.raises(MissingSettingError, match="unset"):
        gateway_from_env()
