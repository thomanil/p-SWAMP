# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""Environment-variable configuration, show_config, and gateway_from_env.

Lifted from the test_pswamp draft (``tests/test_client_config.py``), with the
CSV and Kafka clients (not lifted) replaced by ``support.EnvTestClient``, and
the ``gateway_from_env`` cases added.
"""

from __future__ import annotations

import pytest
from support import EnvTestClient, Measurement

from pswamp_core.datagateway import (
    Capability,
    MissingSettingError,
    env_key,
    gateway_from_env,
)
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.datagateway.config import parse_client_specs


def test_env_key_normalises_the_client_name():
    assert env_key("cold-archive", "DIRECTORY") == "COLD_ARCHIVE_DIRECTORY"


def test_client_from_env(monkeypatch):
    monkeypatch.setenv("ARCHIVE_LABEL", "cold store")
    monkeypatch.setenv("ARCHIVE_PRIORITY", "10")
    monkeypatch.setenv("ARCHIVE_CAPABILITIES", "history_consume,produce")
    monkeypatch.setenv("ARCHIVE_COUNT", "5")

    client = EnvTestClient.from_env("archive")

    assert client.label == "cold store"
    assert client.priority == 10
    assert client.capabilities == Capability.HISTORY_CONSUME | Capability.PRODUCE
    assert len(client.records) == 5


def test_overrides_win_over_the_environment(monkeypatch):
    monkeypatch.setenv("ARCHIVE_LABEL", "x")
    monkeypatch.setenv("ARCHIVE_PRIORITY", "10")

    client = EnvTestClient.from_env("archive", priority=99)

    assert client.priority == 99


def test_missing_required_setting_is_reported(monkeypatch):
    monkeypatch.delenv("ARCHIVE_LABEL", raising=False)

    with pytest.raises(MissingSettingError, match="ARCHIVE_LABEL"):
        EnvTestClient.from_env("archive")


def test_malformed_setting_is_reported(monkeypatch):
    monkeypatch.setenv("ARCHIVE_LABEL", "x")
    monkeypatch.setenv("ARCHIVE_PRIORITY", "high")

    with pytest.raises(MissingSettingError, match="ARCHIVE_PRIORITY"):
        EnvTestClient.from_env("archive")


def test_unknown_capability_is_reported(monkeypatch):
    monkeypatch.setenv("ARCHIVE_LABEL", "x")
    monkeypatch.setenv("ARCHIVE_CAPABILITIES", "TELEPORT")

    with pytest.raises(MissingSettingError, match="TELEPORT"):
        EnvTestClient.from_env("archive")


def test_show_config_lists_the_prefixed_variables(capsys):
    EnvTestClient.show_config("archive")
    printed = capsys.readouterr().out

    assert "EnvTestClient configuration for client 'archive'" in printed
    assert "ARCHIVE_LABEL" in printed
    assert "required" in printed
    assert "ARCHIVE_COUNT" in printed
    assert "default: 3" in printed


def test_show_config_covers_every_declared_setting(capsys):
    EnvTestClient.show_config("bus")
    printed = capsys.readouterr().out

    for setting in EnvTestClient.env_settings:
        assert env_key("bus", setting.setting) in printed


def test_show_config_on_a_client_without_settings(capsys):
    InMemoryClient.show_config()

    assert "takes no environment configuration" in capsys.readouterr().out


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
    monkeypatch.setenv("TWO_PRIORITY", "5")

    gateway = gateway_from_env()

    assert set(gateway.clients) == {"one", "two"}
    assert gateway.clients["two"].priority == 5
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
