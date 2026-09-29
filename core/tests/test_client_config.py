# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""A provider configured from the environment: from_env, show_config, gateway_from_env."""

from __future__ import annotations

import pytest
from support import EnvTestClient

from pswamp_core.datagateway import Capability, MissingSettingError
from pswamp_core.datagateway.clients import InMemoryClient
from pswamp_core.settings import env_key


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

    assert "EnvTestClient configuration for 'archive'" in printed
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
