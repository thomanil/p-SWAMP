# SPDX-FileCopyrightText: 2026 Louis Pauchet <louis.pauchet@sintef.no>
# SPDX-License-Identifier: Apache-2.0

"""A provider configured from the environment: from_env, gateway_from_env."""

from __future__ import annotations

import pytest
from support import EnvTestClient

from pswamp_core.datagateway import Capability, MissingSettingError


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
