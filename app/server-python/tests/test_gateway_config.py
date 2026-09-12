# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Naming a provider by configuration: env keys, type resolution, the client list."""

from __future__ import annotations

import pytest
from conftest import Measurement

from pswamp.data import (
    BUILTIN_CLIENTS,
    CLIENTS_SETTING,
    Capability,
    InMemoryClient,
    MissingSettingError,
    build_gateway_from_env,
    env_key,
    resolve_client_type,
)


def test_env_key_namespaces_by_client_name():
    assert env_key("archive", "DIRECTORY") == "ARCHIVE_DIRECTORY"
    assert env_key("kafka-no", "bootstrap_servers") == "KAFKA_NO_BOOTSTRAP_SERVERS"


def test_resolve_builtin_name_and_module_path():
    assert resolve_client_type("in_memory", BUILTIN_CLIENTS) is InMemoryClient
    assert (
        resolve_client_type("pswamp.data.clients.in_memory:InMemoryClient", {})
        is InMemoryClient
    )
    with pytest.raises(MissingSettingError):
        resolve_client_type("no_such_client", BUILTIN_CLIENTS)
    with pytest.raises(MissingSettingError):
        resolve_client_type("pswamp.data:Nope", {})


def test_defaults_apply_when_nothing_is_configured(monkeypatch):
    monkeypatch.delenv(CLIENTS_SETTING, raising=False)
    built = []

    def defaults():
        client = InMemoryClient("default", [Measurement])
        built.append(client)
        return [client]

    gateway = build_gateway_from_env([Measurement], defaults=defaults, registry=BUILTIN_CLIENTS)

    assert list(gateway.clients) == ["default"]
    assert built


def test_named_clients_are_built_from_their_own_settings(monkeypatch):
    monkeypatch.setenv(CLIENTS_SETTING, "bus, archive")
    monkeypatch.setenv("BUS_TYPE", "in_memory")
    monkeypatch.setenv("BUS_PRIORITY", "5")
    monkeypatch.setenv("ARCHIVE_TYPE", "pswamp.data.clients.in_memory:InMemoryClient")
    monkeypatch.setenv("ARCHIVE_CAPABILITIES", "history_consume")

    gateway = build_gateway_from_env(
        [Measurement], defaults=lambda: [], registry=BUILTIN_CLIENTS
    )

    assert list(gateway.clients) == ["bus", "archive"]
    assert gateway.clients["bus"].priority == 5
    assert gateway.clients["archive"].capabilities == Capability.HISTORY_CONSUME


def test_a_named_client_without_a_type_fails_loudly(monkeypatch):
    monkeypatch.setenv(CLIENTS_SETTING, "mystery")
    monkeypatch.delenv("MYSTERY_TYPE", raising=False)

    with pytest.raises(MissingSettingError, match="MYSTERY_TYPE"):
        build_gateway_from_env([Measurement], defaults=lambda: [], registry=BUILTIN_CLIENTS)
