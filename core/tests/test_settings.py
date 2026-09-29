# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Configuration from the environment: names, kinds, required settings."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from pswamp_core.settings import (
    EnvSetting,
    MissingSettingError,
    env_key,
    format_settings,
    load_class,
    parse_spec,
    read_setting,
)


def test_env_key_normalises_the_name():
    assert env_key("cold-archive", "DIRECTORY") == "COLD_ARCHIVE_DIRECTORY"


@pytest.mark.parametrize(
    ("kind", "raw", "parsed"),
    [
        ("str", "x", "x"),
        ("int", "3", 3),
        ("float", "0.5", 0.5),
        ("seconds", "2", timedelta(seconds=2)),
        ("list", "a, b,,c", ["a", "b", "c"]),
        ("path", "/tmp/x", Path("/tmp/x")),
    ],
)
def test_read_setting_parses_per_kind(monkeypatch, kind, raw, parsed):
    monkeypatch.setenv("T_VALUE", raw)
    assert read_setting("t", EnvSetting("VALUE", "", kind=kind)) == parsed


def test_defaults_required_and_malformed(monkeypatch):
    monkeypatch.delenv("T_VALUE", raising=False)
    assert read_setting("t", EnvSetting("VALUE", "", default="7", kind="int")) == 7
    assert read_setting("t", EnvSetting("VALUE", "")) is None
    with pytest.raises(MissingSettingError):
        read_setting("t", EnvSetting("VALUE", "", required=True))
    monkeypatch.setenv("T_VALUE", "seven")
    with pytest.raises(MissingSettingError):
        read_setting("t", EnvSetting("VALUE", "", kind="int"))


def test_format_settings_lists_every_variable():
    text = format_settings("Thing", "t", [EnvSetting("A", "first", required=True), EnvSetting("B", "second", default="1")])
    assert "T_A" in text and "required" in text and "T_B" in text and "default: 1" in text


def test_spec_parsing_and_class_loading():
    assert parse_spec("V", "n:a.b:C") == ("n", "a.b", "C")
    with pytest.raises(MissingSettingError):
        parse_spec("V", "n:a.b")
    assert load_class("V", "pathlib", "Path", object) is Path
    with pytest.raises(MissingSettingError):
        load_class("V", "pathlib", "Path", int)
    with pytest.raises(MissingSettingError):
        load_class("V", "no.such.module", "X", object)
