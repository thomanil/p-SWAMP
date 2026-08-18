# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Turning a time window's header into something a client can choose from.

p-SWAMP labels a window's columns with a three-row table -- station, channel,
measurement -- and selects columns by querying it
(``get_col_idx(measurement='f')``). That query language is the right thing on the
server, but a browser needs a flat, addressable list, so this module flattens the
header once and lets the client select by column index.
"""

import numpy as np

from .wire import Channel

# What a page shows if the client expresses no preference. Frequency is the
# measurement the interesting disturbance in the bundled recording appears in.
DEFAULT_MEASUREMENT = "f"

# How many channels to preselect. Enough to see a system-wide split, few enough
# to read as individual traces.
DEFAULT_CHANNEL_COUNT = 8

# Stations that separate onto their own frequency island when the recorded line
# trip happens. Preselected together with a few from the main system, so the page
# shows the disturbance rather than 8 traces that stay on top of each other.
ISLANDED_STATIONS = ("6500", "6700", "6701")

# Human-readable measurement names, since the raw codes are terse and repeated
# across every station.
MEASUREMENT_LABELS = {
    "f": "Frequency",
    "Df": "ROCOF",
    "v_Magnitude": "V mag",
    "v_Angle": "V angle",
    "i_Magnitude": "I mag",
    "i_Angle": "I angle",
}


def describe(header) -> list[Channel]:
    """Flatten a TimeWindowLabeled header into an addressable channel list."""
    stations = np.asarray(header["station"])
    channels = np.asarray(header["channel"])
    measurements = np.asarray(header["measurement"])

    return [
        Channel(
            idx=idx,
            station=str(station),
            channel=str(channel),
            measurement=str(measurement),
            label=f"{station} {MEASUREMENT_LABELS.get(str(measurement), measurement)}",
        )
        for idx, (station, channel, measurement) in enumerate(
            zip(stations, channels, measurements)
        )
    ]


def default_selection(all_channels: list[Channel]) -> list[int]:
    """Pick an opening selection that shows something worth looking at."""
    frequency = [c for c in all_channels if c.measurement == DEFAULT_MEASUREMENT]
    if not frequency:
        return [c.idx for c in all_channels[:DEFAULT_CHANNEL_COUNT]]

    islanded = [c for c in frequency if c.station in ISLANDED_STATIONS]
    others = [c for c in frequency if c.station not in ISLANDED_STATIONS]
    chosen = islanded + others[: max(0, DEFAULT_CHANNEL_COUNT - len(islanded))]
    return [c.idx for c in chosen]


def sanitise(requested, n_columns: int) -> list[int]:
    """Clamp a client's requested selection to real columns, in order, no dupes.

    The selection arrives from the browser, so it is untrusted: an out-of-range
    index would be an IndexError deep inside a numpy take, on the server, for
    every subsequent tick.
    """
    seen = set()
    out = []
    for value in requested or ():
        try:
            idx = int(value)
        except (TypeError, ValueError):
            continue
        if 0 <= idx < n_columns and idx not in seen:
            seen.add(idx)
            out.append(idx)
    return out
