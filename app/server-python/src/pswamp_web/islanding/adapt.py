# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Turning an islanding result into something a browser can use.

The application's result is shaped for a consumer that already has the time
window in hand -- which, in the Qt front end, it does. Two things therefore have
to be reconstructed here, and both are easy to get quietly wrong:

**Islands are column indices, not station names.** They index the application's
own frequency-only channel selection, so they mean nothing without its header.

**The main system is missing.** ``detect_islands`` sorts the groups it finds by
size and drops the largest, because the largest is the grid and reporting it as
an island would be noise. So a result of "one island" means *two* groups exist,
and the interesting question -- which stations are still connected to the main
system -- is answered by whatever is left over. The Qt alarm view does the same
subtraction; it is inherent to the result shape, not a workaround.
"""

import numpy as np

from ..stores import island_groups
from ..wire import Island, IslandingParameters, IslandingResult, sample

# Statuses the wire format accepts. An application sets its own status as a plain
# string, so a value outside this set is possible in principle and would fail
# validation on the way out; report it as unknown rather than dropping the whole
# result.
KNOWN_STATUSES = frozenset({"OK", "Alert", "Emergency", "Initializing...", "Undefined"})


def _mean_frequency(app, columns) -> float:
    """Mean frequency across a group of channels, over the analysis window.

    Read from the application's own window rather than carried in the result,
    which does not include it. Taken under the window's lock, and a little later
    than the evaluation it describes -- close enough for a readout, and not worth
    changing the upstream result shape for.

    get_safe, not snapshot: this is the islanding application's window, a stock
    ``TimeWindowLabeled``. Only the measurement store gets the counting subclass,
    and only because the delta protocol needs the append count -- nothing here
    does.
    """
    if len(columns) == 0:
        return float("nan")
    _, data = app.tw.get_safe(np.asarray(columns))
    with np.errstate(invalid="ignore"):
        return float(np.nanmean(data))


def to_wire(app, result: dict) -> IslandingResult | None:
    """Adapt one raw result dict. Returns None if it carries no assessment."""
    payload = result.get("result")
    if not payload:
        return None

    stations = np.asarray(app.tw.header["station"])
    # Index 0 is the main system, recovered by subtraction; see island_groups.
    islands = [
        Island(
            index=index,
            stations=[str(s) for s in stations[columns]],
            mean_freq=sample(_mean_frequency(app, columns)),
        )
        for index, columns in enumerate(
            island_groups(len(stations), payload["islands"])
        )
    ]

    info = result.get("info", {})
    parameters = result.get("parameters", {})
    return IslandingResult(
        t=float(payload.get("time_stamp") or result.get("time_stamp") or 0.0),
        app_uuid=str(info.get("uuid", "")),
        app_name=str(info.get("app_name", "IslandingApp")),
        status=app.status if app.status in KNOWN_STATUSES else "Undefined",
        islands=islands,
        parameters=IslandingParameters(
            window_length=float(parameters.get("window_length", 0.0)),
            mean_threshold=float(parameters.get("mean_threshold", 0.0)),
            eval_freq=float(parameters.get("eval_freq", 0.0)),
        ),
    )
