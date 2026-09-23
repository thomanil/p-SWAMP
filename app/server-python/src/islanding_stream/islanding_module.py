# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The islanding module: p-SWAMP's islanding detector as a core module.

``detect_islands`` and ``moving_average`` are **copied** from the desktop
package (``src/pswamp/monitoring/islanding.py``) rather than imported: the
desktop ``IslandingApp`` is a ``TimeWindowApp`` that runs its own thread over
its own ``io``, and the point here is the analysis as a module that consumes a
topic -- in another process when the environment says so. The defaults are the
desktop app's too: a 10 s window of every station's frequency, evaluated once a
second, with a 0.05 Hz RMS threshold, and a zero frequency read as missing (the
desktop decoder's ``substitute_zero_freq_with_nan``).

It evaluates once per second of **data** time, so a replay at 20x runs the
detector twenty times a wall-clock second: the replay speed is the load knob,
and it loads the analysis and the topic together.

Each result carries what the module saw of its own input as well as what it
found -- frames in since the last result, how long the detection took, how old
its input was and how much of it was dropped -- so the page can show how close
to the edge it is running before the keep-up reports (``Module.run``) say it
went over.

Imports only ``numpy``, ``pydantic`` and ``pswamp_core`` -- nothing from the web
stack or the desktop package -- so it is the same code in the server and in
the worker (``worker.py``).
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Literal

import numpy as np
from pydantic import BaseModel, Field

from pswamp_core.messages import PmuFrame, PmuHeader, ResultEnvelope
from pswamp_core.modules import Module

__all__ = ["IslandingModule", "IslandingStreamResult", "Islands", "detect_islands"]

#: Seconds of frequency history the detector considers; the desktop default.
WINDOW_SECONDS = 10.0
#: Seconds of data time between evaluations (the desktop's ``eval_freq=1``).
EVAL_INTERVAL_S = 1.0
#: RMS distance, in Hz, under which two stations move together.
MEAN_THRESHOLD = 0.05


# --- copied from src/pswamp/monitoring/islanding.py ------------------------------------


def moving_average(x, w):
    return np.convolve(x, np.ones(w), "valid") / w


def detect_islands(t, data, mean_threshold=0.05):
    if np.any(np.isnan(t)):
        return

    freq_raw = data

    freq = np.array([moving_average(f, 10) for f in freq_raw.T]).T
    mean_freq = np.mean(freq, axis=0)

    i_island = 0
    n_islands_max = 10

    island_idx = -np.ones(freq.shape[1], dtype=int)
    assigned = np.zeros(freq.shape[1], dtype=bool)
    assigned[np.isnan(mean_freq)] = True

    islands = []

    while i_island < n_islands_max:
        ref_meas = np.argmax(~assigned)

        same_mv_avg_as_ref_meas = (
            np.linalg.norm(freq[:, [ref_meas]] - freq, axis=0) / np.sqrt(len(t)) < mean_threshold
        )

        assign_these = same_mv_avg_as_ref_meas
        island_idx[assign_these & ~assigned] = i_island
        assigned += assign_these

        islands.append(np.where(assign_these)[0])

        if np.all(assigned):
            break

        i_island += 1

    sort_idx = np.argsort([-len(island_) for island_ in islands])
    islands = [islands[idx_] for idx_ in sort_idx]
    islands = islands[1:]

    return t[-1], islands


# --- end of the copy ---------------------------------------------------------------------


class Islands(BaseModel):
    """What one evaluation found, and how the module was keeping up when it did."""

    status: Literal["OK", "Emergency"] = Field(
        description="Emergency while any group of stations has separated from the main system."
    )
    islands: list[list[str]] = Field(
        description="Each separated group's stations, largest first; the main system is not listed."
    )
    main_system: int = Field(description="How many stations are still in the main system.")
    stations: int = Field(description="How many stations the window holds a frequency for.")
    window_s: float = Field(description="Seconds of frequency history the detector looked at.")
    frames_in: int = Field(description="Frames processed since the previous result.")
    detect_ms: float = Field(description="Wall-clock milliseconds this evaluation took.")
    input_age_s: float | None = Field(
        description="How long the last input had been in flight when it was read; null in-process."
    )
    input_dropped: int = Field(description="Input dropped by this module's queue so far.")


class IslandingStreamResult(ResultEnvelope[Islands]):
    """The module's envelope; its class name is its topic: ``islanding.stream.result``."""

    version: Literal["v1"] = "v1"


class IslandingModule(Module):
    """Keep a window of every station's frequency; detect islands once a data-second."""

    name = "islanding"
    input_model = PmuFrame
    output_model = IslandingStreamResult

    def __init__(self) -> None:
        super().__init__()
        self._header_id: str | None = None
        self._columns: np.ndarray = np.empty(0, dtype=int)
        self._stations: list[str] = []
        self._times = np.empty(0)
        self._freq = np.empty((0, 0))
        self._cursor = 0
        self._filled = 0
        self._last_time: datetime | None = None
        self._next_eval: float | None = None
        self._frames_in = 0
        self.parameters = {
            "window_s": WINDOW_SECONDS,
            "eval_interval_s": EVAL_INTERVAL_S,
            "mean_threshold": MEAN_THRESHOLD,
        }

    def use_header(self, header: PmuHeader) -> None:
        """Derive the frequency columns and size the window for ``header``."""
        self._header_id = header.header_id
        self._columns = np.asarray(header.columns(measurement="f"), dtype=int)
        self._stations = [header.station[i] for i in self._columns]
        size = max(1, round(WINDOW_SECONDS * header.data_rate))
        self._times = np.full(size, np.nan)
        self._freq = np.full((size, self._columns.size), np.nan)
        self._reset()
        self.parameters = {**self.parameters, "header_id": header.header_id}

    def _reset(self) -> None:
        self._cursor = 0
        self._filled = 0
        self._next_eval = None
        self._times[:] = np.nan

    async def process(self, frame: PmuFrame) -> Islands | None:
        if frame.header.header_id != self._header_id:
            self.use_header(frame.header)
        if self._columns.size == 0:
            return None
        # Time going backwards is a loop or a seek: the window no longer
        # describes one stretch of the grid, so start it again.
        if self._last_time is not None and frame.timestamp <= self._last_time:
            self._reset()
        self._last_time = frame.timestamp
        self._frames_in += 1

        t = frame.timestamp.timestamp()
        row = np.array([frame.values[i] for i in self._columns], dtype=float)  # None -> nan
        row[row == 0] = np.nan  # the desktop decoder's substitute_zero_freq_with_nan
        self._times[self._cursor] = t
        self._freq[self._cursor] = row
        self._cursor = (self._cursor + 1) % self._times.size
        self._filled = min(self._filled + 1, self._times.size)

        if self._next_eval is None:
            self._next_eval = t + EVAL_INTERVAL_S
        if self._filled < self._times.size or t < self._next_eval:
            return None
        self._next_eval = t + EVAL_INTERVAL_S
        return self._evaluate()

    def _evaluate(self) -> Islands | None:
        order = np.r_[self._cursor : self._times.size, 0 : self._cursor]
        started = time.perf_counter()
        found = detect_islands(self._times[order], self._freq[order], MEAN_THRESHOLD)
        detect_ms = (time.perf_counter() - started) * 1000
        if found is None:
            return None
        _, raw_islands = found
        # detect_islands can report a station in two groups (it masks assigned
        # channels when labelling them, not when listing them); claim each for
        # the first group that reports it, as pswamp_web's stores.island_groups
        # does, so the groups are a partition.
        claimed: set[int] = set()
        islands: list[list[str]] = []
        for island in raw_islands:
            fresh = [int(i) for i in island if int(i) not in claimed]
            claimed.update(fresh)
            if fresh:
                islands.append([self._stations[i] for i in fresh])
        frames_in, self._frames_in = self._frames_in, 0
        return Islands(
            status="Emergency" if islands else "OK",
            islands=islands,
            main_system=len(self._stations) - len(claimed),
            stations=len(self._stations),
            window_s=WINDOW_SECONDS,
            frames_in=frames_in,
            detect_ms=round(detect_ms, 3),
            input_age_s=None if self.monitor.input_age_s is None else round(self.monitor.input_age_s, 3),
            input_dropped=self.monitor.input_dropped,
        )
