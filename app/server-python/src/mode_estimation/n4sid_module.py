# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The mode-estimation module: p-SWAMP's N4SID system identification as a core module.

``N4SID`` is **copied** from the desktop package (``src/pswamp/monitoring/n4sid.py``)
rather than imported, as the islanding stream copies ``detect_islands``: the
desktop ``N4SIDApp`` is a ``TimeWindowApp`` with its own thread and ``io``, and
what is wanted here is the analysis as a module consuming a topic. It uses the
same ``nfoursid`` library (a dependency of this server for exactly this). The
defaults are the desktop's ``run_n4sid``: every station's frequency over a 45 s
window, identified at order 10 with 10 block rows, once a second of data;
Emergency under 3 % damping of an electromechanical mode (0.1–2 Hz), Alert
under 7 %.

**It is the heavy module.** One identification over 45 s x 44 stations at 50 Hz
takes ~200 ms of CPU, against the islanding detector's half a millisecond --
and it runs once per second of *data*, so the replay speed multiplies it. How
it runs is therefore the variable under test, chosen by
``MODE_ESTIMATION_EXECUTION`` in the process the module runs in:

``inline``
    ``run_sid`` called straight from ``process``, on the event loop. The naive
    port. While it runs, nothing else on that loop does -- in the worker, no
    other client's module and not the consumer either -- so falling behind
    shows up as *input* that is old or dropped.
``thread`` (default)
    Off the loop, in a thread pool (``MODE_ESTIMATION_POOL_SIZE`` threads).
    ``process`` keeps appending frames while an identification runs; one that
    falls due while this client's previous one is still running is **skipped**
    and counted, and the result goes out with the frame that finds it done.
    numpy's LAPACK releases the GIL; nfoursid's and pandas' Python does not.
``process``
    The same, in a process pool: no GIL between identifications, at the price
    of pickling the window (~0.8 MB) to the child and back.

Each result carries the timings that tell those apart: ``compute_ms`` (wall
time inside the identification), ``compute_cpu_ms`` (the CPU time of the
thread or process that ran it -- the gap to wall is waiting for the GIL or a
core), ``queue_ms`` (submitted to started: waiting for a pool slot) and
``latency_ms`` (falling due to the result being published), plus the input
readings every module has. Skipped identifications are reported on the error
tray through the same ``KeepUpMonitor`` as dropped input, in their own unit.

Imports only ``numpy``, ``pandas``, ``scipy``, ``nfoursid``, ``pydantic`` and
``pswamp_core`` -- nothing from the web stack or the desktop package.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import multiprocessing
import os
import time
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Literal

import numpy as np
import pandas as pd
from nfoursid.nfoursid import NFourSID
from pydantic import BaseModel, Field
from scipy.linalg import logm
from scipy.linalg._matfuncs_inv_ssq import LogmExactlySingularWarning, LogmNearlySingularWarning

from pswamp_core.messages import PmuFrame, PmuHeader, ResultEnvelope
from pswamp_core.modules import KeepUpMonitor, Module

if TYPE_CHECKING:
    from pswamp_core.bus import Bus
    from pswamp_core.datagateway import DataGateway

__all__ = ["N4SID", "ModeEstimationResult", "Modes", "N4SIDModule", "identify"]

#: Seconds of frequency history identified at once; the desktop ``run_n4sid`` default.
WINDOW_SECONDS = float(os.environ.get("MODE_ESTIMATION_WINDOW_S", "45"))
#: Seconds of data time between identifications (the desktop's ``eval_freq=1``).
EVAL_INTERVAL_S = 1.0
SYS_ORDER = 10
NUM_BLOCK_ROWS = 10
#: Damping ratios under which an electromechanical mode raises the status.
EMERGENCY_DAMPING = 0.03
ALERT_DAMPING = 0.07

Execution = Literal["inline", "thread", "process"]
EXECUTION_VARIABLE = "MODE_ESTIMATION_EXECUTION"
POOL_SIZE_VARIABLE = "MODE_ESTIMATION_POOL_SIZE"


# --- copied from src/pswamp/monitoring/n4sid.py (the N4SID class, unchanged) ---------

warnings.simplefilter("error", category=LogmNearlySingularWarning)
warnings.simplefilter("error", category=LogmExactlySingularWarning)


class N4SID:
    """
    Class for running system identification using the NFourSID package.
    """
    def __init__(self, dt, n_measurements, sys_order=10, num_block_rows=10):
        self.n_measurements = n_measurements
        self.sys_order = sys_order
        self.num_block_rows = num_block_rows
        self.dt = dt

        self.eigs = np.zeros(self.sys_order, dtype=complex)
        self.damping = np.zeros(self.sys_order)
        self.freq = np.zeros(self.sys_order)
        self.em_idx = np.zeros(self.sys_order, dtype=bool)

        self.rev = np.zeros((self.sys_order,) * 2, dtype=complex)
        self.a_c = np.zeros((self.sys_order,) * 2, dtype=complex)
        self.c = np.zeros((self.n_measurements, self.sys_order), dtype=complex)
        self.mode_shapes = np.zeros(
            (self.n_measurements, self.sys_order), dtype=complex
        )

    def run_sid(self, y):
        """Run system identification (N4SID) and calculate eigenvalues and observability mode shapes.
        Args:
            y:

        Returns:

        """
        y_names = ["y{}".format(i) for i in range(y.shape[1])]
        y_df = pd.DataFrame(columns=y_names, data=y)
        #
        nfoursid = NFourSID(
            y_df,
            output_columns=y_names,
            num_block_rows=self.num_block_rows,
        )
        nfoursid.subspace_identification()
        state_space_identified, covariance_matrix = nfoursid.system_identification(
            rank=self.sys_order
        )

        try:
            a_log_mat = logm(state_space_identified.a)
        except (LogmNearlySingularWarning, LogmExactlySingularWarning):
            self.eigs *= np.nan
            self.damping *= np.nan
            self.freq *= np.nan
            self.rev *= np.nan
            self.mode_shapes *= np.nan
            return self.eigs, self.mode_shapes

        self.a_c = (np.array(a_log_mat)/self.dt)  # Convert discrete system to continuous
        self.c = state_space_identified.c
        self.eigs, self.rev = np.linalg.eig(
            self.a_c
        )  # Compute eigenvalues from continuous system matrix
        self.damping = -self.eigs.real / abs(self.eigs)
        self.freq = self.eigs.imag / (2 * np.pi)

        # Sorting: Lowest damping first. Non-oscillatory eigenvalues are left out of sorting.
        self.em_idx = (abs(self.eigs.imag)/(2*np.pi) > 0.1) & (abs(self.eigs.imag)/(2*np.pi) < 2)
        to_be_sorted = self.em_idx  # (abs(self.eigs)> 1e-6) & (abs(self.eigs.imag) > 1e-6)
        sort_idx = np.argsort(self.damping[to_be_sorted])

        self.eigs = np.concatenate([self.eigs[to_be_sorted][sort_idx], self.eigs[~to_be_sorted]])
        self.damping = np.concatenate([self.damping[to_be_sorted][sort_idx], self.damping[~to_be_sorted]])
        self.freq = np.concatenate([self.freq[to_be_sorted][sort_idx], self.freq[~to_be_sorted]])
        self.rev = np.hstack([self.rev[:, to_be_sorted][:, sort_idx], self.rev[:, ~to_be_sorted]])

        self.em_idx = np.concatenate([self.em_idx[to_be_sorted][sort_idx], self.em_idx[~to_be_sorted]])

        self.mode_shapes = self.c.dot(self.rev)  # Observability mode shapes

        return self.eigs, self.mode_shapes


# --- end of the copy ---------------------------------------------------------------------


@dataclass(frozen=True)
class Identified:
    """One identification's outcome, as plain arrays: what crosses back from a
    thread or a child process."""

    freq: np.ndarray
    damping: np.ndarray
    em_idx: np.ndarray
    mode_shapes: np.ndarray
    started: float  # time.time() when the identification began
    compute_ms: float
    compute_cpu_ms: float


def identify(dt: float, y: np.ndarray, sys_order: int, num_block_rows: int, cpu_clock: str) -> Identified:
    """Run one N4SID over ``y`` and time it. A module-level function so a
    process pool can pickle it; ``cpu_clock`` is ``thread`` or ``process``."""
    started = time.time()
    wall0 = time.perf_counter()
    cpu0 = time.thread_time() if cpu_clock == "thread" else time.process_time()
    sid = N4SID(dt, y.shape[1], sys_order=sys_order, num_block_rows=num_block_rows)
    sid.run_sid(y)
    cpu1 = time.thread_time() if cpu_clock == "thread" else time.process_time()
    return Identified(
        freq=np.asarray(sid.freq, dtype=float),
        damping=np.asarray(sid.damping, dtype=float),
        em_idx=np.asarray(sid.em_idx, dtype=bool),
        mode_shapes=np.asarray(sid.mode_shapes),
        started=started,
        compute_ms=(time.perf_counter() - wall0) * 1000,
        compute_cpu_ms=(cpu1 - cpu0) * 1000,
    )


# --- the pools, one per process, shared by every module instance in it ------------------

_POOLS: dict[str, concurrent.futures.Executor] = {}


def pool_size() -> int:
    return int(os.environ.get(POOL_SIZE_VARIABLE, "") or (os.cpu_count() or 1))


def _pool(execution: Execution) -> concurrent.futures.Executor:
    pool = _POOLS.get(execution)
    if pool is None:
        if execution == "process":
            # spawn, not fork: this process runs an event loop and aiokafka's
            # threads, which a forked child would inherit half-copied.
            pool = concurrent.futures.ProcessPoolExecutor(
                max_workers=pool_size(), mp_context=multiprocessing.get_context("spawn")
            )
        else:
            pool = concurrent.futures.ThreadPoolExecutor(max_workers=pool_size(), thread_name_prefix="n4sid")
        _POOLS[execution] = pool
    return pool


def execution_from_env() -> Execution:
    value = os.environ.get(EXECUTION_VARIABLE, "thread").strip().lower()
    if value not in ("inline", "thread", "process"):
        raise ValueError(f"{EXECUTION_VARIABLE} must be inline, thread or process, not {value!r}")
    return value  # type: ignore[return-value]


# --- the wire -------------------------------------------------------------------------------


class Mode(BaseModel):
    """One electromechanical mode (0.1–2 Hz), least damped first."""

    freq_hz: float = Field(description="Oscillation frequency.")
    damping: float = Field(description="Damping ratio; under 0.03 is an emergency, under 0.07 an alert.")
    stations: list[str] = Field(description="The stations taking most part in it, most first (mode shape magnitude).")
    participation: list[float] = Field(description="Their mode shape magnitudes, normalised to the largest.")


class Modes(BaseModel):
    """What one identification found, and how the module was keeping up when it did."""

    status: Literal["OK", "Alert", "Emergency", "Undetermined"] = Field(
        description="From the least damped mode; Undetermined when the identification was singular."
    )
    modes: list[Mode] = Field(description="Electromechanical modes, least damped first; up to five.")
    window_end: datetime = Field(description="The data time of the window's last sample.")
    window_s: float = Field(description="Seconds of frequency history identified.")
    stations: int = Field(description="How many stations' frequency the window holds.")
    execution: Execution = Field(description="How the identification ran: on the event loop, a thread or a process.")
    compute_ms: float = Field(description="Wall-clock milliseconds inside the identification.")
    compute_cpu_ms: float = Field(description="CPU milliseconds of the thread or process that ran it.")
    queue_ms: float = Field(description="Milliseconds from submission to start: waiting for a pool slot.")
    latency_ms: float = Field(description="Milliseconds from falling due to this result.")
    evaluations: int = Field(description="Identifications that fell due so far.")
    evaluations_skipped: int = Field(description="Of those, skipped because the previous one had not finished.")
    frames_in: int = Field(description="Frames processed since the previous result.")
    input_age_s: float | None = Field(
        description="How long the last input had been in flight when it was read; null in-process."
    )
    input_dropped: int = Field(description="Input dropped by this module's queue so far.")


class ModeEstimationResult(ResultEnvelope[Modes]):
    """The module's envelope; its class name is its topic: ``mode.estimation.result``."""

    version: Literal["v1"] = "v1"


@dataclass
class _Pending:
    future: asyncio.Future
    submitted: float
    due: float
    window_end: datetime


class N4SIDModule(Module):
    """Keep a window of every station's frequency; identify its modes once a data-second."""

    name = "n4sid"
    input_model = PmuFrame
    output_model = ModeEstimationResult

    def __init__(self, execution: Execution | None = None) -> None:
        super().__init__()
        self.execution: Execution = execution or execution_from_env()
        self._header_id: str | None = None
        self._columns = np.empty(0, dtype=int)
        self._stations: list[str] = []
        self._dt = 0.02
        self._times = np.empty(0)
        self._freq = np.empty((0, 0))
        self._cursor = 0
        self._filled = 0
        self._last_time: datetime | None = None
        self._next_eval: float | None = None
        self._frames_in = 0
        self._pending: _Pending | None = None
        self.evaluations = 0
        self.skipped = 0
        self._bus: Bus | None = None
        self.eval_monitor = KeepUpMonitor(
            self.name,
            f"cannot identify every {EVAL_INTERVAL_S:g} s of data",
            self.keep_up,
            label="the n4sid analysis",
            unit="identifications skipped",
        )
        self.parameters = {
            "window_s": WINDOW_SECONDS,
            "eval_interval_s": EVAL_INTERVAL_S,
            "sys_order": SYS_ORDER,
            "num_block_rows": NUM_BLOCK_ROWS,
            "execution": self.execution,
        }

    async def setup(self, gateway: DataGateway, bus: Bus) -> None:
        self._bus = bus

    def use_header(self, header: PmuHeader) -> None:
        self._header_id = header.header_id
        self._columns = np.asarray(header.columns(measurement="f"), dtype=int)
        self._stations = [header.station[i] for i in self._columns]
        self._dt = 1.0 / header.data_rate
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

    async def process(self, frame: PmuFrame) -> Modes | None:
        if frame.header.header_id != self._header_id:
            self.use_header(frame.header)
        if self._columns.size == 0:
            return None
        if self._last_time is not None and frame.timestamp <= self._last_time:
            self._reset()  # a loop or a seek: the window no longer describes one stretch
        self._last_time = frame.timestamp
        self._frames_in += 1

        t = frame.timestamp.timestamp()
        self._times[self._cursor] = t
        self._freq[self._cursor] = [frame.values[i] for i in self._columns]  # None -> nan
        self._cursor = (self._cursor + 1) % self._times.size
        self._filled = min(self._filled + 1, self._times.size)

        # A finished background identification goes out with this frame.
        finished = self._collect()

        if self._next_eval is None:
            self._next_eval = t + EVAL_INTERVAL_S
        if self._filled < self._times.size or t < self._next_eval:
            return finished
        self._next_eval = t + EVAL_INTERVAL_S
        self.evaluations += 1

        order = np.r_[self._cursor : self._times.size, 0 : self._cursor]
        window = self._freq[order]
        if np.isnan(window).any():
            return finished
        due = time.time()

        if self.execution == "inline":
            done = identify(self._dt, window, SYS_ORDER, NUM_BLOCK_ROWS, "thread")
            return self._modes(done, due=due, submitted=due, window_end=frame.timestamp)

        if self._pending is not None:
            # This client's previous identification is still running: skip this one.
            self.skipped += 1
            self._note_skips(1)
            return finished
        self._note_skips(0)
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(
            _pool(self.execution), identify, self._dt, window, SYS_ORDER, NUM_BLOCK_ROWS,
            "thread" if self.execution == "thread" else "process",
        )
        self._pending = _Pending(future, submitted=time.time(), due=due, window_end=frame.timestamp)
        return finished

    def _note_skips(self, n: int) -> None:
        if self._bus is not None:
            self.eval_monitor.note(self._bus, n)

    def _collect(self) -> Modes | None:
        pending = self._pending
        if pending is None or not pending.future.done():
            return None
        self._pending = None
        try:
            done = pending.future.result()
        except Exception as error:  # surfaced like any failure of process()
            raise RuntimeError(f"identification failed: {type(error).__name__}: {error}") from error
        return self._modes(done, due=pending.due, submitted=pending.submitted, window_end=pending.window_end)

    def _modes(self, done: Identified, *, due: float, submitted: float, window_end: datetime) -> Modes:
        modes: list[Mode] = []
        if not np.isnan(done.damping).all():
            for k in np.flatnonzero(done.em_idx):
                if done.freq[k] <= 0:
                    continue  # one of each conjugate pair
                shape = np.abs(done.mode_shapes[:, k])
                top = np.argsort(-shape)[:5]
                peak = shape[top[0]] or 1.0
                modes.append(
                    Mode(
                        freq_hz=round(float(done.freq[k]), 4),
                        damping=round(float(done.damping[k]), 4),
                        stations=[self._stations[i] for i in top],
                        participation=[round(float(shape[i] / peak), 3) for i in top],
                    )
                )
                if len(modes) == 5:
                    break
        if np.isnan(done.damping).all():
            status = "Undetermined"
        elif any(m.damping < EMERGENCY_DAMPING for m in modes):
            status = "Emergency"
        elif any(m.damping < ALERT_DAMPING for m in modes):
            status = "Alert"
        else:
            status = "OK"
        frames_in, self._frames_in = self._frames_in, 0
        return Modes(
            status=status,
            modes=modes,
            window_end=window_end,
            window_s=WINDOW_SECONDS,
            stations=len(self._stations),
            execution=self.execution,
            compute_ms=round(done.compute_ms, 2),
            compute_cpu_ms=round(done.compute_cpu_ms, 2),
            queue_ms=round(max(0.0, done.started - submitted) * 1000, 2),
            latency_ms=round((time.time() - due) * 1000, 2),
            evaluations=self.evaluations,
            evaluations_skipped=self.skipped,
            frames_in=frames_in,
            input_age_s=None if self.monitor.input_age_s is None else round(self.monitor.input_age_s, 3),
            input_dropped=self.monitor.input_dropped,
        )
