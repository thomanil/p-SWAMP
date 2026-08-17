# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Broker-free replay of a recorded, decoded PMU stream.

The labeled counterpart to ``pswamp.streaming.time_series_io``. That module
already showed p-SWAMP's io protocol can be satisfied without a broker, but its
decoder produces an *unlabeled* header (column names ``"0"``, ``"1"``, ...), so
``get_col_idx(measurement='f')`` finds nothing and an application like
``IslandingApp`` -- which selects its channels by ``{'measurement': 'f'}`` --
cannot run against it.

**This belongs upstream eventually**, as ``pswamp.streaming.recorded_io``: it is
generally useful for replaying real datasets in tests and CI, with no web server
involved. It lives here for now so this sandbox needs no changes to p-SWAMP at
all -- see §8.1 of ``doc/WIP-context-port-from-qt-to-web-frontend.md``. Nothing
in it depends on the rest of the web backend beyond ``Indexer``, so moving it is
a file move plus an import line.

Here the header is the same three-row ``station`` / ``channel`` / ``measurement``
table :class:`~pswamp.utils.pypmu.PMUDecoder` produces from a C37.118 config
frame, so an application is portable between a live PMU stream and a recording
without changing anything but its ``io`` and ``input_decoder`` arguments.

The pieces:

``Recording``
    A decoded stream on disk: the labeled header, a time vector, a 2-D data
    array, and the scenario events that produced it.
``LabeledRowDecoder``
    The decoder protocol over ``(meta, t, row)`` frames, with the same
    channel-selection surface as ``PMUDecoder``.
``RecordingPlayer``
    Owns the clock. One thread walks the recording at real-time pace and fans
    each row out to every subscriber.
``RecordedIO``
    The io protocol, one per application, fed by the player.

One player with N subscribers is the deliberate shape: it mirrors what a broker
gives you -- a single topic read by several consumers holding independent
offsets -- so swapping a recording for Kafka is a constructor change and nothing
else. Running each application off its own player instead would let them drift
apart in wall-clock time, and a grid event would appear at a different moment on
every screen watching it.
"""

import json
import logging
import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from pswamp.utils.time_window_labeled import Indexer

logger = logging.getLogger(__name__)

HEADER_ROWS = ("station", "channel", "measurement")

# Measurement types carrying a frequency, for which the C37.118 "no data" value
# is a hard zero rather than a flag. PMUDecoder substitutes NaN for these; this
# module offers the same option, and for the same reason.
FREQUENCY_MEASUREMENTS = ("f", "Df")


@dataclass(frozen=True)
class Recording:
    """A decoded, labeled PMU stream -- what an application would have read off
    the ``pmudata`` topic, captured once so it can be replayed deterministically.

    Attributes:
        header: ``{"station": (C,), "channel": (C,), "measurement": (C,)}`` of
            string arrays, the same table ``PMUDecoder.generate_header`` builds.
        time: ``(N,)`` time stamps in seconds, ascending.
        data: ``(N, C)`` measurements.
        data_rate: Nominal sampling rate in Hz.
        events: Scenario events, e.g.
            ``[{"t": 20.0, "kind": "line", "name": "L3244-6500",
                "action": "disconnect"}]``. Carried for provenance and so a
            consumer can align an assertion with the disturbance.
        source: Free-text provenance (simulation case, git revision, date).
    """

    header: dict
    time: np.ndarray
    data: np.ndarray
    data_rate: float
    events: tuple = ()
    source: str = ""

    def __post_init__(self):
        n, c = self.data.shape
        if self.time.shape != (n,):
            raise ValueError(
                f"time has {self.time.shape}, expected ({n},) to match data"
            )
        for row in HEADER_ROWS:
            if row not in self.header:
                raise ValueError(f"header is missing the {row!r} row")
            if len(self.header[row]) != c:
                raise ValueError(
                    f"header row {row!r} has {len(self.header[row])} entries, "
                    f"expected {c} to match data"
                )

    @property
    def n_samples(self):
        return self.data.shape[0]

    @property
    def n_channels(self):
        return self.data.shape[1]

    @property
    def duration(self):
        """Length of one pass in seconds, including the final sample's own
        interval -- so looping the recording keeps the sampling rate uniform
        across the seam instead of repeating an instant."""
        return float(self.time[-1] - self.time[0]) + 1.0 / self.data_rate

    def select(self, col_idx):
        """Return a copy keeping only the given columns. Used when recording, to
        drop channels the scenario does not need before writing to disk."""
        col_idx = np.asarray(col_idx)
        return Recording(
            header={row: np.asarray(self.header[row])[col_idx] for row in HEADER_ROWS},
            time=self.time,
            data=self.data[:, col_idx],
            data_rate=self.data_rate,
            events=self.events,
            source=self.source,
        )

    def col_idx(self, *args, **kwargs):
        """Look up columns the same way a TimeWindowLabeled does, e.g.
        ``rec.col_idx(measurement='f')``."""
        return Indexer(
            header={row: list(self.header[row]) for row in HEADER_ROWS}
        ).get_col_idx(*args, **kwargs)

    def save(self, path):
        """Write to a compressed .npz.

        Chosen over Parquet or HDF5 because loading needs nothing beyond numpy,
        which is already a hard dependency -- a recording stays readable anywhere
        the analysis core runs.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            path,
            time=np.asarray(self.time, dtype=np.float64),
            data=np.asarray(self.data, dtype=np.float32),
            header_station=np.asarray(self.header["station"]),
            header_channel=np.asarray(self.header["channel"]),
            header_measurement=np.asarray(self.header["measurement"]),
            meta=np.array(
                json.dumps(
                    {
                        "data_rate": self.data_rate,
                        "events": list(self.events),
                        "source": self.source,
                    }
                )
            ),
        )

    @classmethod
    def load(cls, path):
        with np.load(Path(path), allow_pickle=False) as npz:
            meta = json.loads(str(npz["meta"]))
            return cls(
                header={
                    "station": npz["header_station"],
                    "channel": npz["header_channel"],
                    "measurement": npz["header_measurement"],
                },
                time=npz["time"],
                data=npz["data"],
                data_rate=float(meta["data_rate"]),
                events=tuple(meta.get("events", ())),
                source=meta.get("source", ""),
            )


class LabeledRowDecoder:
    """Decoder for ``(meta, t, row)`` frames produced by a :class:`RecordedIO`.

    Deliberately mirrors :class:`~pswamp.utils.pypmu.PMUDecoder`: same
    constructor arguments, same ``generate_header`` return shape, same
    ``data_frame_to_row`` contract. An application written against one works
    against the other unchanged, which is the whole point -- the recording is
    meant to stand in for the live stream, not to be a special case applications
    have to know about.
    """

    def __init__(
        self,
        channel_selection=None,
        channel_selection_idx=None,
        substitute_zero_freq_with_nan=True,
    ):
        self.channel_selection = channel_selection
        if channel_selection_idx is not None:
            self.channel_selection = None
        self.channel_selection_idx = (
            np.array(channel_selection_idx)
            if channel_selection_idx is not None
            else slice(None)
        )

        self.substitute_zero_freq_with_nan = substitute_zero_freq_with_nan
        self.data_dtype = float

        # Index (into the *selected* columns) of the frequency channels, so the
        # zero-to-NaN substitution below can be applied without re-deriving it
        # per frame. Resolved by generate_header.
        self._freq_idx = None

    @staticmethod
    def _meta(data_frame):
        return data_frame[0]

    def get_data_rate(self, sample_data_frame):
        return self._meta(sample_data_frame)["data_rate"]

    def get_time_stamp(self, data_frame):
        return data_frame[1]

    def generate_header(self, sample_data_frame=None, config_frame=None):
        meta = (
            config_frame if config_frame is not None else self._meta(sample_data_frame)
        )
        full_header = {row: np.asarray(meta["header"][row]) for row in HEADER_ROWS}

        if self.channel_selection is not None:
            indexer = Indexer(
                header={row: list(full_header[row]) for row in HEADER_ROWS}
            )
            self.channel_selection_idx = indexer.get_col_idx(**self.channel_selection)

        if self.channel_selection_idx is None:
            self.channel_selection_idx = slice(None)

        header = {
            row: full_header[row][self.channel_selection_idx] for row in HEADER_ROWS
        }

        self._freq_idx = np.where(
            np.isin(header["measurement"], FREQUENCY_MEASUREMENTS)
        )[0]
        return header

    def data_frame_to_row(self, data_frame):
        _, t, row = data_frame
        row = np.asarray(row, dtype=self.data_dtype)[self.channel_selection_idx]

        if self.substitute_zero_freq_with_nan and self._freq_idx is not None:
            # Copy first: the row is a view into the recording's shared data
            # array, which every other subscriber is reading concurrently.
            row = row.copy()
            freq = row[self._freq_idx]
            freq[freq == 0] = np.nan
            row[self._freq_idx] = freq

        return t, row


class RecordingPlayer:
    """Walks a recording at real-time pace, fanning each row out to subscribers.

    Stands in for a broker: :meth:`subscribe` returns an independent
    :class:`RecordedIO`, and every subscriber sees the same row at the same
    instant.
    """

    def __init__(
        self,
        recording,
        speed=1.0,
        loop=True,
        rebase_to_wallclock=True,
        t_start=None,
    ):
        """
        Args:
            recording: The :class:`Recording` to play.
            speed: Playback rate multiplier. 1.0 is real time.
            loop: Restart from the beginning on reaching the end.
            rebase_to_wallclock: Map the recording's own time base (which
                typically starts at 0, since a simulator publishes simulation
                time) onto epoch seconds, so alarm and status timestamps read as
                "now". Without this, timestamps land in January 1970 and any
                consumer comparing them to ``time.time()`` misbehaves.
            t_start: Epoch second the first sample maps to. Defaults to the
                moment :meth:`start` is called.
        """
        self.recording = recording
        self.speed = float(speed)
        self.loop = loop
        self.rebase_to_wallclock = rebase_to_wallclock

        self._t_rec_0 = float(recording.time[0])
        self._duration = recording.duration

        # Resolved once, here rather than in start(), so a frame read before
        # playback begins (get_sample_data_frame, during application
        # construction) is on the same time base as every frame after it.
        if t_start is not None:
            self._t_zero = float(t_start)
        elif rebase_to_wallclock:
            self._t_zero = time.time()
        else:
            self._t_zero = self._t_rec_0

        # Built once: frame_at hands this to every subscriber on every row, and
        # at 50 Hz a fresh dict per frame per subscriber is pure garbage.
        self._meta = {
            "header": recording.header,
            "data_rate": recording.data_rate,
            "events": recording.events,
            "source": recording.source,
        }

        self._subscribers = []
        self._subscribers_lock = threading.Lock()

        self._stop_event = threading.Event()
        self._thread = None

        # Absolute, non-wrapping row index. Negative values are meaningful and
        # address samples *before* the first one played, which is how a
        # subscriber pre-fills a time window at startup.
        self._i = 0

    @property
    def cursor(self):
        """Absolute index of the next row to be played."""
        return self._i

    @property
    def stopped(self):
        return self._stop_event.is_set()

    @property
    def meta(self):
        """The config frame handed to every subscriber."""
        return self._meta

    def time_at(self, index):
        """Output time stamp for an absolute row index.

        Handles indices outside ``[0, n_samples)`` by counting whole passes:
        floor division and modulo agree on negatives, so index ``-1`` is the
        last row of the previous pass and lands one sample *before* the start.
        Time therefore increases monotonically across a loop seam, which
        matters because consumers such as ``AlarmHandler`` compare timestamps
        and go quiet if one ever moves backwards.
        """
        n = self.recording.n_samples
        pass_no, offset = divmod(int(index), n)
        t_rel = float(self.recording.time[offset]) - self._t_rec_0
        return self._t_zero + pass_no * self._duration + t_rel

    def row_at(self, index):
        """Data row for an absolute index. A read-only view, not a copy: every
        subscriber shares one array, and the decoder copies before mutating."""
        return self.recording.data[int(index) % self.recording.n_samples]

    def frame_at(self, index):
        return (self.meta, self.time_at(index), self.row_at(index))

    def subscribe(self, **io_kwargs):
        """Create an independent reader. Safe to call before or after start."""
        io = RecordedIO(self, **io_kwargs)
        with self._subscribers_lock:
            self._subscribers.append(io)
        return io

    def unsubscribe(self, io):
        with self._subscribers_lock:
            if io in self._subscribers:
                self._subscribers.remove(io)

    def start(self):
        if self._thread is not None:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name="RecordingPlayer", daemon=True
        )
        self._thread.start()

    def stop(self, timeout=2.0):
        self._stop_event.set()
        thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(timeout=timeout)
            if thread.is_alive():
                logger.warning(
                    "RecordingPlayer thread did not stop within %ss", timeout
                )

    def _run(self):
        interval = 1.0 / (self.recording.data_rate * self.speed)
        n = self.recording.n_samples
        next_tick = time.monotonic()

        while not self._stop_event.is_set():
            if not self.loop and self._i >= n:
                break

            frame = self.frame_at(self._i)
            with self._subscribers_lock:
                subscribers = list(self._subscribers)
            for io in subscribers:
                io._offer(frame)
            self._i += 1

            # Deadline pacing rather than sleep(interval), so playback does not
            # drift. On a long overrun the deadline is reset instead of firing a
            # burst of catch-up rows, which would arrive at the applications
            # faster than real time and distort any rate-dependent analysis.
            next_tick += interval
            now = time.monotonic()
            if now > next_tick + interval:
                next_tick = now
            if self._stop_event.wait(max(0.0, next_tick - now)):
                break

        self._stop_event.set()


class RecordedIO:
    """The p-SWAMP io protocol over a :class:`RecordingPlayer`.

    Input arrives on a queue the player fills; output is handed to a ``publish``
    callback instead of a broker producer. That callback is the seam a web
    server (or a test, or a file writer) hooks into -- applications keep calling
    ``handle_result`` / ``handle_output`` / ``handle_status`` exactly as they do
    against Kafka.
    """

    def __init__(self, player, publish=None, maxsize=None):
        """
        Args:
            player: The owning :class:`RecordingPlayer`.
            publish: ``publish(topic, payload)``, called from the application's
                own thread. Topics are ``"result"``, ``"status"``, and whatever
                the application passes to ``handle_output``.
            maxsize: Input queue depth. Defaults to four seconds of samples.
        """
        self.player = player
        self.publish = publish
        if maxsize is None:
            maxsize = max(64, int(4 * player.recording.data_rate))
        self._queue = queue.Queue(maxsize=maxsize)
        self._dropped = 0

    # -- input ---------------------------------------------------------------

    def _offer(self, frame):
        """Called from the player thread for live frames. Never blocks: a
        consumer that has fallen behind is dropped back to the newest data
        rather than allowed to stall the player -- and every other subscriber
        with it."""
        try:
            self._queue.put_nowait(frame)
        except queue.Full:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                pass
            self._dropped += 1
            if self._dropped % 100 == 1:
                logger.warning(
                    "RecordedIO consumer is behind; dropped %s frames so far",
                    self._dropped,
                )
            try:
                self._queue.put_nowait(frame)
            except queue.Full:
                pass

    def _push_history(self, frames):
        """Enqueue a burst of past frames without dropping any.

        Distinct from _offer because the two overflow conditions mean opposite
        things. A full queue on the live path means the consumer cannot keep up
        and the newest data matters most. A full queue here just means the burst
        is larger than a queue sized for streaming -- which is expected, since a
        time window is normally many seconds deep -- and dropping from it would
        silently hand the application a partly-filled window and an apparently
        healthy start.
        """
        needed = self._queue.qsize() + len(frames)
        if needed > self._queue.maxsize:
            self._queue.maxsize = needed
        for frame in frames:
            self._queue.put_nowait(frame)

    def get_config_frame(self):
        return self.player.meta

    def get_sample_data_frame(self):
        """A representative frame, read directly from the recording so it costs
        the caller nothing and does not disturb the queue. Applications use it
        at construction time to learn the header and the data rate."""
        return self.player.frame_at(self.player.cursor)

    def get_next_data_frame(self):
        """Block until the next frame arrives.

        Raises:
            StopIteration: when the player has stopped. ``SnapshotApp.update``
                already treats that as "the stream ended" and shuts the
                application down, so no application code needs to change.
        """
        while True:
            try:
                return self._queue.get(timeout=0.5)
            except queue.Empty:
                if self.player.stopped:
                    raise StopIteration
                # Otherwise the player is simply slower than the timeout (a very
                # low data rate, or paused); keep waiting.

    def seek_relative_input_offset(self, offset):
        """Move this reader's offset by ``offset`` samples.

        A recording is random-access, so unlike the pure-stream case this is
        real work rather than a no-op: the rows are pushed straight onto this
        reader's queue, and the application drains them as fast as its thread
        runs. That is how ``TimeWindowApp`` arrives with a full time window
        instead of spending the first window-length producing NaN results --
        the same effect as rewinding a consumer offset on a broker.

        Only rewinding (negative offset) is meaningful; a positive offset would
        mean skipping data that has not been produced yet, and is ignored.
        """
        offset = int(offset)
        if offset >= 0:
            return

        start = self.player.cursor + offset
        if not self.player.loop:
            # Without looping there is nothing before the first sample, so a
            # rewind past it simply yields less history than asked for.
            start = max(start, 0)
        self._push_history(
            [self.player.frame_at(index) for index in range(start, self.player.cursor)]
        )

    def get_next_command(self):
        """No command channel exists for a recording. Applications driven by
        ``run_in_thread`` never call this; only the blocking ``start()`` does."""
        return None

    # -- output --------------------------------------------------------------

    def _emit(self, topic, payload):
        if self.publish is not None:
            self.publish(topic, payload)

    def handle_result(self, result):
        if result is None:
            return
        self._emit("result", result)

    def handle_output(self, topic, output):
        self._emit(topic, output)

    def handle_status(self, status):
        self._emit("status", status)
