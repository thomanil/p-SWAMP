# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The process-wide pipeline: one PMU source, and the applications reading it.

This is where the server departs from the pattern the other app packages in this
repo follow, and the departure is worth stating plainly.

``timeline`` and ``pmu_test_streamer`` give every client its own model, because
their "state" is a cursor into a sequence and two people looking at it have no
reason to agree. A grid monitor is the opposite: there is one grid, one replay of
it, and one islanding detector, no matter how many browsers are open. Two
operators seeing different frequencies would be a bug, not a feature.

So the split moves rather than disappearing. The Hub owns the shared, read-only
data -- the recording, the player, the application threads, the alarm and status
stores. Each page package keeps its own ``states: dict[int, ClientState]``, but
what lives in it is now view state: which channels this client selected, what it
was last sent. The invariant becomes: state is per client, data is process-wide.
"""

import logging
import threading

from pswamp.monitoring.islanding import IslandingApp
from pswamp.monitoring.line_outage_detection import LineOutageDetectionApp

from .bus import Bus
from .recorded_io import LabeledRowDecoder, RecordingPlayer
from .replay import (
    ISLANDING_EVAL_HZ,
    ISLANDING_WINDOW_SECONDS,
    WINDOW_SECONDS,
    MeasurementStoreApp,
    load_recording,
)
from .stores import AlarmStore, AppStatusStore, IslandStore, LineOutageStore
from .wire import ReplayStatus

logger = logging.getLogger("pswamp_web.hub")

# Topic the islanding application's results are published on. Results carry no
# reliable marker of which application produced them, so the source is encoded in
# the topic; status and alarm messages already name their application.
ISLANDING_RESULT_TOPIC = "islanding.result"
LINE_OUTAGE_RESULT_TOPIC = "line_outage.result"


class Hub:
    """Owns the replay and the applications. One instance per process."""

    def __init__(self) -> None:
        self.bus = Bus()
        self.alarms = AlarmStore()
        self.statuses = AppStatusStore()
        self.islands = IslandStore()
        self.line_outages = LineOutageStore()

        self.recording = None
        self.player: RecordingPlayer | None = None
        self.store_app: MeasurementStoreApp | None = None
        self.islanding_app: IslandingApp | None = None
        self.line_outage_app: LineOutageDetectionApp | None = None

        self._threads: list[threading.Thread] = []
        self._detach: list = []
        self._started = False

    # -- lifecycle -----------------------------------------------------------

    def start(self, loop) -> None:
        if self._started:
            return
        self.bus.bind(loop)

        self.recording = load_recording()
        logger.info(
            "loaded recording: %s samples x %s channels @ %s Hz (%s)",
            self.recording.n_samples,
            self.recording.n_channels,
            self.recording.data_rate,
            self.recording.source,
        )

        self.player = RecordingPlayer(self.recording, speed=1.0, loop=True)

        # One reader per application, all off the same player, so every
        # application sees the same sample at the same instant -- the property a
        # single Kafka topic with independent consumer offsets would give.
        self.store_app = MeasurementStoreApp(
            io=self.player.subscribe(publish=self._publisher()),
            input_decoder=LabeledRowDecoder,
            window_length=WINDOW_SECONDS,
            eval_freq=1,
            app_name="Measurement Store",
        )
        self.islanding_app = IslandingApp(
            io=self.player.subscribe(publish=self._publisher(ISLANDING_RESULT_TOPIC)),
            input_decoder=LabeledRowDecoder,
            window_length=ISLANDING_WINDOW_SECONDS,
            eval_freq=ISLANDING_EVAL_HZ,
        )

        # Line outage detection reads current magnitudes, which the other
        # applications ignore. It is silent unless a branch changes state, so it
        # costs almost nothing when the grid is intact.
        # No detection parameters passed: threshold, window and evaluation rate
        # are the application's own defaults and belong to it, not to this
        # transport layer. Passing copies of them here would silently pin today's
        # values if they were ever retuned upstream.
        self.line_outage_app = LineOutageDetectionApp(
            io=self.player.subscribe(publish=self._publisher(LINE_OUTAGE_RESULT_TOPIC)),
            input_decoder=LabeledRowDecoder,
        )

        self.islands.attach(self.islanding_app)
        self._attach_stores()

        # Applications first, player second. Each application already has its
        # pre-filled window sitting in its reader queue, so it has work to do
        # immediately; starting the player first would leave it publishing into
        # queues nobody is draining yet, and the first samples would be dropped
        # as though a consumer were lagging.
        for app in (self.store_app, self.islanding_app, self.line_outage_app):
            app.run_in_thread()
            self._threads.append(app.runner_thread)
        self.player.start()

        self._started = True
        logger.info("pipeline started")

    def stop(self) -> None:
        """Tear down, newest-dependency-first. Called off the event loop."""
        if not self._started:
            return
        for app in (self.line_outage_app, self.islanding_app, self.store_app):
            if app is not None:
                app.stop()
        if self.player is not None:
            # Wakes any application blocked waiting for the next frame: the read
            # times out, sees the player has stopped, and raises StopIteration,
            # which SnapshotApp already treats as end-of-stream.
            self.player.stop()
        for thread in self._threads:
            thread.join(timeout=2.0)
            if thread.is_alive():
                logger.warning(
                    "application thread %s did not stop in time", thread.name
                )
        self._threads.clear()

        for detach in self._detach:
            detach()
        self._detach.clear()

        self.bus.bind(None)
        self._started = False
        logger.info("pipeline stopped")

    # -- internals -----------------------------------------------------------

    def _publisher(self, result_topic: str | None = None):
        """Build the publish callback handed to one application's reader.

        Called from that application's own thread, so everything it does must be
        thread-safe; publishing to the bus is, by construction.
        """
        bus = self.bus

        def publish(topic: str, payload: object) -> None:
            if topic == "result" and result_topic is not None:
                topic = result_topic
            elif topic == "result":
                return  # an application whose results nothing subscribes to
            bus.publish_threadsafe(topic, payload)

        return publish

    def _attach_stores(self) -> None:
        """Feed the alarm and status stores from the bus, for the process
        lifetime. Both are pure state, so they consume synchronously rather than
        through a queue they would have to be drained from."""
        self._detach = [
            self.bus.add_listener("status", self.statuses.handle),
            self.bus.add_listener("alarms", self.alarms.handle),
            self.bus.add_listener(ISLANDING_RESULT_TOPIC, self.islands.handle),
            self.bus.add_listener(LINE_OUTAGE_RESULT_TOPIC, self.line_outages.handle),
        ]

    # -- read-side helpers ---------------------------------------------------

    def replay_status(self) -> ReplayStatus:
        player, recording = self.player, self.recording
        if player is None or recording is None:
            return ReplayStatus(
                source="",
                playing=False,
                data_rate=0,
                n_samples=0,
                n_channels=0,
                cursor=0,
                position=0.0,
                duration=0.0,
            )
        cursor = player.cursor
        return ReplayStatus(
            source=recording.source,
            playing=not player.stopped,
            data_rate=recording.data_rate,
            n_samples=recording.n_samples,
            n_channels=recording.n_channels,
            cursor=cursor,
            position=(cursor % recording.n_samples) / recording.data_rate,
            duration=recording.duration,
        )


HUB = Hub()
