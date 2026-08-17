"""Pure domain model for the PMU test streamer: a position in a file of records.

No I/O beyond loading the data file once at import, and no knowledge of
WebSockets — api.py owns all of that.

sample_data.txt is a **one-off sample committed for testing**, not a live feed or a
generated artifact: 300 PMU records extracted by hand from the Nordic 44 grid
simulation in the p-SWAMP project

It exists purely to give the streamer something realistic to replay while the
client-server shape is being worked out. Nothing here parses it — a record is
whatever one line says it is — so replacing or extending the file needs no code
change as long as it stays one record per line.
"""

from pathlib import Path

# Replay at the rate the data was recorded, i.e. real time. The records are PMU
# frames sampled every 50ms (20 Hz) at five stations, so one timestamp is five
# consecutive lines and wall-clock speed is 20 * 5 = 100 lines/s. Derived rather
# than hardcoded, so re-extracting the data with a different station count or sample
# rate keeps playback honest — just update these two numbers to match the file.
SAMPLE_HZ = 20  # PMU frames per second in sample_data.txt
STATIONS_PER_FRAME = 5  # lines sharing one timestamp
TICKS_PER_SECOND = SAMPLE_HZ * STATIONS_PER_FRAME
WINDOW_RADIUS = 4  # records shown on each side of the current one → 9 rows

# The data file lives inside this package, so the Dockerfile's `COPY src/ ./src/`
# ships it with no build change — the same "beside my own source" trick server.py
# uses to find static/. Read once at import: it is small, immutable, and shared by
# every client (each client has only its own *position*, below).
DATA_FILE = Path(__file__).parent / "sample_data.txt"
LINES: list[str] = DATA_FILE.read_text().splitlines()


class PmuStreamModel:
    """One client's position in the record stream.

    Playback **loops**: stepping past the last record wraps to the first and
    stepping back from the first wraps to the last, so a demo left playing never
    runs dry. `line_at` still reports None outside the file's bounds — the wrap
    happens when the position moves, not when a window is rendered, so the window
    thins out at the edges instead of pretending the file is circular mid-view.
    """

    def __init__(self, start: int = 0):
        self.index = start if 0 <= start < len(LINES) else 0

    def step_forward(self) -> None:
        self.index = (self.index + 1) % len(LINES)

    def step_back(self) -> None:
        self.index = (self.index - 1) % len(LINES)

    def line_at(self, index: int) -> str | None:
        """The record at a position, or None for positions outside the file."""
        if 0 <= index < len(LINES):
            return LINES[index]
        return None

    def visible_window(self) -> list[dict | None]:
        """The records visible around the current position.

        Each entry is `{"line_number": int, "text": str}` or None where the window
        runs off either end of the file. Line numbers are 1-based, matching how an
        editor or `wc -l` would count the source file.
        """
        window: list[dict | None] = []
        for i in range(self.index - WINDOW_RADIUS, self.index + WINDOW_RADIUS + 1):
            text = self.line_at(i)
            if text is None:
                window.append(None)
            else:
                window.append({"line_number": i + 1, "text": text})
        return window
