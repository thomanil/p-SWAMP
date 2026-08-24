# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The wire format between the p-SWAMP analysis core and a browser.

p-SWAMP's Qt front end never needed one of these: its widgets hold a reference to
the very ``TimeWindow`` object the application thread is writing into, and read it
on a repaint timer. Nothing is ever serialised, so nothing ever had to have a
schema. The one place a message does cross a process boundary -- the streaming
layer -- pickles Python objects inside a JSON envelope, which no browser can read.

So this module is where the shapes get named. Every message a page receives is
declared here, and every page sends through :func:`send_state`.

It also holds the two names every REST command endpoint in this package declares
itself with -- :data:`ClientId` and :class:`CommandAck`. Those are deliberate
twins of the ones in ``src/shared.py``: this package may not import anything from
the rest of the web backend, because it is written to move into the desktop
package as ``pswamp/web/``. Change one pair and change the other.

Two conventions worth knowing before adding a message:

**snake_case on the wire.** The client hook converts to camelCase at the point it
maps a message into its page's state, which is where the other pages in this repo
already do it. These payloads are array-heavy rather than key-heavy, so the
mapping stays small.

**NaN is null.** Not a formality: ``json.dumps`` emits a bare ``NaN`` token,
which ``JSON.parse`` rejects outright, and NaN is the *normal* case here -- a
``TimeWindow`` is all-NaN until it fills, and the PMU decoder substitutes NaN for
missing frequencies. Without this the very first message to every page would be a
parse error in the browser. ``null`` is also what a charting library wants to see
for a gap, so nothing is lost on the way in.
"""

import math
from typing import Annotated, Literal

from fastapi import Query, WebSocket
from pydantic import BaseModel

# A measurement that may be missing. NaN and infinities become null on the wire.
Sample = float | None

# Statuses an application can report. Mirrors the strings p-SWAMP's applications
# actually set on themselves; "Undefined" is the value before the first analysis.
AppStatusValue = Literal["OK", "Alert", "Emergency", "Initializing...", "Undefined"]


def sample(value, ndigits: int | None = None) -> Sample:
    """Coerce one float to something JSON can represent.

    ``ndigits`` is worth passing wherever a message carries many of these: a
    float64 repr is up to 17 characters of which only the first few mean
    anything, and on a payload that is mostly numbers the difference is roughly
    half the bytes.
    """
    if value is None:
        return None
    value = float(value)
    if not math.isfinite(value):
        return None
    return value if ndigits is None else round(value, ndigits)


def series(values, ndigits: int = 6) -> list[Sample]:
    """Coerce a 1-D float array to a JSON-safe list.

    Rounding is not cosmetic: PMU measurements carry far fewer significant
    figures than a float64 repr prints, and six decimals cuts roughly a third off
    a payload that is almost entirely digits.
    """
    out: list[Sample] = []
    for value in values:
        value = float(value)
        # value != value is the NaN test, and the cheapest one.
        out.append(
            None
            if (value != value or value in (math.inf, -math.inf))
            else round(value, ndigits)
        )
    return out


ClientId = Annotated[
    str,
    Query(
        alias="client_id",
        pattern=r"^\d{1,20}$",
        description=(
            "The browser's client id -- the same value its WebSockets send, which "
            "is what makes a command apply to the pipeline the page is watching."
        ),
    ),
]
"""The caller's identity on a command request.

A *string* here, not an int as in ``shared.py``: pipeline keys are strings, and
the pattern is the exact rule :func:`..hub.read_client_id` applies to the socket's
query parameter. The two must agree, or a page's commands would address a
different pipeline from its sockets.
"""


class CommandAck(BaseModel):
    """The reply to every command POST in this package.

    Not the resulting state: that arrives on whichever socket the page has open,
    on the server's own schedule. See the twin in shared.py.
    """

    status: Literal["ok"] = "ok"
    applied: str


async def send_state(ws: WebSocket, message: BaseModel) -> None:
    """Send a message to one client.

    Deliberately the only way a page sends. ``WebSocket.send_json`` routes through
    ``json.dumps``, which happily emits bare ``NaN`` and ``Infinity`` tokens that
    are not valid JSON and that ``JSON.parse`` refuses; pydantic's serialiser does
    not, given the field types declared below. Routing every page through here
    means a new message type cannot quietly reintroduce that bug.
    """
    await ws.send_text(message.model_dump_json())


# --- time window ------------------------------------------------------------


class Channel(BaseModel):
    """One column of a labelled time window.

    Flattened from p-SWAMP's three-row header, plus a precomputed display label so
    the client never has to join the parts itself.
    """

    idx: int  # column index within the application's window; the selection token
    station: str  # "3000"
    channel: str  # "Frequency" | "V_Magnitude" | "I[L3000-3020]_Angle"
    measurement: str  # "f" | "Df" | "v_Magnitude" | "v_Angle" | "i_..."
    label: str  # "3000 Frequency"


class TimeWindowSlice(BaseModel):
    """A slice of the client's measurement window.

    ``series`` is column-major -- one list per channel, parallel to ``channels``.
    That is the layout a streaming chart wants; row-major would force the client
    to transpose a matrix of a few hundred thousand numbers on every update.

    ``mode`` is what keeps this affordable. A full window is tens of thousands of
    numbers and re-sending it several times a second is megabytes per second per
    client; an "append" carries only the samples added since the last message.
    The server sends "full" on connect and whenever the selection changes, and
    "append" the rest of the time. ``seq`` counts messages so a client can tell
    it missed one and ask for a fresh "full".
    """

    type: Literal["state"] = "state"
    mode: Literal["full", "append"]
    seq: int
    t: list[Sample]
    series: list[list[Sample]]
    channels: list[Channel] | None = None
    n_samples: int | None = None
    sampling_rate: float | None = None


# --- phasors ----------------------------------------------------------------


class Phasor(BaseModel):
    station: str
    channel: str
    mag: Sample  # volts
    ang: Sample  # radians
    island: int | None = None  # 0 is the main system


class PhasorSnapshot(BaseModel):
    """The most recent phasor per station.

    ``mag_ref`` and ``ang_ref`` are sent rather than applied, so the client can
    offer per-unit and mean-angle-relative views as toggles -- which is what the
    Qt phasor plot does -- without the server having to guess which is wanted.
    """

    type: Literal["state"] = "state"
    t: float
    phasors: list[Phasor]
    mag_ref: Sample = None
    ang_ref: Sample = None


# --- islanding --------------------------------------------------------------


class Island(BaseModel):
    index: int  # 0 is the main system
    stations: list[str]
    mean_freq: Sample


class IslandingParameters(BaseModel):
    window_length: float
    mean_threshold: float
    eval_freq: float


class IslandingResult(BaseModel):
    type: Literal["state"] = "state"
    t: float
    app_uuid: str
    app_name: str
    status: AppStatusValue
    islands: list[Island]
    parameters: IslandingParameters


# --- line outage detection --------------------------------------------------


class LineOutageEvent(BaseModel):
    """One branch changing connection state, as the detector reports it.

    ``stations`` and ``measurements`` come straight from the time window's
    header for the channels that flipped, so a single physical line trip
    normally appears with the station at each end -- the current on that branch
    goes to zero as seen from both.
    """

    t: float
    kind: Literal["disconnect", "connect"]
    stations: list[str]
    measurements: list[str]


class LineOutageLog(BaseModel):
    """Newest first. The detector is silent unless something changes, so this is
    a log of transitions rather than a snapshot of present state."""

    type: Literal["state"] = "state"
    app_uuid: str | None = None
    app_name: str | None = None
    window_length: float | None = None
    events: list[LineOutageEvent]


# --- alarms -----------------------------------------------------------------


class AlarmEvent(BaseModel):
    t: float
    type: str  # "init" | "not_critical" | "acknowledge" | "silence" | "user_message"
    message: str


class Alarm(BaseModel):
    uuid: str
    app_uuid: str
    app_name: str
    t_start: float
    t_end: float | None = None
    status: Literal["unseen", "acknowledged", "silenced", "not_critical"]
    events: list[AlarmEvent]


class AlarmList(BaseModel):
    type: Literal["state"] = "state"
    alarms: list[Alarm]  # newest first


# --- application status -----------------------------------------------------


class AppStatus(BaseModel):
    uuid: str
    app_name: str
    status: AppStatusValue
    t: float  # data time stamp the application reported
    received_at: float  # server wall clock when it arrived
    stale: bool  # nothing heard recently; the Qt table greys these out


class AppStatusTable(BaseModel):
    type: Literal["state"] = "state"
    apps: list[AppStatus]
    server_time: float
    replay: "ReplayStatus"


class ReplayStatus(BaseModel):
    """State of the PMU source feeding every application."""

    source: str
    playing: bool
    data_rate: float
    n_samples: int
    n_channels: int
    cursor: int  # absolute row index, so it keeps counting across loops
    position: float  # seconds into the current pass
    duration: float


# --- grid model -------------------------------------------------------------


class GridBus(BaseModel):
    name: str
    v_nom: Sample = None
    area: str | None = None
    lon: Sample = None
    lat: Sample = None


class GridBranch(BaseModel):
    name: str
    from_bus: str
    to_bus: str
    kind: Literal["line", "trafo"]


class PmuSite(BaseModel):
    name: str
    lon: float
    lat: float


class GridModel(BaseModel):
    """Static topology. Served over HTTP rather than a socket: it never changes,
    it is worth caching, and putting it on the socket would make every page's
    hook handle a second message shape for no benefit."""

    buses: list[GridBus]
    branches: list[GridBranch]
    pmus: list[PmuSite]
    bbox: tuple[float, float, float, float]  # lon_min, lat_min, lon_max, lat_max
