# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Voltage phasors, one per station.

The web counterpart of p-SWAMP's ``visualization.voltage_phasor_plot`` and the
``PhasorPlotFancy`` family.

Reads the same measurement window ``/time-window`` does -- the connecting
client's own, one per pipeline -- on its own ticker, and adds no application of
its own, which is the point of having one measurement store per pipeline rather
than a buffer per page.

Only the most recent row is sent, so this is a snapshot rather than a stream:
44 stations at 10 Hz is a few kilobytes a second with no history to maintain.
"""

import asyncio
import contextlib
import functools

import numpy as np
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..hub import Hub, connected_hub
from ..replay import load_recording
from ..wire import Phasor, PhasorSnapshot, sample, send_state

# Half the time window page's rate. Unlike a scrolling trace, where the eye
# follows motion, a dial of 44 arrows reads the same at 5 Hz as at 10 -- and each
# message is a full snapshot of every station, so the rate is what the cost is
# proportional to. Most of those bytes are key names and station labels rather
# than digits, so shortening the numbers barely helps; sending fewer snapshots
# does.
PUSH_HZ = 5

router = APIRouter()


@functools.lru_cache(maxsize=1)
def _voltage_columns() -> tuple:
    """(station, channel, magnitude column, angle column) per station.

    The decoder emits a station's magnitude and angle as two separate columns, so
    they have to be paired back up by station and channel name.

    Read from the *recording* rather than from any one pipeline's window. Every
    pipeline replays the same file through the same decoder with no channel
    selection, so the measurement store's header is this header — but taking it
    from a live Hub would cache one client's object and keep it alive after that
    client's pipeline had been evicted.
    """
    header = load_recording().header
    stations = np.asarray(header["station"])
    channels = np.asarray(header["channel"])
    measurements = np.asarray(header["measurement"])

    magnitudes = {
        (str(stations[i]), str(channels[i]).removesuffix("_Magnitude")): i
        for i in np.where(measurements == "v_Magnitude")[0]
    }
    angles = {
        (str(stations[i]), str(channels[i]).removesuffix("_Angle")): i
        for i in np.where(measurements == "v_Angle")[0]
    }
    return tuple(
        (station, channel, mag_col, angles[(station, channel)])
        for (station, channel), mag_col in magnitudes.items()
        if (station, channel) in angles
    )


def snapshot_message(hub: Hub) -> PhasorSnapshot:
    columns = _voltage_columns()
    indices = [col for entry in columns for col in (entry[2], entry[3])]
    # get_safe rather than snapshot: a snapshot is only a phasor set, so the
    # append count the time-window page needs is of no use here.
    times, data = hub.store_app.tw.get_safe(np.asarray(indices))

    latest = data[-1]
    magnitudes = latest[0::2]
    angles = latest[1::2]

    islands = hub.islands.station_to_island

    finite = magnitudes[np.isfinite(magnitudes)]
    mag_ref = float(np.max(finite)) if finite.size else None

    # Circular mean, not a plain average: angles wrap at +/-pi, and averaging
    # them numerically puts the reference in the wrong place as soon as the
    # system straddles the discontinuity.
    finite_angles = angles[np.isfinite(angles)]
    ang_ref = (
        float(
            np.arctan2(np.mean(np.sin(finite_angles)), np.mean(np.cos(finite_angles)))
        )
        if finite_angles.size
        else None
    )

    return PhasorSnapshot(
        t=float(times[-1]) if np.isfinite(times[-1]) else 0.0,
        phasors=[
            Phasor(
                station=station,
                channel=channel,
                # Volts to the nearest tenth and radians to six decimals are
                # both far finer than a PMU resolves, let alone a dial shows.
                mag=sample(magnitude, 1),
                ang=sample(angle, 6),
                island=islands.get(station),
            )
            for (station, channel, _, _), magnitude, angle in zip(
                columns, magnitudes, angles
            )
        ],
        mag_ref=sample(mag_ref),
        ang_ref=sample(ang_ref),
    )


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    async with connected_hub(ws) as hub:
        if hub is None:
            return

        async def push() -> None:
            interval = 1 / PUSH_HZ
            while True:
                await send_state(ws, snapshot_message(hub))
                await asyncio.sleep(interval)

        pusher = asyncio.create_task(push())
        try:
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            pusher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pusher
