# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""``python -m time_series_stub``: the stub as a process, configured from the environment.

    TIME_SERIES_STUB_BOOTSTRAP_SERVERS   kafka:9092          (required) brokers of the results topic
    TIME_SERIES_STUB_TOPIC               time.series.result  the results topic (the default)
    TIME_SERIES_STUB_REPEAT              20                  how many times to tile the sample (60 s)
    TIME_SERIES_STUB_PATH                <the sample file>   another recording in the same line format
    TIME_SERIES_STUB_PORT                8100

The topic and brokers must match the client's ``{NAME}_TOPIC`` and
``{NAME}_BOOTSTRAP_SERVERS``; compose and k8s set both from the same values.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

import uvicorn

from pmu_test_streamer.sample_client import DEFAULT_PATH
from pswamp_core.messages import TimeSeriesResult

from .app import create_app
from .kafka_sink import KafkaSink
from .recording import TiledRecording
from .service import QueryService

PREFIX = "TIME_SERIES_STUB_"


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: list[str]
    topic: str = TimeSeriesResult.topic
    repeat: int = 20
    path: Path = DEFAULT_PATH
    port: int = 8100

    @classmethod
    def from_env(cls) -> Settings:
        servers = os.environ.get(PREFIX + "BOOTSTRAP_SERVERS", "").strip()
        if not servers:
            raise SystemExit(
                f"{PREFIX}BOOTSTRAP_SERVERS is unset: the stub has nowhere to publish results.\n"
                "Set it to the brokers the client reads, e.g. kafka:9092"
            )
        return cls(
            bootstrap_servers=[s.strip() for s in servers.split(",") if s.strip()],
            topic=os.environ.get(PREFIX + "TOPIC", "").strip() or TimeSeriesResult.topic,
            repeat=int(os.environ.get(PREFIX + "REPEAT", "20") or 20),
            path=Path(os.environ.get(PREFIX + "PATH", "").strip() or DEFAULT_PATH),
            port=int(os.environ.get(PREFIX + "PORT", "8100") or 8100),
        )


def main() -> int:
    settings = Settings.from_env()
    recording = TiledRecording.load(settings.path, settings.repeat)
    sink = KafkaSink(settings.bootstrap_servers, settings.topic)
    service = QueryService(recording, sink)
    app = create_app(service, on_startup=sink.open, on_shutdown=sink.close)
    start, end = recording.coverage("pmu.frame")
    print(
        f"time-series stub: {len(recording.frames)} frames "
        f"[{start.isoformat()}, {end.isoformat()}) from {settings.path} x{settings.repeat}; "
        f"results on {settings.topic} at {settings.bootstrap_servers}; port {settings.port}",
        file=sys.stderr,
    )
    uvicorn.run(app, host="0.0.0.0", port=settings.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
