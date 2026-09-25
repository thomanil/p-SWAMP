# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""``python -m remote_data_stub``: the stub as a process, configured from the environment.

    REMOTE_DATA_STUB_REPEAT   20                       how many times to tile the sample (60 s)
    REMOTE_DATA_STUB_PATH     sample_frames.ndjson     another file of pmu.frame lines, in time order
    REMOTE_DATA_STUB_PORT     8100

The client reaches it at ``REMOTE_DATA_URL``; compose and k8s point that at
this process's port. ``core/examples`` must be on ``PYTHONPATH`` (it is not
part of the installed ``pswamp_core`` package): compose and k8s set it, and
``scripts/check-remote-data-service.sh`` shows the local form.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

import uvicorn

from .app import create_app
from .recording import DEFAULT_PATH, TiledRecording
from .service import QueryService

PREFIX = "REMOTE_DATA_STUB_"


@dataclass(frozen=True)
class Settings:
    repeat: int = 20
    path: Path = DEFAULT_PATH
    port: int = 8100

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            repeat=int(os.environ.get(PREFIX + "REPEAT", "20") or 20),
            path=Path(os.environ.get(PREFIX + "PATH", "").strip() or DEFAULT_PATH),
            port=int(os.environ.get(PREFIX + "PORT", "8100") or 8100),
        )


def main() -> int:
    settings = Settings.from_env()
    recording = TiledRecording.load(settings.path, settings.repeat)
    app = create_app(QueryService(recording))
    start, end = recording.coverage("pmu.frame")
    print(
        f"remote data stub: {len(recording.frames)} frames "
        f"[{start.isoformat()}, {end.isoformat()}) from {settings.path} x{settings.repeat}; "
        f"port {settings.port}",
        file=sys.stderr,
    )
    uvicorn.run(app, host="0.0.0.0", port=settings.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
