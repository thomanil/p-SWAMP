"""The web backend's data layer: the process gateway and the providers plugged in.

Not an app package -- no ``router``, never in ``APPS``. It is a *service*
package listed in ``server.SERVICES``: its ``lifespan`` builds the process's one
``DataGateway`` before any app handles a request, and the app packages that read
PMU data (``pmu_test_streamer``, ``pmu_report``) reach it through
:func:`gateway`. See ``service.py`` for configuration and ``sample_file.py`` for
the provider that ships in the repo.
"""

from .sample_file import DEFAULT_FILE, RECORDING_EPOCH, STREAM_ID, SampleFileClient, parse_records
from .service import (
    MODELS,
    REGISTRY,
    coverage_of,
    default_clients,
    gateway,
    lifespan,
    stream_header,
)

__all__ = [
    "DEFAULT_FILE",
    "MODELS",
    "RECORDING_EPOCH",
    "REGISTRY",
    "STREAM_ID",
    "SampleFileClient",
    "coverage_of",
    "default_clients",
    "gateway",
    "lifespan",
    "parse_records",
    "stream_header",
]
