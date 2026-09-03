# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Logging for the web backend: one stdout logger per name.

Defined here rather than in ``src/shared.py`` because the import may only run one
way -- this package moves upstream as ``pswamp.web`` and may not import from the
rest of the backend, while ``shared.py`` re-exports this inward for the app
packages beside it.

It matters more than it looks. ``logging.getLogger(...)`` on its own attaches no
handler, and uvicorn configures only its own loggers, so every message this
package logged went nowhere. That was survivable when the only line was "pipeline
started" once per process. It is not now: pipeline starts, evictions, capacity
refusals and dead application threads are the whole operational picture of the
registry, and a silent registry is one you cannot reason about.
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """A logger writing to stdout, which Docker/k8s capture verbatim.

    Owns its handler and does not propagate, so it behaves the same however the
    server was launched (``uv run src/server.py``, ``uvicorn server:app``) and
    never double-prints through uvicorn's root config. Idempotent: called twice
    with one name it returns the configured logger untouched, so a reload cannot
    stack handlers and print every line twice.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                f"%(asctime)s %(levelname)s [{name}] %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
