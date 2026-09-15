# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""Logging for the core: one stdout logger per name, over stdlib ``logging``.

The test_pswamp draft used loguru. The core uses the standard library so that
it adds no dependency and behaves like the rest of the repo (``pswamp_web/log.py``
is the model for this file). ``logging.getLogger`` alone attaches no handler and
uvicorn configures only its own loggers, so a bare logger here would go nowhere;
this one owns its handler, writes to stdout (which Docker/k8s capture), does not
propagate, and is idempotent so a reload cannot stack handlers.
"""

import logging
import sys

__all__ = ["get_logger"]


def get_logger(name: str) -> logging.Logger:
    """A configured stdout logger for ``name``; safe to call repeatedly."""
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
