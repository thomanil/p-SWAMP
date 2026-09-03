"""A stdout logger for the broker layer, independent of the rest of the backend.

A near-copy of `pswamp_web/log.py` on purpose: the producer runs as its own
process (`python producer.py`) and must not drag in `pswamp_web` (and through it
FastAPI) just to log a line. Keeping `brokers` free of that import is what lets the
producer image start lean. The consumer side imports this too, so both ends of the
experiment log the same way.
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """A logger writing to stdout (captured verbatim by Docker/k8s).

    Owns its handler, does not propagate, and is idempotent — the same reasoning
    as `pswamp_web/log.py`: consistent output however the process was launched, and
    no double-printing on a reload.
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
