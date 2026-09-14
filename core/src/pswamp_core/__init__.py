# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""pswamp-core: the shared data architecture of p-SWAMP.

Layered bottom-up, each layer importing only the ones below it:

    messages     every message is a versioned pydantic ``DataModel``; topic = class name
    datagateway  providers (``DataClient``), the stitched time-addressed stream
                 (``DataGateway``), and the ``Player`` that paces it and takes commands
    bus          in-process publish/subscribe typed on message classes
    modules      "consume one model, produce another" as a coroutine
    pipeline     one stream's player + bus + modules, and the per-key registry

See doc/server-data-architecture.md at the repo root for the whole picture.
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
