# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""pswamp-core: the shared data architecture of p-SWAMP.

Layered bottom-up, each layer importing only the ones below it:

    messages     every message is a versioned pydantic ``DataModel``; topic = class name
    datagateway  providers (``DataClient``), the stitched time-addressed stream
                 (``DataGateway``), and the ``Player`` that paces it and takes commands
    bus          in-process publish/subscribe typed on message classes
    command_routing  typed commands to their receiver: routed by class, checked
                 at dispatch, applied in order by one inbox per receiver
    modules      "consume one model, produce another" as a coroutine; may answer commands
    pipeline     one stream's player + bus + modules, the per-key registry, and
                 ``dispatch``, the one way a command enters
    transport    keyed publish/subscribe between processes (a broker), for a module
                 that runs as its own service; ``InMemoryTransport`` for tests
    remote       the two halves of a module elsewhere: ``RemoteModule`` in the
                 pipeline's module list, ``ModuleHost`` in the worker process

See doc/server-data-architecture.md at the repo root for the whole picture.
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
