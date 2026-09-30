# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""pswamp-core: the shared data architecture of p-SWAMP.

Data flows down, commands flow up, and the transport is between them::

    DATA DOWN    source → DataClient (the active one) → DataGateway → Player
                 → topic <app>.pmu.frame (key = pipeline) → Module → topic <app>.<result> → edge
    COMMANDS UP  edge → validate at the player (the 409) → topic <app>.<command> → Player | Module

Layered bottom-up, each layer importing only the ones below it:

    messages         every message is a versioned pydantic ``DataModel``; topic = class name
    datagateway      providers (``DataClient``), the gateway over them (named sources, one
                     active), and the ``Player`` that paces its stream and takes commands
    subscription     a consumer's queue and its overflow policy; ``Sink``, what is published into
    keep_up          falling behind, noticed and reported as an ``ErrorEvent``
    transport        keyed publish/subscribe, one topic per class per app: ``InMemoryTransport``
                     (one process) or ``KafkaTransport`` (a broker); the ``Outbox`` in front of it
    command_routing  a command's class is its address; the inbox that applies them in order
    modules          "consume one model, produce another"; may answer commands, may read the gateway
    pipeline         a family (what an app's pipelines are made of), one pipeline per key
                     (gateway + player + topics), ``dispatch``, and the per-key registry
    host             where a module runs: one instance per pipeline key, off the transport
    worker           ``python -m pswamp_core.worker``: hosts the families it is told to

See doc/server-data-architecture.md at the repo root for the whole picture.
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
