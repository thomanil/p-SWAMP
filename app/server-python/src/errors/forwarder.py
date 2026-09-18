# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""``ErrorForwarderModule``: the bridge from one pipeline's bus to the hub.

A core ``Module`` whose input is ``ErrorEvent`` and whose ``process`` publishes
nothing back -- it hands the event to the hub with the pipeline's client id and
the app's slug, which is all the hub needs and all the bus does not know. One
instance per pipeline, appended to the module list by every app that builds
one (``build_pipeline`` in the streamer, frequency peek and the explorer).
"""

from __future__ import annotations

from pswamp_core.bus import Overflow
from pswamp_core.messages import ErrorEvent, ResultEnvelope
from pswamp_core.modules import Module

from .hub import HUB, ErrorHub

__all__ = ["ErrorForwarderModule"]


class ErrorForwarderModule(Module):
    name = "error-forwarder"
    input_model = ErrorEvent
    output_model = ResultEnvelope  # never published: process returns None
    overflow = Overflow.GROW  # an error is never dropped for being late

    def __init__(self, client_id: str, app: str, hub: ErrorHub = HUB) -> None:
        super().__init__()
        self.client_id = client_id
        self.app = app
        self.hub = hub
        self.forwarded = 0

    async def process(self, message: ErrorEvent) -> None:
        self.hub.publish(self.client_id, self.app, message)
        self.forwarded += 1
        return None
