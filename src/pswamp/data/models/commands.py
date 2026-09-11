# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the p-SWAMP Project.

"""The reply to a command that starts a job.

STEP3 §4.4 / §8.4. Deliberately the same shape as the web layer's ``CommandAck``
(``status``, ``applied``) plus the two ids a request/response exchange needs, so
a job's POST is still "commands up, state down": the ack carries no result. The
report itself arrives on a socket later, wearing the same ``request_id``.

``CommandAck`` itself still lives in ``pswamp_web/wire.py`` for this slice; folding
it in here is the ``models/commands.py`` move STEP3 §4.4 describes.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

__all__ = ["JobAck"]


class JobAck(BaseModel):
    """Acknowledgement of a batch command. Never carries the result."""

    status: Literal["ok"] = "ok"
    applied: str = Field(description="Which operation was started.")
    job_id: str = Field(description="Server-side id of the running job.")
    request_id: str = Field(
        description=(
            "Correlation id the resulting Report will carry: the caller's own if "
            "it supplied one, otherwise the job_id."
        ),
    )
