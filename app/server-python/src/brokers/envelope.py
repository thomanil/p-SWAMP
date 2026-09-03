"""The one message shape carried on both topics.

A record is wrapped in a tiny JSON envelope so the consumer can measure the
transfer: `produced_at_ms` is the producer's wall clock at publish time, and the
consumer's `now_ms - produced_at_ms` is the end-to-end latency through that broker.
That comparison is only valid because producer and consumer share a host clock —
true in Docker Compose (same daemon) and in the single-node minikube deployment.

`seq` is a monotonically increasing counter from the producer, so a consumer can
see gaps (drops) independently of the original file's line numbers.

`text` is the raw PMU record, verbatim from sample_data.txt. Nothing here parses
it — the streamer only displays it — so the sample file can change with no code
change, exactly as before the pub/sub layer existed.
"""

import json
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Envelope:
    seq: int
    produced_at_ms: int
    text: str


def encode(envelope: Envelope) -> bytes:
    """Serialise an envelope to the bytes published on a topic."""
    return json.dumps(
        {
            "seq": envelope.seq,
            "produced_at_ms": envelope.produced_at_ms,
            "text": envelope.text,
        }
    ).encode("utf-8")


def decode(raw: bytes) -> Envelope:
    """Parse the bytes read from a topic back into an envelope."""
    data = json.loads(raw)
    return Envelope(
        seq=int(data["seq"]),
        produced_at_ms=int(data["produced_at_ms"]),
        text=str(data["text"]),
    )
