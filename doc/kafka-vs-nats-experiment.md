# Kafka vs NATS — a side-by-side pub/sub experiment

*Branch: `kafka-nats-experiments`.*

## What this is

The PMU streamer (`/pmu-test-streamer`) used to read `sample_data.txt` directly on
the server. This experiment inserts a **pub/sub transfer layer** and runs **Kafka
and NATS side by side** so they can be compared on two axes:

- **Raw performance** — end-to-end latency and throughput, shown live in the page.
- **Operational complexity** — how many moving parts each broker needs to stand up
  in Docker Compose and in the minikube k8s deployment. See the table at the end.

The chain is now:

```
sample_data.txt
  → producer service        (loops the file in real time, publishes each record
                             to BOTH a Kafka topic AND a NATS subject)
  → server-side consumer     (subscribes to the ONE broker the client selected)
  → WebSocket → web page     (Kafka|NATS toggle + latency/throughput readout)
```

The web page has a live **Kafka | NATS** toggle that switches which pipe the
socket retransmits from, with no reconnect.

## The pieces

| Piece | Where | Notes |
|---|---|---|
| Broker clients | `app/server-python/src/brokers/` | `producers.py`, `consumers.py`, `envelope.py`, `config.py`. Async-only (aiokafka + nats-py) so consuming adds no thread seam to the single-loop server. |
| Producer | `app/server-python/src/producer.py` | Separate deployable, **reuses the server image** (`command: python producer.py`). Reads the sample by path, publishes to both brokers at `PRODUCER_RATE_HZ` (default 100 = real time). |
| Consumer + UI | `app/server-python/src/pmu_test_streamer/` + `app/client-web/src/pages/pmu-test-streamer/` | The streamer, evolved in place from file-reader to broker-consumer. |
| Compose | `docker-compose.yml` | `kafka`, `nats`, `producer` services + broker env on `server`. |
| k8s | `k8s/p-swamp-local.yaml` | Deployments/Services for `nats`, `kafka`, `producer` + broker env on `p-swamp`. Ephemeral (emptyDir), nothing persisted. |

**The envelope** (`brokers/envelope.py`) is `{seq, produced_at_ms, text}`.
`produced_at_ms` is the producer's wall clock; the consumer's
`now_ms - produced_at_ms` is the end-to-end latency. That is only valid because
producer and consumer share a host clock — true in Compose (same daemon) and in
the single-node minikube deployment.

## Running it

### Docker Compose (local dev)

```
./scripts/start-local-hotloaded-pswamp-server.sh      # brings up kafka + nats + producer + server
./scripts/start-local-hotloaded-pswamp-web-client.sh  # the web client with HMR
```

Open `http://localhost:5173/pmu-test-streamer`. Records stream immediately (the
default pipe is NATS, which connects instantly). Toggle to Kafka and watch the
latency/throughput readout change. Play/Stop pauses and resumes forwarding.

To stress the brokers, raise the producer rate (uncomment `PRODUCER_RATE_HZ` on
the `producer` service in `docker-compose.yml`, e.g. `"1000"`).

### Watching the stream in the console

Both ends log the stream as it flows, so you can watch records move through each
broker in `docker compose logs` (or the aggregated `docker compose up` output):

```
producer-1  | [KAFKA] message 1340 published
producer-1  | [NATS]  message 1340 published
server-1    | [NATS]  message 1340 received (client 2714457456)
```

Sampled to ~5 records/s per broker by default (`STREAM_TRACE_EVERY=20`) so the log
stays readable. Set `STREAM_TRACE_EVERY=1` (on the `producer` and/or `server`
service env) to see every message, or `STREAM_TRACE=0` to silence the per-message
lines.

### minikube (the real deploy artifact)

```
./scripts/start-pswamp-in-local-minikube-cluster.sh
```

It builds the image into minikube, applies all manifests (server, producer, kafka,
nats), waits for them, and opens the web client on NodePort 30080 (with the
port-forward fallback on macOS/Windows — see AGENTS.md).

## Robustness notes

- **The server boots with brokers down.** Consumers connect lazily; a down pipe
  shows as an error banner on the page rather than a hang, and `/healthz` stays a
  pure process-liveness probe (no broker dependency). This is a deliberate,
  experiment-scoped relaxation of the "server has no external dependencies"
  invariant in AGENTS.md.
- **Everything is per client and live-tail.** Each client gets its own consumer
  (Kafka: a unique group; NATS: a plain subscription), started on first socket and
  stopped on last disconnect. Switching brokers or reconnecting resumes at "now" —
  correct for a live replay.
- **No CI coverage of the broker path.** CI's e2e smoke test runs the bare image
  with `docker run` (no brokers) and drives only the reference-subapp, so the
  Kafka/NATS path is exercised locally, not in CI. Acceptable for an experiment
  branch.

## Ops-complexity observations (the deliverable)

The numbers below are the *configuration surface*, not a benchmark — fill in the
performance columns from the live readout on your own hardware.

| | NATS | Kafka |
|---|---|---|
| Compose service config | 1 line (`-m 8222`) | ~12 env vars (KRaft listeners, quorum, replication factors) |
| k8s env vars | 0 | 11 |
| Extra moving parts | none (single process) | combined broker+controller (KRaft); still one process, but a controller quorum, listener map, and offsets/txn topics to configure |
| Image | `nats:2.10-alpine`, tiny | `apache/kafka:3.9.0`, large |
| First-connect time | ~instant | seconds (metadata + group coordination) |
| Host exposure | trivial (`4222`) | needs a second advertised listener; not exposed here |
| End-to-end latency (median) | *measure* | *measure* |
| Throughput ceiling | *measure* | *measure* |

The one-flag NATS setup against Kafka's listener/quorum/replication block is the
headline: for this workload — a single-node local broker carrying one topic — NATS
is dramatically less to stand up and operate. Kafka earns its complexity with
durability, partitioning and consumer-group semantics this experiment does not
use.
