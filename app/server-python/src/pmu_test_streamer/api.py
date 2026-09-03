"""The PMU streamer's backend: consume one broker, retransmit over the socket.

This is the right end of the Kafka-vs-NATS experiment. It used to read
sample_data.txt directly; now the data arrives over a live pub/sub topic (a
separate producer service loops the file into BOTH a Kafka topic and a NATS
subject), and this app consumes from whichever pipe the client currently selected
and pushes the records — plus live latency/throughput metrics — down the socket.

The wiring, per client:

  - `states[client_id]`  — the StreamModel (selected broker, play flag, scrolling
     window, metrics). Outlives a disconnect, so a reconnect resumes the same
     broker choice. This is view state only.
  - `consumers[client_id]` — a ConsumerRunner: the asyncio task actually reading
     the broker. Exists only while the client has at least one live socket; started
     on first connect, stopped on last disconnect, so an idle client runs no broker
     connection (a pipeline is cheap here, but an orphaned consumer is still waste).
  - `sockets`            — this client's live WebSockets, for pushing.

Commands come up over REST (select a broker, play/pause), state goes down the
socket; the reasoning is in AGENTS.md and doc/the-client-server-api.md. server.py
mounts this `router` under /api/pmu-test-streamer.

Concurrency: aiokafka and nats-py are asyncio-native, so the consume tasks are
cooperatively scheduled on the one event loop like everything else here — no new
thread seam (contrast the desktop package's blocking kafka-python). Nothing
connects to a broker at import; that only happens when a ConsumerRunner starts.
"""

import asyncio
import contextlib
import itertools
import os
import time
from typing import Literal

from brokers import config
from brokers.consumers import KafkaConsumerClient, NatsConsumerClient
from fastapi import APIRouter, FastAPI, WebSocket
from pydantic import BaseModel, Field
from shared import (
    ClientId,
    CommandAck,
    SocketRegistry,
    get_logger,
    read_client_id,
    send_state,
    wait_for_disconnect,
)

from .model import Broker, StreamModel, now_ms

logger = get_logger("pmu")

# How the consume loop paces itself. POLL_TIMEOUT bounds each broker read so the
# loop regains control to notice a switch, refresh throughput and detect silence;
# PUSH_INTERVAL throttles socket pushes to ~10 Hz (the raw stream is ~100/s, and a
# 100 Hz push would run React far more often than the DOM changes — the same
# reasoning as "keep the sample path off React" in pswamp_web/); RETRY_SECONDS
# spaces reconnects while a broker is unavailable. START_TIMEOUT bounds the
# connect: aiokafka's start() otherwise blocks on its ~40 s request timeout when
# the broker is down, so without this a page selecting a dead pipe would sit
# blank for most of a minute instead of showing "pipe unavailable".
POLL_TIMEOUT = 0.1
PUSH_INTERVAL = 0.1
RETRY_SECONDS = 2.0
START_TIMEOUT = 5.0

# Makes each Kafka consumer its own group, so every client (and every re-subscribe
# after a switch) live-tails independently rather than sharing partitions. The
# per-process nonce is what makes the group unique across server *restarts* too:
# without it a restarted server would reuse "pmu-web-<client>-0" and could resume a
# group offset left in Kafka by an earlier run, draining the retained backlog
# instead of tailing.
_PROCESS_NONCE = os.urandom(4).hex()
_group_counter = itertools.count()


# --- authoritative in-memory state ------------------------------------------

states: dict[str, StreamModel] = {}


def get_state(client_id: str) -> StreamModel:
    """The single place per-client view state is born; called on connect and on
    every command, so a command can never hit a missing client."""
    model = states.get(client_id)
    if model is None:
        model = states[client_id] = StreamModel()
    return model


sockets = SocketRegistry()


# --- wire message -----------------------------------------------------------


class PmuRecord(BaseModel):
    """One record in the scrolling window."""

    seq: int = Field(description="Producer sequence number; gaps reveal drops.")
    text: str = Field(description="The raw PMU record, verbatim from the sample.")


class MetricStats(BaseModel):
    """One metric summarised four ways over the current pipe: the live reading plus
    the min, max and mean since the last broker switch."""

    current: float = Field(description="Live reading (latest smoothed / trailing-second value).")
    min: float = Field(description="Smallest value seen since the last broker switch.")
    max: float = Field(description="Largest value seen since the last broker switch.")
    mean: float = Field(description="Mean value since the last broker switch.")


class BrokerMetrics(BaseModel):
    """Live comparison numbers for the active pipe."""

    latency_ms: MetricStats = Field(description="End-to-end latency, ms.")
    throughput_hz: MetricStats = Field(description="Throughput, records/s.")
    received: int = Field(description="Records seen since the last broker switch.")


class PmuStreamState(BaseModel):
    """The single message shape pushed on connect and every change.

    A declared model, not a loose dict, because this IS the downstream half of the
    published contract (collected via this package's WS_MESSAGE export); a bare
    dict would drop the app out of the contract the web client generates types
    from.
    """

    type: Literal["state"] = "state"
    broker: Broker = Field(description="Which pipe is being retransmitted.")
    playing: bool = Field(description="Whether records are being forwarded.")
    window: list[PmuRecord] = Field(description="Most-recent records, oldest first.")
    metrics: BrokerMetrics
    error: str | None = Field(
        default=None,
        description="Why the selected pipe is unavailable, or null when healthy.",
    )


def _stats_wire(stats, digits: int) -> MetricStats:
    """A domain `Stats` rounded onto the wire model at a fixed precision."""
    return MetricStats(
        current=round(stats.current, digits),
        min=round(stats.min, digits),
        max=round(stats.max, digits),
        mean=round(stats.mean, digits),
    )


def state_message(model: StreamModel) -> PmuStreamState:
    """Build the downstream message from a client's model."""
    return PmuStreamState(
        broker=model.broker,
        playing=model.playing,
        window=[PmuRecord(seq=r.seq, text=r.text) for r in model.window],
        metrics=BrokerMetrics(
            latency_ms=_stats_wire(model.metrics.latency_stats(), 1),
            throughput_hz=_stats_wire(model.metrics.throughput_stats(time.monotonic()), 1),
            received=model.metrics.received,
        ),
        error=model.error,
    )


# --- per-client consumer task -----------------------------------------------


def _make_consumer(broker: Broker, client_id: str):
    """One broker client, with the same poll interface either way."""
    if broker == "kafka":
        group = f"pmu-web-{client_id}-{_PROCESS_NONCE}-{next(_group_counter)}"
        return KafkaConsumerClient(group_id=group)
    return NatsConsumerClient()


class ConsumerRunner:
    """The asyncio task that reads one client's selected broker and pushes state.

    A single long-lived task per client. A broker switch does not replace the task;
    it sets `_switch`, which breaks the inner consume loop so the outer loop tears
    down the current broker client and subscribes to the newly selected one.
    """

    def __init__(self, client_id: str, model: StreamModel) -> None:
        self.client_id = client_id
        self.model = model
        self._switch = asyncio.Event()
        self._stopped = False
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    def request_switch(self) -> None:
        """Ask the loop to drop the current pipe and pick up model.broker."""
        self._switch.set()

    async def stop(self) -> None:
        self._stopped = True
        self._switch.set()  # break any in-progress poll wait
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _push(self) -> None:
        await sockets.send_to_client(self.client_id, state_message(self.model))

    async def _run(self) -> None:
        while not self._stopped:
            broker = self.model.broker
            self._switch.clear()
            consumer = _make_consumer(broker, self.client_id)
            try:
                await asyncio.wait_for(consumer.start(), timeout=START_TIMEOUT)
                self.model.error = None
                await self._push()
                await self._consume(consumer)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # broker down or a mid-run drop
                logger.warning("client %s: %s pipe error (%s)", self.client_id, broker, exc)
                self.model.error = f"{broker} pipe unavailable"
                with contextlib.suppress(Exception):
                    await self._push()
                # Wait, but wake immediately if the client switches brokers.
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._switch.wait(), timeout=RETRY_SECONDS)
            finally:
                with contextlib.suppress(Exception):
                    await consumer.stop()

    async def _consume(self, consumer) -> None:
        """Read the broker until the client switches away or the task is stopped.

        Always drains the broker (so a paused Kafka consumer does not fall behind),
        but only ingests into the window/metrics while playing. Pushes at ~10 Hz
        regardless, so the page reflects the paused state and a decaying throughput.
        """
        last_push = 0.0
        while not self._stopped and not self._switch.is_set():
            envelopes = await consumer.poll(POLL_TIMEOUT)
            now_mono = time.monotonic()
            if self.model.playing:
                stamp = now_ms()
                broker_tag = self.model.broker.upper()
                for envelope in envelopes:
                    self.model.ingest(envelope, stamp, now_mono)
                    if config.should_trace(envelope.seq):
                        logger.info(
                            "[%s] message %d received (client %s)",
                            broker_tag,
                            envelope.seq,
                            self.client_id,
                        )
            if now_mono - last_push >= PUSH_INTERVAL:
                await self._push()
                last_push = now_mono


consumers: dict[str, ConsumerRunner] = {}


def _ensure_consumer(client_id: str, model: StreamModel) -> ConsumerRunner:
    """Start a consumer for this client if none is running yet."""
    runner = consumers.get(client_id)
    if runner is None:
        runner = consumers[client_id] = ConsumerRunner(client_id, model)
        runner.start()
        logger.info("client %s: consumer started (%s)", client_id, model.broker)
    return runner


async def _release_consumer(client_id: str) -> None:
    """Stop this client's consumer once its last socket has closed."""
    if sockets.of(client_id):
        return  # still has a live socket
    runner = consumers.pop(client_id, None)
    if runner is not None:
        await runner.stop()
        logger.info("client %s: consumer stopped (idle)", client_id)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """This app's slice of the process lifespan: nothing to start (consumers are
    per-client, born on connect), but on shutdown stop any that are still running
    so their broker connections drain cleanly. server.py composes this with the
    other packages' lifespans."""
    try:
        yield
    finally:
        for runner in list(consumers.values()):
            with contextlib.suppress(Exception):
                await runner.stop()
        consumers.clear()


# --- REST commands ----------------------------------------------------------

router = APIRouter()


async def applied(client_id: str, action: str) -> CommandAck:
    """Log the command, push this client its new state, acknowledge the request."""
    logger.info("client %s: %s", client_id, action)
    await sockets.send_to_client(client_id, state_message(get_state(client_id)))
    return CommandAck(applied=action)


class BrokerSelection(BaseModel):
    """Body of POST /broker/select."""

    broker: Broker = Field(description="Which pipe to retransmit from.")


@router.post("/broker/select", operation_id="pmu_test_streamer_select_broker")
async def select_broker(client_id: ClientId, body: BrokerSelection) -> CommandAck:
    """Switch which of the two live pipes this client retransmits from.

    Changes the selection and, if a consumer is running, asks it to drop the
    current pipe and pick up the new one. Metrics reset so the readout reflects the
    newly selected broker, not a blend.
    """
    model = get_state(client_id)
    if body.broker != model.broker:
        model.switch(body.broker)
        runner = consumers.get(client_id)
        if runner is not None:
            runner.request_switch()
    return await applied(client_id, f"select-broker:{body.broker}")


@router.post("/playback/play", operation_id="pmu_test_streamer_play")
async def play(client_id: ClientId) -> CommandAck:
    """Resume forwarding records to this client."""
    get_state(client_id).playing = True
    return await applied(client_id, "play")


@router.post("/playback/stop", operation_id="pmu_test_streamer_stop")
async def stop(client_id: ClientId) -> CommandAck:
    """Pause forwarding. The stream keeps flowing in the broker; we stop
    retransmitting it, so resuming is instant."""
    get_state(client_id).playing = False
    return await applied(client_id, "stop")


# --- websocket endpoint (downstream only) -----------------------------------


@router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    # The client identifies itself with a numeric seed in the URL
    # (?client_id=<seed>); reject a connection without a valid one. read_client_id
    # applies the very rule the ClientId query parameter enforces, so a page's
    # socket and its commands can never address different state.
    client_id = read_client_id(ws)
    if client_id is None:
        await ws.close(code=1008)  # policy violation
        return

    known = client_id in states
    async with sockets.connected(ws, client_id):
        model = get_state(client_id)
        _ensure_consumer(client_id, model)  # start on first socket, shared after
        logger.info(
            "client %s: %s (%s)",
            client_id,
            "reconnected" if known else "connected",
            model.broker,
        )
        # Opening snapshot straight down this socket (not via the registry): it is
        # for the connection that just arrived. The consume task pushes subsequent
        # updates to every socket this client holds.
        await send_state(ws, state_message(model))
        # Nothing is sent up this socket; this notices the client going away.
        await wait_for_disconnect(ws)
    # Outside the block, so the socket is already out of the registry: stop the
    # consumer only if this was the client's last socket.
    await _release_consumer(client_id)
    logger.info("client %s: disconnected", client_id)
