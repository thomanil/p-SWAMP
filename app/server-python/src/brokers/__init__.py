"""Kafka-vs-NATS experiment: the pub/sub transfer layer, shared by both ends.

This package is the seam that turns the PMU streamer from a file-reader into a
consumer of a live message topic. It is imported by two entrypoints:

  - `producer.py`      — reads sample_data.txt and publishes each record to BOTH
                         a Kafka topic and a NATS topic, in real time.
  - `pmu_test_streamer` — consumes from the ONE broker a client currently selected
                         and retransmits over its WebSocket.

Why it exists at all: the point of the experiment is to evaluate Kafka and NATS
side by side — raw performance (end-to-end latency, throughput) and operational
complexity (how many moving parts each needs). Keeping identical producer and
consumer code for both brokers is what makes that a fair comparison, so the two
client shapes live here together.

Two rules this package keeps, both load-bearing:

  - **Async only.** The state server runs on one asyncio event loop with no locks
    (see AGENTS.md). Both clients are asyncio-native — aiokafka and nats-py — so
    consuming adds NO new thread seam, unlike the desktop package's blocking
    kafka-python in src/pswamp/streaming/kafka_io.py.
  - **No I/O at import.** Nothing here connects to a broker when imported; a
    connection happens only when a producer/consumer is started. That is what
    keeps `import server` (the Dockerfile build check and error_check.sh's contract
    step) working with no brokers running.

`__init__` deliberately re-exports nothing heavy: import `brokers.producers` or
`brokers.consumers` for the client you need, so the producer never drags in the
consumer's machinery and vice versa.
"""
