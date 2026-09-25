# core/examples

Runnable examples that sit beside the `pswamp_core` package rather than in it:
they are services and tools, not library code, and nothing that installs
`pswamp-core` gets them. What they need beyond the core is the `examples`
dependency group in `../pyproject.toml` (FastAPI, uvicorn); the web backend's
environment in `app/server-python/` has both, which is where they are run.
Put this directory on `PYTHONPATH` to import them.

## The remote data contract

The contract itself is `doc/remote-data-integration-contract.md`, written in
HTTP terms alone, since the service a deployment runs may be built on any stack.

- `remote_data_stub/` is the reference service: coverage and range queries over
  a three-second sample (`sample_frames.ndjson`, sixty `pmu.frame` lines) tiled
  to a minute, with each query's answer streamed back as NDJSON. The Time Series
  Explorer queries it in compose and the local k8s manifest.
- `check_remote_data_service.py` checks a running service against the contract
  over plain HTTP. It uses the Python standard library and nothing from p-SWAMP.

From the repo root:

```bash
# run the stub on :8100
PYTHONPATH=core/examples uv run --project app/server-python python -m remote_data_stub

# check a service against the contract (no URL: start the stub, check it, stop it)
./scripts/check-remote-data-service.sh [URL]
```

Their tests are `core/tests/test_remote_data_stub.py` and
`core/tests/test_remote_data_service.py`, run by
`./scripts/run-python-server-tests.sh`.
