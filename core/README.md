# pswamp-core

The shared data architecture of p-SWAMP: the pieces that carry PMU data from a
provider, through analysis modules, to a host such as the web server. A small
Python package whose only default dependency is pydantic.

`doc/server-data-architecture.md` at the repo root explains how the pieces fit.

The web backend (`app/server-python/`) consumes this package as an editable
path dependency. The tests under `core/tests/` run in that backend's
environment: `./scripts/run-python-server-tests.sh`.
