# STEP 7b — A heavy algorithm under load: N4SID mode estimation as its own service

Follow-up to STEP 7. There the module (islanding detection) cost half a
millisecond a second and the load was the frames; the limit it found was the
server's event loop. This step takes the heaviest analysis in the desktop
package -- N4SID subspace identification of the grid's electromechanical
modes -- puts it behind the same seam, and asks one question of every failure:
**is it the algorithm, or the pipeline around it?**

## Goal

Run `src/pswamp/monitoring/n4sid.py` as a core module in a separate worker
process consuming a topic, with a simple page for its output; profile the
module and its pipeline; say what fails first and why.

## Decisions

| Question | Decision |
|---|---|
| The algorithm | The desktop `N4SID` class copied verbatim into `mode_estimation/n4sid_module.py` (as islanding was), over the same `nfoursid` library. |
| `nfoursid` | A dependency of the web backend (asked 2026-09-23), rather than a vendored copy. It hard-imports matplotlib, so matplotlib, pillow, fonttools, kiwisolver, contourpy, cycler and pyparsing are now in the image, unused. |
| Input | The N44 line-trip recording through the islanding stream's provider, named by spec string (`modes_n44:islanding_stream.n44_client:N44RecordingClient`), full 700-column frames. The module uses the 44 `f` columns. |
| Parameters | The desktop `run_n4sid` defaults: 45 s window, order 10, 10 block rows, one identification per second of data; Emergency under 3 % damping of a 0.1–2 Hz mode, Alert under 7 %. |
| The variable under test | How an identification runs, `MODE_ESTIMATION_EXECUTION`: `inline` (on the event loop), `thread` (a pool, the default) or `process` (a process pool); `MODE_ESTIMATION_POOL_SIZE`; and `OPENBLAS_NUM_THREADS`. |

Resolved without asking: in `thread`/`process` mode a client has **one
identification in flight**; one that falls due while the previous is still
running is skipped, counted, and reported on the tray through a second
`KeepUpMonitor` ("the n4sid analysis cannot identify every 1 s of data: N
identifications skipped" -- `KeepUpMonitor.note` and its `unit` were added to
the core for this). A finished identification goes out with the next frame.
Each result carries `compute_ms`, `compute_cpu_ms`, `queue_ms`, `latency_ms`,
`evaluations`, `evaluations_skipped`, besides the input readings -- which is
what lets a result say where its time went.

## What exists

`app/server-python/src/mode_estimation/` (`n4sid_module.py`, `api.py`,
`worker.py`), `pages/mode-estimation/` (status badge, a table of modes with
frequency, damping and the five stations taking most part, and a keep-up
table), `mode-estimation-worker` in compose and `p-swamp-mode-estimation-worker`
in k8s (transport `modes`, prefix `mode-estimation`), `nfoursid` in
`app/server-python/pyproject.toml`, `KeepUpMonitor.note`/`unit` in the core,
`tests/test_mode_estimation.py` (the three execution modes, skip reporting,
the process pool, the worker path, the pipeline).

The identification is right: on the recording after the trip it finds
1.42 Hz at 1.8–1.9 % and 1.08 Hz at 2.1 % damping -- Emergency -- led by
stations 6500, 6700 and 6701, the ones the islanding detector separates.

## How it was measured

Compose stack as in STEP 7 (12 vCPUs in Docker Desktop's VM on an M4 Pro: 8
performance and 4 efficiency cores). A throwaway driver ran N clients through
the page's own endpoints: fresh pipelines per run (server restarted, new
client ids), a 10x warm-up until the 45 s window was full, 3 s to settle,
then a 20 s (1x) or 60 s (faster) measured step. Per step: share of due
identifications skipped, median compute and CPU time, worst queue time,
median and worst latency (due to published), worst input age, drops, and
cgroup CPU per container. The worker was reconfigured between runs with a
compose override. Algorithm-only numbers were taken inside the worker
container with the pipeline idle.

## The algorithm on its own

One identification over 45 s x 44 stations (2250 x 44), inside the worker
container (Linux arm64, OpenBLAS 0.3.31):

| Window | `OPENBLAS_NUM_THREADS=1` | OpenBLAS default (12 threads) |
|---|---|---|
| 10 s | 62 ms | 218 ms wall, 2.6 s CPU |
| 20 s | 259 ms | 777 ms wall, 8.4 s CPU |
| 45 s | **622 ms** | **852 ms wall, 10.1 s CPU** |

**97 % of it is LAPACK**: SVD 72 %, QR 25 %, on the block-Hankel matrices of
the window (cProfile of three runs). `nfoursid`'s own Python and pandas are
noise, so the GIL is not in the way: numpy releases it inside LAPACK.

Several at once, 45 s windows, 24 identifications:

| Concurrent | Threads, BLAS=1 | Processes, BLAS=1 | Threads, BLAS default |
|---|---|---|---|
| 1 | 1.6/s (628 ms each) | 1.6/s (624 ms) | 0.8/s (1086 ms) |
| 2 | 3.0/s (670 ms) | 3.1/s (640 ms) | 0.5/s (4.4 s) |
| 4 | 5.3/s (726 ms) | 5.3/s (754 ms) | 0.2/s (18 s) |
| 8 | 7.2/s (997 ms) | 7.9/s (986 ms) | **0.1/s (54 s)** |

Two things: **OpenBLAS's default threading is pathological here** -- a
thread per core per call, busy-waiting, so concurrent identifications fight
over the same cores and throughput *falls* as they are added. And with one
BLAS thread each, threads scale almost exactly like processes, and each
identification gets slower as more run at once (+60 % at eight) -- memory
bandwidth, and past eight the efficiency cores.

For scale: the same identification takes 210 ms on the Mac itself (numpy on
macOS uses Apple's Accelerate), three times faster than the container. The
container is what a Linux server runs; the `OPENBLAS_CORETYPE=ARMV8`
workaround on this branch makes no measurable difference (605 vs 619 ms).

## The pipeline

Full frames throughout; "skipped" is of the identifications that fell due.

| Run | Execution | Clients x speed | Skipped | Compute (CPU) | Queue max | Latency med / max | Input age max | Worker CPU |
|---|---|---|---|---|---|---|---|---|
| P1 | thread/12, BLAS default | 1 x 1 | 21 % | 799 (763) ms | 1 ms | 0.8 / 2.1 s | 0.04 s | **961 %** |
| P1 | thread/12, BLAS default | 4 x 1 | ~all (1 result in 20 s) | **5411** (4927) ms | 0 | 5.4 s | 0.00 s | 411 % |
| P2 | thread/12, BLAS=1 | 1 x 1 | 0 | 642 (642) ms | 1 ms | 0.66 / 0.70 s | 0.00 s | 67 % |
| P2 | thread/12, BLAS=1 | 4 x 1 | 0 | 844 (773) ms | 3 ms | 0.86 / 1.04 s | 0.00 s | 319 % |
| P2 | thread/12, BLAS=1 | 8 x 1 | 26–43 % | 922–1280 ms | 4 ms | 0.93–1.29 / 1.2–1.4 s | 0.02 s | 158–282 % |
| P2 | thread/12, BLAS=1 | 1 x 2 | 49 % | 681 ms | 0 | 0.69 / 0.74 s | 0.00 s | 32 % |
| P2 | thread/12, BLAS=1 | 1 x 3 | 65 % | 685 ms | 1 ms | 0.69 / 0.72 s | 0.00 s | 36 % |
| P2 | thread/12, BLAS=1 | 4 x 2 | 50 % | 805 ms | 1 ms | 0.81 / 0.93 s | 0.00 s | 128 % |
| P3 | inline, BLAS=1 | 1 x 1 | — | 639 ms | — | 0.64 / 0.68 s | **1.7 s** | 68 % |
| P3 | inline, BLAS=1 | 1 x 2 | — | 621 ms | — | 0.62 / 0.71 s | **5.0 s** | 54 % |
| P3 | inline, BLAS=1 | 4 x 1 | — (1.4 of 4 done per s) | 625 ms | — | 0.63 / 0.69 s | **18 s, growing** | 101 % |
| P4 | process/12, BLAS=1 | 8 x 1 | 9 % | 948 ms | 651 ms | 0.96 / 1.68 s | 0.01 s | 365 % |
| P4 | process/12, BLAS=1 | 4 x 2 | 49 % | 767 ms | 3 ms | 0.77 / 0.85 s | 0.00 s | 130 % |
| P5 | thread/2, BLAS=1 | 8 x 1 | 61 % | 701 ms | **2188 ms** | 2.2 / 2.9 s | 0.00 s | 131 % |

Around it, at these rates (at most 400 frames/s): the server used 5–30 % of a
core and the broker 15–30 %. Frames were never dropped on the worker side,
and the only server-side drops came from the 10x warm-up of eight clients
(4000 frames/s, over the STEP 7 ceiling), not from the measured steps.

**Where the worker's CPU goes** (thread mode, 8 clients at 1x, per-thread
CPU from `/proc` over 20 s): the event-loop thread -- consuming, decoding and
windowing 400 frames/s of 41 KB, publishing results and reports -- 19 % of a
core, **13.5 %** of the worker's CPU; the identification threads the other
**86.5 %**.

## What fails, and whose fault it is

Ranked by how early it bites:

1. **Configuration: BLAS threading (P1).** With OpenBLAS left at its default,
   a single client at real time already skips a fifth of its identifications
   while burning ten cores, and four clients stop getting results at all. The
   algorithm needs 0.62 s of one core; the default turns that into 10 s of
   CPU spread over twelve threads that then fight every other identification
   in the pool. **Infrastructure, not algorithm** -- and invisible from the
   module's code. Fixed for this worker: `OPENBLAS_NUM_THREADS=1` in compose
   and k8s, the pool providing the parallelism.
2. **Architecture: CPU work on the event loop (P3).** Run inline, as a naive
   port of the desktop app would, one identification blocks the worker's only
   loop for 0.6 s. Every client shares that loop, so with four clients at real
   time the worker completes 1.4 identifications a second on one core with
   eleven idle, and **every client's input is 18 s stale and growing** --
   including clients whose own analysis is cheap. The tray says "n4sid is not
   keeping up with pmu.frame", which blames the input for what the analysis
   did. The core's `Module` contract (`async def process`) gives no hint that
   CPU-bound work must leave the loop; the thread and process modes here are
   this module's own code.
3. **Design: one identification in flight per client (P2, P4 at 2x and 3x).**
   A client replaying faster than real time skips half (2x) or two-thirds (3x)
   of its identifications while the worker idles at a third of one core: the
   module never runs two identifications of the same client at once. At real
   time -- the only speed a live deployment runs at -- this does not bite;
   for replay it caps a client at one core no matter how many are free.
4. **Capacity: the algorithm itself (P2 at 8 clients, P4, P5).** With the
   above out of the way, the real limit is the arithmetic: 0.62 s of CPU per
   client per second at real time, rising to 0.9–1.3 s when eight run at once
   (shared memory bandwidth, efficiency cores). An identification that takes
   most of its one-second deadline misses it under load: 26–43 % skipped at
   eight clients in threads, 9 % in processes, with the worker using only
   two to four of twelve cores. A pool smaller than the demand (P5, the k8s
   configuration: 2 threads for 8 clients) turns into queueing -- 2.2 s in the
   queue, results 3 s late, 61 % skipped -- exactly the capacity sum (2
   threads / 0.7 s ≈ 2.9 identifications a second for 8 due).
5. **Not the pipeline.** The transport, the broker, the server and the
   worker's loop together cost less than a fifth of what the analysis costs at
   eight clients, input reached the module within milliseconds in every mode
   but `inline`, and nothing was dropped between the server and the module.

So: at real time the algorithm is the capacity limit (roughly 8–12 clients
per worker on this machine with the BLAS fix, fewer on the k8s limits), and
everything that failed *before* reaching it was the surrounding execution
model -- BLAS threads, the event loop, one-in-flight -- not the transport.

## Open points

1. **CPU-bound modules need a place in the core.** Today each module decides
   (or forgets) to leave the event loop. A `Module` flag or base class for
   CPU-bound analysis -- the core runs `process` in a shared executor, with
   one-in-flight and skip reporting built in -- would make the right thing the
   default and keep `inline` from ever being the naive port.
2. **Worker-level executor and BLAS policy.** One pool per worker process,
   sized to the cores it is given, with `OPENBLAS_NUM_THREADS=1` (or
   `threadpoolctl`) set by the worker rather than per deployment -- and the
   server's own image, where the grid monitor's desktop applications run
   numpy on threads too, deserves the same check.
3. **Pipelining a client's identifications** (more than one in flight, results
   reordered by `window_end`) would let a replay use free cores; not needed
   for live data.
4. **Deadlines that fit the work.** One identification per data-second over a
   45 s window recomputes 44 s of the same data; a longer interval, a shorter
   window, or an incremental subspace update would cut the cost by an order of
   magnitude. That is an analysis decision, not an infrastructure one.
5. **Scaling out.** Past one worker's cores, a second worker needs keyed
   partitions and a consumer group (STEP 7 open point 7); the module keeps a
   45 s window per key, so a key must stay on one worker.
6. **The image grew by matplotlib** for an import `nfoursid` does not use at
   runtime; a vendored copy or an upstream change making the import lazy would
   take it out again.
7. **Tray wording for the inline case**: "not keeping up with pmu.frame" is
   true but points at the input; a module whose own `process` time exceeds its
   input interval could say so directly (the monitor has the timing).
