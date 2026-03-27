# Distributed KV Store (MPI) — Mini Documentation

This document gives a practical overview of how to compile and run the project, what command-line options are supported, how components fit together, and how to add custom tests/workloads.

---

## 1) Project Overview

This repository implements a distributed key-value store over MPI processes.

- **Combined mode (`kvstore`)**: one binary where rank 0 acts as client/coordinator and ranks 1..N-1 are storage nodes.
- **Separated mode (`kvserver` + `kvclient`)**: server and client launched together with MPMD (`mpirun ... : ...`).
  - In separated mode, **server rank 0 acts as router/coordinator**.

Core operation types:

- `PUT`
- `GET`
- `DEL`
- `SHUTDOWN`

Messages are fixed-size (`Message`) and sent as raw bytes (`MPI_BYTE`, `sizeof(Message)`) to avoid custom MPI datatype padding issues.

---

## 2) Build Instructions

### Prerequisites

- MPI toolchain installed (`mpic++`, `mpirun`)
- C++17 compiler support

### Build all executables

```bash
make
```

This produces:

- `kvstore` (combined mode)
- `kvserver` (router + nodes)
- `kvclient` (standalone client)

### Clean artifacts

```bash
make clean
```

---

## 3) Command-Line / Run Options

## 3.1 Makefile targets

### Combined demo run

```bash
make run
```

Equivalent shape:

```bash
mpirun --oversubscribe -np 4 ./kvstore
```

### Separated (MPMD) run

```bash
make run-separated
```

Equivalent shape:

```bash
mpirun --oversubscribe \
  -np 4 ./kvserver --server-world-size 4 \
  : -np 1 ./kvclient --server-world-size 4
```

> `--server-world-size` tells client/server how many ranks belong to the server world (router + storage ranks).

## 3.2 `kvstore` options (combined mode)

`kvstore` supports a demo mode (default) and benchmark mode.

### `kvstore` option reference

| Option | What it does | Default / behavior |
|---|---|---|
| `--benchmark` | Switches rank 0 from demo CRUD sequence to benchmark harness execution. | Off by default (demo mode). |
| `--mode blocking\|async` | Sets benchmark execution mode (`blocking` = synchronous request/response, `async` = pipelined async operations). | `blocking` if omitted. |
| `--pipeline-depth <int>` | Max number of concurrent in-flight operations in async benchmark mode. | Parsed value is used in async mode; forced to `1` in blocking mode. |
| `--ops <int>` | Number of operations generated per benchmark round. | Uses benchmark default if omitted. |
| `--rounds <int>` | Number of benchmark rounds to run. | Uses benchmark default if omitted. |
| `--key-space <int>` | Number of logical keys used when generating benchmark workloads (larger values reduce key reuse). | Uses benchmark default if omitted. |
| `--timeout-ms <int>` | Async receive timeout in milliseconds for benchmark requests. | Uses benchmark default if omitted. |
| `--csv <path>` | Writes benchmark results to CSV at the provided path. | No CSV file unless provided. |
| `--quiet` | Reduces node-side logging verbosity in combined mode. | Verbose node logging by default. |
| `--non-verbose` | Alias for `--quiet` (same behavior). | Same as `--quiet`. |
| `--enable-persistence` | Enables persistence globally (acts as default-on for snapshot + WAL unless component flags override). | In demo mode, persistence is on by default even without this flag. |
| `--disable-persistence` | Disables persistence globally (acts as default-off for snapshot + WAL unless component flags override). | In benchmark mode, this is effectively the default unless explicitly enabled. |
| `--enable-snapshot` | Forces snapshot persistence on, regardless of global persistence default. | Inherits global persistence default when omitted. |
| `--disable-snapshot` | Forces snapshot persistence off, regardless of global persistence default. | Inherits global persistence default when omitted. |
| `--enable-wal` | Forces WAL persistence on, regardless of global persistence default. | Inherits global persistence default when omitted. |
| `--disable-wal` | Forces WAL persistence off, regardless of global persistence default. | Inherits global persistence default when omitted. |
| `--snapshot-interval-sec <int>` | Sets snapshot interval in seconds for storage nodes (time-based snapshot trigger). | `1800` seconds (30 minutes). Must be `> 0`. |

Notes on persistence combinations:

- If snapshot is enabled, snapshot writes are attempted after successful `PUT` and successful `DEL` operations, but only when the configured snapshot interval has elapsed.
- If WAL is also enabled, WAL is still appended on each write/delete and truncated when a snapshot is saved.
- If only WAL is enabled, recovery replays WAL without snapshot load.

Current snapshot behavior details (from implementation):

- Snapshot interval is time-based and defaults to 1800 seconds (30 minutes).
- You can override the interval with `--snapshot-interval-sec <seconds>` (must be `> 0`).
- On startup, node recovery loads snapshot first, then replays WAL entries (if WAL is enabled).

Performance improvement ideas:

- For benchmark-heavy runs, prefer `--disable-persistence` or `--disable-snapshot --enable-wal` to avoid full-map snapshot rewrites on hot paths.
- Keep `snapshot+WAL` for durability while avoiding per-operation full snapshot costs.
- Increase `--snapshot-interval-sec` so snapshots happen less frequently under write-heavy workloads.
- Move snapshot writing off the request path (background/checkpoint thread) so PUT/DEL latency is less sensitive to disk flushes.
- Avoid opening/closing WAL file handles on every operation by keeping a WAL stream open per node process.

### Run demo mode (default)

```bash
mpirun --oversubscribe -np 4 ./kvstore
```

### Run benchmark mode

```bash
mpirun --oversubscribe -np 4 ./kvstore --benchmark [options]
```

Example:

```bash
mpirun --oversubscribe -np 4 ./kvstore \
  --benchmark \
  --mode async \
  --pipeline-depth 8 \
  --ops 5000 \
  --rounds 3 \
  --key-space 512 \
  --timeout-ms 2000 \
  --csv bench_async.csv
```

## 3.3 `kvserver` options (separated mode)

| Option | What it does | Default / behavior |
|---|---|---|
| `--server-world-size <int>` | Declares how many MPI ranks belong to the server side (router rank 0 + storage node ranks). The server only runs node loops for ranks `< server_world_size`. | Defaults to MPI world size if omitted; must be `>= 2`. |
| `--disable-persistence` | Disables persistence globally on storage nodes (snapshot + WAL off unless component flags override). | Persistence is enabled by default if omitted. |
| `--enable-snapshot` | Forces snapshot persistence on for nodes. | Inherits global persistence default when omitted. |
| `--disable-snapshot` | Forces snapshot persistence off for nodes. | Inherits global persistence default when omitted. |
| `--enable-wal` | Forces WAL persistence on for nodes. | Inherits global persistence default when omitted. |
| `--disable-wal` | Forces WAL persistence off for nodes. | Inherits global persistence default when omitted. |
| `--snapshot-interval-sec <int>` | Sets snapshot interval in seconds for storage nodes (time-based snapshot trigger). | `1800` seconds (30 minutes). Must be `> 0`. |

Example:

```bash
mpirun --oversubscribe -np 4 ./kvserver --server-world-size 4
```

## 3.4 `kvclient` options (separated mode)

| Option | What it does | Default / behavior |
|---|---|---|
| `--server-world-size <int>` | Tells the client the server-rank span to target (router + storage ranks). Must match the server launch configuration for correct routing and shutdown behavior. | Defaults to MPI world size if omitted; must be `>= 2`. |

Example:

```bash
mpirun --oversubscribe -np 1 ./kvclient --server-world-size 4
```

---

## 4) Function / Component Outline

## 4.1 Entrypoints

- `src/main.cpp`
  - Combined mode driver.
  - Rank 0 runs demo or benchmark client logic.
  - Ranks 1..N-1 run node server loop.
- `src/server_main.cpp`
  - Separated server driver.
  - Rank 0: router/coordinator loop.
  - Ranks 1..server_world_size-1: node server.
- `src/client_main.cpp`
  - Standalone client demo in router-mediated mode.

## 4.2 Client layer

- `src/client/client.h/.cpp`
  - `KvClient` provides `put/get/del`, batch execute, and async execute.
  - Supports two operation modes:
    - `DIRECT_TO_PRIMARY`
    - `ROUTER_MEDIATED`
  - Async pipeline supports per-key safety: at most one in-flight request per key.

## 4.3 Routing

- `src/router/router.h/.cpp`
  - `route_key_to_primary(key, world_size)` maps a key hash to storage rank in `[1, world_size-1]`.

## 4.4 Node server + storage

- `src/node/node_server.cpp`
  - Handles `PUT_REQ`, `GET_REQ`, `DEL_REQ`, `SHUTDOWN`.
  - Persists snapshots for local storage state.
- `src/node/storage.h/.cpp`
  - Local key-value state and version tracking.

## 4.5 Protocol and MPI wrappers

- `src/common/protocol.h`
  - Fixed-size `Message`, message constructors, status enums.
- `src/common/mpi_comm.h/.cpp`
  - Safe wrappers for send/recv/probe using `MPI_BYTE` and `sizeof(Message)`.

## 4.6 Benchmarking

- `src/benchmark/benchmark.h/.cpp`
  - Blocking + async benchmark runners.
  - CSV output and summary stats.

---

## 5) Useful Commands / Test Matrix

## 5.1 Build validation

```bash
make
```

## 5.2 Combined-mode smoke test

```bash
mpirun --oversubscribe -np 4 ./kvstore
```

## 5.3 Combined-mode benchmark test

```bash
mpirun --oversubscribe -np 4 ./kvstore --benchmark --mode blocking --ops 1000 --rounds 1 --csv bench_blocking.csv
```

```bash
mpirun --oversubscribe -np 4 ./kvstore --benchmark --mode async --pipeline-depth 8 --ops 1000 --rounds 1 --timeout-ms 1000 --csv bench_async.csv
```

## 5.4 Separated router-mediated smoke test

```bash
mpirun --oversubscribe -np 4 ./kvserver --server-world-size 4 : -np 1 ./kvclient --server-world-size 4
```

## 5.5 Root-container note (if applicable)

Some environments require allowing MPI as root:

```bash
OMPI_ALLOW_RUN_AS_ROOT=1 OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1 mpirun --oversubscribe -np 4 ./kvstore
```

---

## 6) How to Create Custom Tests (Examples)

Below are lightweight ways to create custom tests without adding a full test framework.

## 6.1 Custom benchmark scenario

Use benchmark flags to synthesize a workload quickly:

```bash
mpirun --oversubscribe -np 6 ./kvstore \
  --benchmark --mode async --pipeline-depth 16 \
  --ops 20000 --rounds 2 --key-space 128 --timeout-ms 3000 \
  --csv custom_hotspot_test.csv
```

- Smaller `key-space` increases contention/hot keys.
- Larger `pipeline-depth` increases concurrency pressure.

## 6.2 Scripted regression runner

Create a local script (example `scripts/run_regression.sh`) with multiple commands:

```bash
#!/usr/bin/env bash
set -euo pipefail

make
mpirun --oversubscribe -np 4 ./kvstore
mpirun --oversubscribe -np 4 ./kvstore --benchmark --mode blocking --ops 2000 --rounds 1 --csv reg_blocking.csv
mpirun --oversubscribe -np 4 ./kvstore --benchmark --mode async --pipeline-depth 8 --ops 2000 --rounds 1 --timeout-ms 2000 --csv reg_async.csv
mpirun --oversubscribe -np 4 ./kvserver --server-world-size 4 : -np 1 ./kvclient --server-world-size 4
```

Then run:

```bash
bash scripts/run_regression.sh
```

## 6.3 Custom client behavior test (small code edit path)

If you want deterministic operation ordering/keys, add a temporary custom command vector and execute via:

- `KvClient::execute_many(...)` for strict sequential behavior
- `KvClient::execute_many_async(...)` for pipelined behavior with per-key in-flight ordering protection

A typical custom vector pattern:

- `PUT(k1)`
- `PUT(k2)`
- `DEL(k1)`
- `GET(k1)`
- `GET(k2)`

This helps validate same-key ordering while still allowing overlap across different keys.

---

## 7) Troubleshooting

- **Not enough slots**: add `--oversubscribe`.
- **Client/server rank mismatch in separated mode**: ensure `--server-world-size` matches both binaries and the server `-np` size.
- **Benchmark timeout exceptions**: increase `--timeout-ms` or reduce pipeline depth.
