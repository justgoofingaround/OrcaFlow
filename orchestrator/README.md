# Orchestrator (Rust Control Plane)

Rust control plane for orchestrating workers, scheduling jobs, running local and PySpark workloads. It pairs with **OrcaFlow** at the repository root (`ARCHITECTURE.md`) as the execution layer behind REST and ML routing.

---

## Overview

| Component | Binary | Role |
|-----------|--------|------|
| **Controller** | `ccm-controller` | HTTP master: worker registry, job submission/kill APIs, Spark coordination, scheduler and local-job executor loops, configurable protocols. |
| **Agent** | `ccm-agent` | Worker process: exposes a gRPC **Worker** service, registers with the controller, executes assigned work and reports status. |

At runtime the controller binds to `local_ip:port` (default port `5000`), sets `MASTER_URL` and `SPARK_MASTER_WEB_URL` for child processes, and spawns background tasks for `worker_manager`, `job_scheduler`, local job execution, and Spark scheduling.

---

## Prerequisites

- **Rust** toolchain (2021 edition), **Cargo**
- **Linux** is the primary target: dependencies include `procfs`, `nix`, and `sysinfo` features aimed at Linux hosts. Building or running on other OSes may require changes or WSL.
- Optional: **Spark** cluster URLs if you use Spark-related APIs (`--spark-master-url`, `--spark-web-url` on the controller; `--spark-master-url` on the agent).

---

## Build

From this directory:

```bash
cargo build --release
```

Artifacts:

- `target/release/ccm-controller`
- `target/release/ccm-agent`

Protobuf sources under `proto/` are compiled via `build.rs` (`tonic-build`).

---

## Controller (`ccm-controller`)

### CLI (see `src/ccm-controller/dto.rs`)

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `5000` | HTTP listen port. |
| `--workarea` | current directory | Work directory; DB scaffolding and protocols use this tree. |
| `--max-local-memory` | `40.0` | Local execution memory budget (GiB semantics as used in code). |
| `--max-local-cores` | `6.0` | Local CPU budget. |
| `--parent-pids` | _(optional)_ | Space-separated parent PIDs. |
| `--spark-master-url` | `""` | Spark master URL when using Spark features. |
| `--spark-web-url` | `""` | Spark UI base URL for monitoring integration. |
| **`--default-protocol`** | _(required)_ | Default protocol key for dispatched jobs (must match configured farm protocol). |
| `--protocol-path` | `""` | Extra path used when loading protocol configurations. |

### HTTP API (Axum routes)

Examples of routes registered in `setup.rs`:

- `GET /` — heartbeat
- `POST /run_job`, `POST /kill_job`
- `POST /update_worker`, `POST /update_worker_status`
- `POST /update_job_status`, `POST /get_job_status`
- `POST /update_config`, `GET /get_config`
- `POST /add_spark_driver`, `POST /run_spark_job`
- `GET /can_request_spark_workers`, `POST /get_running_workers_count`
- `POST /refresh_protocols`, `POST /kill_idle_worker`
- `DELETE /kill_all_jobs`

Responses and payloads align with JSON types shared with internal modules (`dto`, APIs under `apis.rs`).

### Protocol Configuration

Protocol definitions live under `farm_protocols/`:

- **`local/`** — local execution configuration (`config.json` with scratch disk and resource knobs).

Protocol settings are loaded via the shared `protocols_map`; `POST /refresh_protocols` reloads them when configuration changes.

---

## Agent (`ccm-agent`)

Workers talk to the controller over HTTP for registration/status and expose **gRPC** for `Heartbeat`, `RunJob`, `KillJob`, `Terminate`, and Spark `RegisterApp` (see `proto/worker.proto`).

### CLI (see `src/ccm-agent/setup.rs`)

Important flags:

- `--master-url` — Controller base URL (`http://<controller-ip>:<port>`).
- `--master-workarea` — Must align with controller `--workarea`.
- **`--name`** — Worker/logger name.
- **`--worker-hash`** — Stable id hash for registry updates.
- `--max-cores`, `--max-memory` — Capacity advertised after local qualification checks.
- `--cluster-home` — Cluster filesystem root used by tooling.
- `--protocol` — Worker protocol identifier.
- `--spark-master-url` — Spark master for Spark workloads.
- Optional lists (space-separated where applicable): `--job-names`, `--categories`, `--job-types`, `--lightweight`.

The agent verifies it is “qualified” (cores/memory vs machine) against the controller before registering; failures are written under the master workarea and the process exits with status `1`.

---

## Supporting files

| Path | Purpose |
|------|---------|
| `proto/worker.proto`, `proto/flow_mgmt.proto` | gRPC service definitions (built in `build.rs`). |
| `spark_conf/worker.conf` | Spark-related configuration fragment. |


---

## Example: minimal local controller

```bash
./target/release/ccm-controller \
  --workarea /tmp/ccm-work \
  --default-protocol local
```

Then start an agent with matching `master-url`, `master-workarea`, `name`, `worker-hash`, `cluster-home`, `protocol`, and resource limits (see flags above).

---

## Relationship to OrcaFlow

The Python **OrcaFlow** API (`../orcaflow/api/`) can submit and classify jobs; wiring that API to this cluster manager (HTTP/gRPC) is project-specific. This crate is the **Rust execution and worker-management core** described in the top-level architecture document.

---

## License / course context

Part of the NYU Big Data (CS-GY-6513) OrcaFlow project; see the repository root `README.md` for environment and Docker setup that often runs beside this control plane.
