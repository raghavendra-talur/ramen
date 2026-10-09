# localrun

Build and run the ramen manager locally against drenv clusters, without
deploying to Kubernetes.

## Usage

### Prerequisites

A running drenv environment with minio deployed on the DR clusters. The cluster
topology is read from a drenv environment file (default:
`test/envs/regional-dr.yaml`). Kubeconfigs are expected at
`~/.config/drenv/<env-name>/kubeconfigs/<cluster-name>`.

### One-time setup

Install CRDs, create namespaces, S3 secrets, ConfigMaps, DRClusters, and
DRPolicies:

```
go run ./localrun configure
```

This is idempotent and can be re-run after code changes that modify CRDs.

### Run the managers

```
go run ./localrun run
```

This builds the manager binary, scales down the in-cluster ramen operators, and
starts one manager process per (cluster, reconciler). Output is prefixed with
`[<cluster>:<reconciler>]` and also written to
`localrun/logs/<cluster>-<reconciler>.log`. Press Ctrl-C (or send SIGTERM) to
stop all.

Useful flags:

```
go run ./localrun run --skip-build            # reuse localrun/bin/manager
go run ./localrun run --reconcilers vrg,pvrgl # run only these reconcilers
go run ./localrun run --log-dir /tmp/ramen    # write logs elsewhere
go run ./localrun run --no-logs               # terminal output only
```

### Check what is running

```
go run ./localrun status
go run ./localrun status --json
```

`status` reads `localrun/state.json` and checks it against reality. Each
expected manager is shown as:

- `ready`: the process is running and `/readyz` answers 200
- `not-ready`: the process is running but `/readyz` fails or times out (1s)
- `dead`: the manager was started but its process is gone
- `missing`: the manager was never started by the current supervisor

A process counts as running only if it exists and its command line is still the
localrun manager binary with that manager's health probe address, so a reused
pid is reported as dead. The expected managers come from the envfile and the
`--reconcilers` filter the supervisor was started with.

With `--json`, stdout is only the JSON report (warnings go to stderr):

```json
{
  "stateFile": "/abs/ramen/localrun/state.json",
  "supervisor": {"pid": 123, "alive": true, "startedAt": "2026-10-09T10:00:00Z", "stoppedAt": ""},
  "env": "rdr",
  "envfile": "/abs/ramen/test/envs/regional-dr.yaml",
  "managers": [
    {"cluster": "hub", "reconciler": "drpc", "expected": true, "pid": 456,
     "alive": true, "ready": true, "readyError": "", "healthAddr": "127.0.0.1:9412",
     "log": "/abs/ramen/localrun/logs/hub-drpc.log", "startedAt": "2026-10-09T10:00:01Z",
     "exitedAt": "", "exitErr": ""}
  ]
}
```

Without a state file, `supervisor` is `null` and every expected manager is
listed as not alive. `status` exits 0 whenever it produces a report.

### Stop

```
go run ./localrun stop [--timeout 15s]
```

`stop` sends SIGTERM to the supervisor and waits for it to stop its managers.
Any manager still running afterwards gets SIGTERM, and whatever is left after
the timeout gets SIGKILL. It prints one line per action and is safe to run when
nothing is running.

### Refresh

```
go run ./localrun refresh
```

`refresh` runs `stop`, clears the log directory, rebuilds, and starts the
managers again. It takes the same flags as `run`.

### Using a different environment file

```
go run ./localrun --envfile path/to/env.yaml configure
go run ./localrun --envfile path/to/env.yaml run
```

The envfile must have a `name` field and a `ramen` section with `hub` and
`clusters` fields, like `test/envs/regional-dr.yaml`:

```yaml
name: "rdr"
ramen:
  hub: hub
  clusters: [dr1, dr2]
```

`status` describes the envfile recorded in the state file unless `--envfile` is
given.

### Commands and flags

| Command         | Flag            | Default                      | Description                               |
| --------------- | --------------- | ---------------------------- | ----------------------------------------- |
| (global)        | `--envfile`     | `test/envs/regional-dr.yaml` | Path to drenv environment file            |
| `configure`     |                 |                              | Set up clusters and exit                  |
| `run`           |                 |                              | Build and start ramen managers            |
| `run`/`refresh` | `--skip-build`  | false                        | Skip building the manager binary          |
| `run`/`refresh` | `--reconcilers` | all                          | Comma-separated reconciler filter         |
| `run`/`refresh` | `--log-dir`     | `localrun/logs`              | Directory for per-reconciler log files    |
| `run`/`refresh` | `--no-logs`     | false                        | Disable log files (terminal only)         |
| `refresh`       |                 |                              | Stop, clear logs, rebuild, and run        |
| `status`        | `--json`        | false                        | Print the status report as JSON           |
| `stop`          | `--timeout`     | 15s                          | Wait after SIGTERM before sending SIGKILL |

Reconcilers: `drpolicy`, `drcluster`, `drpc` on the hub; `pvrgl`, `vrg`,
`drclusterconfig`, `rgd`, `rgs` on each DR cluster.

## Architecture

### The problem

The normal ramen development loop is: edit code, build a container image, push
it to a registry, update the deployment, wait for the pod to restart, then check
logs. This takes minutes per iteration.

### How localrun solves it

localrun is a process supervisor that compiles `cmd/main.go` into a native
binary and runs one copy per (cluster, reconciler) locally. Each instance gets
its cluster's kubeconfig and talks directly to the cluster's API server. There
is no container build, no image push, and no pod restart.

The edit-build-test cycle becomes: edit code, run `go run ./localrun refresh`,
and the managers are up in seconds.

### How it works

```
localrun run
  |
  |-- reads envfile -> discovers cluster names and kubeconfig paths
  |-- writes localrun/state.json (supervisor pid, envfile, filter)
  |
  |-- builds cmd/main.go -> localrun/bin/manager
  |-- scales down the in-cluster ramen operators
  |
  |-- spawns manager (hub, drpc) -- KUBECONFIG=hub, RAMEN_CONTROLLER_TYPE=dr-hub,
  |                                 RAMEN_RECONCILERS=drpc
  |-- spawns manager (dr1, vrg)  -- KUBECONFIG=dr1, RAMEN_CONTROLLER_TYPE=dr-cluster,
  |                                 RAMEN_RECONCILERS=vrg
  |-- ... one per (cluster, reconciler)
  |
  |-- records each manager start and exit in state.json
  |-- prefixes output with [hub:drpc], [dr1:vrg], ...
  |-- SIGINT/SIGTERM: sends SIGTERM to all, waits, records stoppedAt
```

Each manager instance runs the same binary as the in-cluster deployment. The
only difference is how it gets its configuration:

- **Controller type**: set via `RAMEN_CONTROLLER_TYPE` environment variable
  (`dr-hub` for hub, `dr-cluster` for DR clusters).
- **Reconciler**: set via `RAMEN_RECONCILERS`, one per process.
- **Namespace**: set via `POD_NAMESPACE=ramen-system`.
- **Kubeconfig**: passed via `--kubeconfig` flag.
- **Manager options**: passed as flags, like the in-cluster deployment does —
  unique `--metrics-bind-address`/`--health-probe-bind-address` per process to
  avoid bind conflicts on one machine, and `--leader-elect=false` (the
  per-reconciler processes of a cluster would otherwise contend for one lease).
- **RamenConfig**: read from a ConfigMap in `ramen-system` on each cluster,
  created by `configure`.

### State file

`localrun/state.json` is owned by the running supervisor (`run` or `refresh`)
and replaced atomically (temp file and rename) when the supervisor starts,
whenever a manager starts or exits, and when the supervisor exits. Other
programs, like `status`, `stop`, and the ramendev dashboard, only read it. Its
field names are a contract:

```json
{
  "pid": 123, "startedAt": "RFC3339", "stoppedAt": "",
  "envfile": "/abs/path", "env": "rdr", "reconcilers": ["vrg", "pvrgl"],
  "managers": [
    {"cluster": "hub", "reconciler": "drpc", "pid": 456,
     "healthAddr": "127.0.0.1:9412", "metricsAddr": "127.0.0.1:9312",
     "log": "/abs/.../hub-drpc.log", "startedAt": "RFC3339",
     "exitedAt": "", "exitErr": ""}
  ]
}
```

`reconcilers` is the `--reconcilers` filter as given (empty means all). A
manager that failed to start is recorded with pid 0 and the error in `exitErr`.
The file records what the supervisor saw; use `status` to learn what is actually
running.

### Configuration (`configure`)

The configure subcommand sets up each cluster with:

1. **Namespaces**: `ramen-system` and `ramen-ops`
1. **CRDs**: `make install-hub` for hub, `make install-dr-cluster` for DR
   clusters
1. **S3 secrets**: one per DR cluster with minio credentials, created in
   `ramen-system` on all clusters
1. **ConfigMap**: per-cluster config from `localrun/configs/<cluster>.yaml` with
   minio endpoints auto-discovered and injected
1. **Hub-only resources**: ManagedClusterSetBinding, DRCluster resources, and
   DRPolicies (dr-policy-1m, dr-policy-5m)

The per-cluster ConfigMaps in `localrun/configs/` only specify
environment-specific overrides (S3 profiles). All other fields use the defaults
from the ramen code; manager options (metrics, health probe, leader election)
are flags on the manager, not config.

### Port assignments

Each (cluster, reconciler) process binds `127.0.0.1:93<cluster><rec>` for
metrics and `127.0.0.1:94<cluster><rec>` for its health probe, where `<cluster>`
is 1 for hub, 2/3 for the DR clusters, and `<rec>` is the reconciler's slot
digit (see `reconcilerPort` in `main.go`).
