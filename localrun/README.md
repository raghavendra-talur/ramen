# localrun

Build and run the ramen manager locally against drenv clusters, without
deploying to Kubernetes.

## Usage

### Prerequisites

A running drenv environment with minio deployed on the DR clusters.
The cluster topology is read from a drenv environment file (default:
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

This builds the manager binary and starts one instance per cluster.
All output is prefixed with the cluster name (`[hub]`, `[dr1]`, `[dr2]`).
Press Ctrl-C to stop all.

To skip the build step (e.g. after a no-op change):

```
go run ./localrun run --skip-build
```

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

### Commands and flags

| Command     | Flag           | Default                          | Description                      |
|-------------|----------------|----------------------------------|----------------------------------|
| (global)    | `--envfile`    | `test/envs/regional-dr.yaml`     | Path to drenv environment file   |
| `run`       |                |                                  | Build and start ramen managers   |
| `run`       | `--skip-build` | false                            | Skip building the manager binary |
| `configure` |                |                                  | Set up clusters and exit         |

## Architecture

### The problem

The normal ramen development loop is: edit code, build a container image, push
it to a registry, update the deployment, wait for the pod to restart, then
check logs. This takes minutes per iteration.

### How localrun solves it

localrun is a process supervisor that compiles `cmd/main.go` into a native
binary and runs one copy per cluster locally. Each instance gets its own
kubeconfig and talks directly to the cluster's API server. There is no
container build, no image push, and no pod restart.

The edit-build-test cycle becomes: edit code, run `go run ./localrun run`,
and the managers are up in seconds.

### How it works

```
localrun
  |
  |-- reads envfile → discovers cluster names and kubeconfig paths
  |
  |-- builds cmd/main.go -> localrun/bin/manager
  |
  |-- spawns manager (hub)  -- KUBECONFIG=hub,  RAMEN_CONTROLLER_TYPE=dr-hub
  |-- spawns manager (dr1)  -- KUBECONFIG=dr1,  RAMEN_CONTROLLER_TYPE=dr-cluster
  |-- spawns manager (dr2)  -- KUBECONFIG=dr2,  RAMEN_CONTROLLER_TYPE=dr-cluster
  |
  |-- prefixes all stdout/stderr with [hub], [dr1], [dr2]
  |-- Ctrl-C sends SIGTERM to all, waits for clean shutdown
```

Each manager instance runs the same binary as the in-cluster deployment.
The only difference is how it gets its configuration:

- **Controller type**: set via `RAMEN_CONTROLLER_TYPE` environment variable
  (`dr-hub` for hub, `dr-cluster` for DR clusters).
- **Namespace**: set via `POD_NAMESPACE=ramen-system`.
- **Kubeconfig**: passed via `--kubeconfig` flag.
- **Manager options**: passed as flags, like the in-cluster deployment does —
  unique `--metrics-bind-address`/`--health-probe-bind-address` per process to
  avoid bind conflicts on one machine, and `--leader-elect=false` (the
  per-reconciler processes of a cluster would otherwise contend for one lease).
- **RamenConfig**: read from a ConfigMap in `ramen-system` on each cluster,
  created by `configure`.

### Configuration (`configure`)

The configure subcommand sets up each cluster with:

1. **Namespaces**: `ramen-system` and `ramen-ops`
2. **CRDs**: `make install-hub` for hub, `make install-dr-cluster` for DR clusters
3. **S3 secrets**: one per DR cluster with minio credentials, created in
   `ramen-system` on all clusters
4. **ConfigMap**: per-cluster config from `localrun/configs/<cluster>.yaml`
   with minio endpoints auto-discovered and injected
5. **Hub-only resources**: ManagedClusterSetBinding, DRCluster resources,
   and DRPolicies (dr-policy-1m, dr-policy-5m)

The per-cluster ConfigMaps in `localrun/configs/` only specify
environment-specific overrides (S3 profiles). All other fields use the
defaults from the ramen code; manager options (metrics, health probe,
leader election) are flags on the manager, not config.

### Port assignments

Each (cluster, reconciler) process binds `127.0.0.1:93<cluster><rec>` for
metrics and `127.0.0.1:94<cluster><rec>` for its health probe, where
`<cluster>` is 1 for hub, 2/3 for the DR clusters, and `<rec>` is the
reconciler's slot digit (see `reconcilerPort` in `main.go`).
