# localrun

Build and run the ramen manager locally against drenv clusters, without
deploying to Kubernetes.

## Usage

### Prerequisites

A running drenv environment with three clusters (hub, dr1, dr2) and minio
deployed on dr1 and dr2. Kubeconfigs are expected at
`~/.config/drenv/rdr/kubeconfigs/{hub,dr1,dr2}`.

### One-time setup

Install CRDs, create namespaces, S3 secrets, ConfigMaps, DRClusters, and
DRPolicies on all three clusters:

```
go run ./localrun --configure
```

This is idempotent and can be re-run after code changes that modify CRDs.

### Run the managers

```
go run ./localrun
```

This builds the manager binary and starts three instances (one per cluster).
All output is prefixed with the cluster name (`[hub]`, `[dr1]`, `[dr2]`).
Press Ctrl-C to stop all three.

To skip the build step (e.g. after a no-op change):

```
go run ./localrun --skip-build
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--configure` | false | Run cluster setup and exit |
| `--skip-build` | false | Skip building the manager binary |
| `--hub-kubeconfig` | `~/.config/drenv/rdr/kubeconfigs/hub` | Path to hub kubeconfig |
| `--dr1-kubeconfig` | `~/.config/drenv/rdr/kubeconfigs/dr1` | Path to dr1 kubeconfig |
| `--dr2-kubeconfig` | `~/.config/drenv/rdr/kubeconfigs/dr2` | Path to dr2 kubeconfig |

## Architecture

### The problem

The normal ramen development loop is: edit code, build a container image, push
it to a registry, update the deployment, wait for the pod to restart, then
check logs. This takes minutes per iteration.

### How localrun solves it

localrun is a process supervisor that compiles `cmd/main.go` into a native
binary and runs three copies of it locally, one for each cluster. Each
instance gets its own kubeconfig and talks directly to the cluster's API
server. There is no container build, no image push, and no pod restart.

The edit-build-test cycle becomes: edit code, run `go run ./localrun`, and
the managers are up in seconds.

### How it works

```
localrun
  |
  |-- builds cmd/main.go -> localrun/bin/manager
  |
  |-- spawns manager (hub)  -- KUBECONFIG=hub,  RAMEN_CONTROLLER_TYPE=dr-hub
  |-- spawns manager (dr1)  -- KUBECONFIG=dr1,  RAMEN_CONTROLLER_TYPE=dr-cluster
  |-- spawns manager (dr2)  -- KUBECONFIG=dr2,  RAMEN_CONTROLLER_TYPE=dr-cluster
  |
  |-- prefixes all stdout/stderr with [hub], [dr1], [dr2]
  |-- Ctrl-C sends SIGTERM to all three, waits for clean shutdown
```

Each manager instance runs the same binary as the in-cluster deployment.
The only difference is how it gets its configuration:

- **Controller type**: set via `RAMEN_CONTROLLER_TYPE` environment variable
  (`dr-hub` for hub, `dr-cluster` for dr1/dr2).
- **Namespace**: set via `POD_NAMESPACE=ramen-system`.
- **Kubeconfig**: passed via `--kubeconfig` flag.
- **RamenConfig**: read from a ConfigMap in `ramen-system` on each cluster,
  created by `--configure`. Each cluster gets unique health/metrics ports to
  avoid bind conflicts when all three run on the same machine.

### Configuration (`--configure`)

The configure step sets up each cluster with:

1. **Namespaces**: `ramen-system` and `ramen-ops`
2. **CRDs**: `make install-hub` for hub, `make install-dr-cluster` for dr1/dr2
3. **S3 secrets**: `ramen-s3-secret-dr1` and `ramen-s3-secret-dr2` with minio
   credentials, created in `ramen-system` on all three clusters
4. **ConfigMap**: per-cluster config from `localrun/configs/{hub,dr1,dr2}.yaml`
   with minio endpoints auto-discovered and injected
5. **Hub-only resources**: ManagedClusterSetBinding, DRCluster resources
   (dr1, dr2), and DRPolicies (dr-policy-1m, dr-policy-5m)

The per-cluster ConfigMaps in `localrun/configs/` only specify overrides
(unique ports, S3 profiles). All other fields use the defaults from the
ramen code.

### Port assignments

| Cluster | Metrics | Health probe |
|---------|---------|--------------|
| hub     | :9310   | :9410        |
| dr1     | :9320   | :9420        |
| dr2     | :9330   | :9430        |
