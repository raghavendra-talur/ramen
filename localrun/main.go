// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// localrun builds and runs the ramen manager locally against drenv clusters.
// Usage: go run ./localrun [command] [flags]
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
)

var hubReconcilers = []string{"drpolicy", "drcluster", "drpc"}

var drClusterReconcilers = []string{"pvrgl", "vrg", "drclusterconfig", "rgd", "rgs"}

var reconcilerPort = map[string]string{
	"drpolicy":        "0",
	"drcluster":       "1",
	"drpc":            "2",
	"pvrgl":           "3",
	"vrg":             "4",
	"rgd":             "5",
	"rgs":             "6",
	"drclusterconfig": "7",
}

type cluster struct {
	name       string
	kubeconfig string
	hub        bool
	openshift  bool
	index      int
}

type envFile struct {
	Name  string `json:"name"`
	Ramen struct {
		Hub      string   `json:"hub"`
		Clusters []string `json:"clusters"`
	} `json:"ramen"`
}

// runOptions holds the flags shared by run and refresh.
type runOptions struct {
	root        string
	envfile     string
	logDir      string
	noLogs      bool
	reconcilers string
	skipBuild   bool
}

func (o runOptions) bin() string {
	return filepath.Join(o.root, "localrun", "bin", "manager")
}

// resolvedLogDir returns the absolute log directory, or "" with --no-logs.
func (o runOptions) resolvedLogDir() string {
	if o.noLogs {
		return ""
	}

	if filepath.IsAbs(o.logDir) {
		return o.logDir
	}

	return filepath.Join(o.root, o.logDir)
}

func addRunFlags(cmd *cobra.Command, o *runOptions) {
	cmd.Flags().BoolVar(&o.skipBuild, "skip-build", false, "skip building the manager binary")
	cmd.Flags().StringVar(&o.logDir, "log-dir", "localrun/logs", "directory for per-reconciler log files")
	cmd.Flags().BoolVar(&o.noLogs, "no-logs", false, "disable log files (terminal only)")
	cmd.Flags().StringVar(&o.reconcilers, "reconcilers", "", "comma-separated reconciler filter (e.g. vrg,pvrgl)")
}

func main() {
	opts := runOptions{root: repoRoot()}

	rootCmd := &cobra.Command{
		Use:   "localrun",
		Short: "Build and run ramen managers locally against drenv clusters",
	}

	rootCmd.PersistentFlags().StringVar(&opts.envfile, "envfile",
		filepath.Join(opts.root, "test", "envs", "regional-dr.yaml"),
		"path to drenv environment file")

	rootCmd.AddCommand(
		newRunCmd(&opts),
		newConfigureCmd(&opts),
		newRefreshCmd(&opts),
		newStatusCmd(&opts),
		newStopCmd(&opts),
	)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func newRunCmd(opts *runOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Build and start ramen managers (one process per reconciler)",
		Run: func(cmd *cobra.Command, args []string) {
			if err := supervise(*opts, loadClusters(opts.envfile)); err != nil {
				fatal("%v", err)
			}
		},
	}

	addRunFlags(cmd, opts)

	return cmd
}

func newConfigureCmd(opts *runOptions) *cobra.Command {
	return &cobra.Command{
		Use:   "configure",
		Short: "Install CRDs, create namespaces, S3 secrets, and config",
		Run: func(cmd *cobra.Command, args []string) {
			clusters := loadClusters(opts.envfile)

			for _, c := range clusters {
				if c.openshift {
					fatal("configure is only supported for upstream (drenv) environments; "+
						"cluster %q is OpenShift and must be configured out-of-band", c.name)
				}
			}

			configure(opts.root, clusters)
		},
	}
}

func newRefreshCmd(opts *runOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "refresh",
		Short: "Stop running managers, clear logs, rebuild, and restart",
		Run: func(cmd *cobra.Command, args []string) {
			clusters := loadClusters(opts.envfile)

			fmt.Println("Stopping the running supervisor and managers...")

			if err := newStopper(opts, os.Stdout, defaultStopTimeout).stop(stateFilePath(opts.root)); err != nil {
				fatal("stop: %v", err)
			}

			if logDir := opts.resolvedLogDir(); logDir != "" {
				fmt.Printf("Clearing log directory %s...\n", logDir)
				clearLogDir(logDir)
			}

			if err := supervise(*opts, clusters); err != nil {
				fatal("%v", err)
			}
		},
	}

	addRunFlags(cmd, opts)

	return cmd
}

// supervise builds the manager, scales down the in-cluster operators, and
// runs one manager per (cluster, reconciler) until SIGINT or SIGTERM. It
// records itself and each manager in the state file. Clusters start in
// parallel; managers on one cluster start one at a time (see startInOrder).
// It fails when the managers stop on their own rather than on a signal.
func supervise(o runOptions, clusters []cluster) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	state := newSupervisorRecorder(o)
	defer state.stopped()

	bin := o.bin()

	if !o.skipBuild {
		build(o.root, bin)
	}

	logDir := o.resolvedLogDir()
	if logDir != "" {
		os.MkdirAll(logDir, 0o755)
	}

	for _, c := range clusters {
		scaleDownOperator(c)
	}

	if ctx.Err() != nil {
		fmt.Println("Interrupted before starting managers.")

		return nil
	}

	fmt.Println("Starting ramen managers (Ctrl-C to stop)...")

	var wg sync.WaitGroup

	filter := parseReconcilerList(o.reconcilers)

	prober := httpProber{client: &http.Client{Timeout: probeTimeout}}

	for _, c := range clusters {
		// The starter counts as running until it has started every manager,
		// so wg.Wait cannot return between one manager exiting and the next
		// starting.
		wg.Add(1)

		go func() {
			defer wg.Done()

			startInOrder(ctx, selectedReconcilers(c, filter), func(rec string, settled func()) {
				wg.Add(1)

				go func() {
					defer wg.Done()

					runManager(ctx, bin, c, rec, logDir, state, prober, settled)
				}()
			})
		}()
	}

	wg.Wait()
	fmt.Println("All managers stopped.")

	return managersEnded(ctx)
}

func newSupervisorRecorder(o runOptions) *stateRecorder {
	env, err := parseEnvFile(o.envfile)
	if err != nil {
		fatal("%v", err)
	}

	envfile, err := filepath.Abs(o.envfile)
	if err != nil {
		fatal("cannot resolve envfile path %s: %v", o.envfile, err)
	}

	return newStateRecorder(stateFilePath(o.root), envfile, env.Name, parseReconcilerList(o.reconcilers))
}

func loadClusters(envfilePath string) []cluster {
	env, err := parseEnvFile(envfilePath)
	if err != nil {
		fatal("%v", err)
	}

	clusters, err := clusterTopology(env)
	if err != nil {
		fatal("%v", err)
	}

	for _, c := range clusters {
		checkFile(c.kubeconfig, "kubeconfig")
	}

	// Probe in parallel: an unreachable cluster costs a full timeout.
	var wg sync.WaitGroup

	for i := range clusters {
		wg.Add(1)

		go func() {
			defer wg.Done()

			clusters[i].openshift = isOpenShift(clusters[i].kubeconfig)
		}()
	}

	wg.Wait()

	for _, c := range clusters {
		if c.openshift {
			fmt.Printf("[%s] OpenShift cluster detected\n", c.name)
		}
	}

	return clusters
}

// parseEnvFile reads and validates a drenv environment file without touching
// any cluster.
func parseEnvFile(envfilePath string) (envFile, error) {
	var env envFile

	data, err := os.ReadFile(envfilePath)
	if err != nil {
		return env, fmt.Errorf("cannot read envfile %s: %w", envfilePath, err)
	}

	if err := yaml.Unmarshal(data, &env); err != nil {
		return env, fmt.Errorf("cannot parse envfile %s: %w", envfilePath, err)
	}

	switch {
	case env.Name == "":
		return env, fmt.Errorf("envfile %s: missing 'name' field", envfilePath)
	case env.Ramen.Hub == "":
		return env, fmt.Errorf("envfile %s: missing 'ramen.hub' field", envfilePath)
	case len(env.Ramen.Clusters) == 0:
		return env, fmt.Errorf("envfile %s: missing 'ramen.clusters' field", envfilePath)
	}

	return env, nil
}

// clusterTopology returns the clusters of env with their port index and
// kubeconfig path. It does not check that the kubeconfigs exist.
func clusterTopology(env envFile) ([]cluster, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("cannot get home directory: %w", err)
	}

	kcDir := filepath.Join(home, ".config", "drenv", env.Name, "kubeconfigs")

	clusters := make([]cluster, 0, 1+len(env.Ramen.Clusters))
	clusters = append(clusters,
		cluster{name: env.Ramen.Hub, kubeconfig: filepath.Join(kcDir, env.Ramen.Hub), hub: true, index: 1})

	for i, name := range env.Ramen.Clusters {
		clusters = append(clusters, cluster{
			name:       name,
			kubeconfig: filepath.Join(kcDir, name),
			index:      i + 2,
		})
	}

	return clusters, nil
}

func reconcilersForCluster(c cluster) []string {
	if c.hub {
		return hubReconcilers
	}

	return drClusterReconcilers
}

// parseReconcilerList parses the --reconcilers flag. An empty list means all
// reconcilers.
func parseReconcilerList(s string) []string {
	names := []string{}

	for _, name := range strings.Split(s, ",") {
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}

	return names
}

// selectedReconcilers returns the reconcilers to run on c, restricted to
// filter unless it is empty.
func selectedReconcilers(c cluster, filter []string) []string {
	all := reconcilersForCluster(c)
	if len(filter) == 0 {
		return all
	}

	var selected []string

	for _, rec := range all {
		if slices.Contains(filter, rec) {
			selected = append(selected, rec)
		}
	}

	return selected
}

func metricsAddr(c cluster, rec string) string {
	return fmt.Sprintf("127.0.0.1:93%d%s", c.index, reconcilerPort[rec])
}

func healthAddr(c cluster, rec string) string {
	return fmt.Sprintf("127.0.0.1:94%d%s", c.index, reconcilerPort[rec])
}

func minioServiceURL(clusterContext string) string {
	hostIP, err := exec.Command("kubectl", "get", "pod",
		"--selector=component=minio",
		"--namespace=minio",
		"--context="+clusterContext,
		"--output=jsonpath={.items[0].status.hostIP}",
	).Output()
	if err != nil {
		fatal("[%s] cannot get minio pod hostIP: %v", clusterContext, err)
	}

	nodePort, err := exec.Command("kubectl", "get", "service/minio",
		"--namespace=minio",
		"--context="+clusterContext,
		"--output=jsonpath={.spec.ports[0].nodePort}",
	).Output()
	if err != nil {
		fatal("[%s] cannot get minio service nodePort: %v", clusterContext, err)
	}

	url := fmt.Sprintf("http://%s:%s", strings.TrimSpace(string(hostIP)), strings.TrimSpace(string(nodePort)))
	fmt.Printf("[%s] minio endpoint: %s\n", clusterContext, url)

	return url
}

func configure(root string, clusters []cluster) {
	fmt.Println("Configuring clusters...")

	var drClusters []cluster

	for _, c := range clusters {
		if !c.hub {
			drClusters = append(drClusters, c)
		}
	}

	drURLs := make(map[string]string, len(drClusters))

	for _, c := range drClusters {
		drURLs[c.name] = minioServiceURL(c.name)
	}

	cfgDir := filepath.Join(root, "localrun", "configs")

	for _, c := range clusters {
		fmt.Printf("[%s] creating namespaces...\n", c.name)
		createNamespace(c.kubeconfig, "ramen-system")
		createNamespace(c.kubeconfig, "ramen-ops")

		target := "install-dr-cluster"
		if c.hub {
			target = "install-hub"
		}

		fmt.Printf("[%s] installing CRDs (%s)...\n", c.name, target)

		cmd := exec.Command("make", target)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "KUBECONFIG="+c.kubeconfig)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			fatal("[%s] %s failed: %v", c.name, target, err)
		}

		fmt.Printf("[%s] creating S3 secrets...\n", c.name)

		for _, dc := range drClusters {
			applyS3Secret(c.kubeconfig, dc.name)
		}

		fmt.Printf("[%s] creating ramen config...\n", c.name)
		configFile := filepath.Join(cfgDir, c.name+".yaml")
		applyResolvedConfig(c.kubeconfig, configFile, drURLs)
	}

	var hubKC string

	for _, c := range clusters {
		if c.hub {
			hubKC = c.kubeconfig

			break
		}
	}

	fmt.Println("[hub] creating ManagedClusterSetBinding...")
	applyManagedClusterSetBinding(hubKC)

	fmt.Println("[hub] creating DRClusters...")

	for _, dc := range drClusters {
		applyDRCluster(hubKC, dc.name)
	}

	fmt.Println("[hub] creating DRPolicies...")

	for _, interval := range []string{"1m", "5m"} {
		applyDRPolicy(hubKC, drClusters, interval)
	}

	fmt.Println("Configuration complete.")
}

func applyManifest(kubeconfig, manifest string) {
	cmd := exec.Command("kubectl", "--kubeconfig="+kubeconfig, "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		fatal("kubectl apply failed: %v", err)
	}
}

func applyS3Secret(kubeconfig, clusterName string) {
	manifest := fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: ramen-s3-secret-%s
  namespace: ramen-system
stringData:
  AWS_ACCESS_KEY_ID: minio
  AWS_SECRET_ACCESS_KEY: minio123
`, clusterName)
	applyManifest(kubeconfig, manifest)
}

func applyDRCluster(kubeconfig, name string) {
	manifest := fmt.Sprintf(`apiVersion: ramendr.openshift.io/v1alpha1
kind: DRCluster
metadata:
  name: %s
spec:
  s3ProfileName: minio-on-%s
`, name, name)
	applyManifest(kubeconfig, manifest)
}

func applyDRPolicy(kubeconfig string, drClusters []cluster, interval string) {
	var clusterLines string

	for _, dc := range drClusters {
		clusterLines += fmt.Sprintf("  - %s\n", dc.name)
	}

	manifest := fmt.Sprintf(`apiVersion: ramendr.openshift.io/v1alpha1
kind: DRPolicy
metadata:
  name: dr-policy-%s
spec:
  drClusters:
%s  schedulingInterval: %s
  replicationClassSelector: {}
  volumeSnapshotClassSelector: {}
`, interval, clusterLines, interval)
	applyManifest(kubeconfig, manifest)
}

func applyManagedClusterSetBinding(kubeconfig string) {
	manifest := `apiVersion: cluster.open-cluster-management.io/v1beta2
kind: ManagedClusterSetBinding
metadata:
  name: default
  namespace: ramen-ops
spec:
  clusterSet: default
`
	applyManifest(kubeconfig, manifest)
}

func applyResolvedConfig(kubeconfig, configPath string, drURLs map[string]string) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		fatal("cannot read config %s: %v", configPath, err)
	}

	resolved := string(data)

	for name, url := range drURLs {
		placeholder := fmt.Sprintf("http://CHANGE_ME_%s_IP:30000", strings.ToUpper(name))
		resolved = strings.ReplaceAll(resolved, placeholder, url)
	}

	cmd := exec.Command("kubectl", "--kubeconfig="+kubeconfig, "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(resolved)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		fatal("kubectl apply config failed: %v", err)
	}
}

func createNamespace(kubeconfig, ns string) {
	create := exec.Command("kubectl", "--kubeconfig="+kubeconfig,
		"create", "namespace", ns, "--dry-run=client", "-o", "yaml")
	apply := exec.Command("kubectl", "--kubeconfig="+kubeconfig, "apply", "-f", "-")

	var err error

	apply.Stdin, err = create.StdoutPipe()
	if err != nil {
		fatal("pipe failed: %v", err)
	}

	apply.Stdout = os.Stdout
	apply.Stderr = os.Stderr
	create.Stderr = os.Stderr

	if err := apply.Start(); err != nil {
		fatal("kubectl apply failed to start: %v", err)
	}

	if err := create.Run(); err != nil {
		fatal("kubectl create namespace %s failed: %v", ns, err)
	}

	if err := apply.Wait(); err != nil {
		fatal("kubectl apply namespace %s failed: %v", ns, err)
	}
}

func build(root, out string) {
	fmt.Println("Building manager...")

	os.MkdirAll(filepath.Dir(out), 0o755)

	cmd := buildCommand(root, out)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		fatal("build failed: %v", err)
	}

	fmt.Println("Build complete.")
}

// buildCommand returns the go build command for the manager binary. It
// disables cgo, as the container image build does: the manager does not need
// it, and a cgo link breaks whenever the host linker and SDK disagree.
func buildCommand(root, out string) *exec.Cmd {
	cmd := exec.Command("go", "build", "-o", out, "./cmd")
	cmd.Dir = root

	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")

	return cmd
}

func scaleDownOperator(c cluster) {
	ns := podNamespace(c)

	deployName := "ramen-dr-cluster-operator"
	if c.hub {
		deployName = "ramen-hub-operator"
	}

	cmd := exec.Command("kubectl", "--kubeconfig="+c.kubeconfig,
		"-n", ns, "scale", "--replicas=0", "deployment", deployName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err == nil {
		fmt.Printf("[%s] scaled down in-cluster %s\n", c.name, deployName)
	}
}

// runManager runs one manager until it exits, calling settled once it is past
// startup: ready, exited, or failed to start.
func runManager(
	ctx context.Context,
	bin string,
	c cluster,
	rec, logDirPath string,
	state *stateRecorder,
	prober readinessProber,
	settled func(),
) {
	tag := fmt.Sprintf("%s:%s", c.name, rec)
	cmd := managerCommand(ctx, bin, c, rec)

	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	logFile := openLogFile(tag, logDirPath, c, rec)
	if logFile != nil {
		defer logFile.Close()
	}

	m := managerState{
		Cluster:     c.name,
		Reconciler:  rec,
		HealthAddr:  healthAddr(c, rec),
		MetricsAddr: metricsAddr(c, rec),
	}
	if logFile != nil {
		m.Log = logFile.Name()
	}

	fmt.Printf("[%s] starting (kubeconfig=%s)\n", tag, c.kubeconfig)

	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "[%s] start failed: %v\n", tag, err)
		state.managerExited(state.managerStarted(m), fmt.Errorf("start failed: %w", err))
		settled()

		return
	}

	m.PID = cmd.Process.Pid
	idx := state.managerStarted(m)

	exited := make(chan struct{})
	defer close(exited)

	go waitStarted(ctx, prober, m.HealthAddr, exited, startupTimeout, settled)

	waitLogs := streamLogs(tag, logFile, stdout, stderr)

	waitErr := cmd.Wait()

	waitLogs()
	state.managerExited(idx, waitErr)

	if ctx.Err() != nil {
		fmt.Printf("[%s] stopped\n", tag)
	} else if waitErr != nil {
		fmt.Fprintf(os.Stderr, "[%s] exited: %v\n", tag, waitErr)
	}
}

// streamLogs copies each of a manager's output streams to stderr and its log
// file, and returns a function waiting until all of them are drained.
func streamLogs(tag string, logFile *os.File, streams ...io.Reader) func() {
	var wg sync.WaitGroup

	for _, r := range streams {
		wg.Add(1)

		go func() {
			defer wg.Done()

			logLines(os.Stderr, r, tag, logFile)
		}()
	}

	return wg.Wait
}

// managerCommand returns the command running bin as the manager for one
// reconciler on cluster c.
func managerCommand(ctx context.Context, bin string, c cluster, rec string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, bin,
		"--kubeconfig="+c.kubeconfig,
		"--metrics-bind-address="+metricsAddr(c, rec),
		"--health-probe-bind-address="+healthAddr(c, rec),
		// One process per (cluster, reconciler) pair: all processes for a
		// cluster would contend for the same lease, so no leader election.
		"--leader-elect=false",
	)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 10 * time.Second

	controllerType := "dr-cluster"
	if c.hub {
		controllerType = "dr-hub"
	}

	cmd.Env = append(os.Environ(),
		"POD_NAMESPACE="+podNamespace(c),
		"RAMEN_CONTROLLER_TYPE="+controllerType,
		"RAMEN_RECONCILERS="+rec,
	)

	return cmd
}

// openLogFile creates the manager's log file, or returns nil when logging to
// files is disabled or the file cannot be created.
func openLogFile(tag, logDirPath string, c cluster, rec string) *os.File {
	if logDirPath == "" {
		return nil
	}

	logFileName := filepath.Join(logDirPath, fmt.Sprintf("%s-%s.log", c.name, rec))

	logFile, err := os.Create(logFileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[%s] cannot create log file %s: %v\n", tag, logFileName, err)

		return nil
	}

	fmt.Printf("[%s] logging to %s\n", tag, logFileName)

	return logFile
}

func logLines(dst io.Writer, src io.Reader, tag string, logFile *os.File) {
	s := bufio.NewScanner(src)
	s.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for s.Scan() {
		line := s.Text()
		fmt.Fprintf(dst, "[%s] %s\n", tag, line)

		if logFile != nil {
			fmt.Fprintln(logFile, line)
		}
	}
}

func clearLogDir(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

func checkFile(path, kind string) {
	if _, err := os.Stat(path); err != nil {
		fatal("%s not found: %s", kind, path)
	}
}

func repoRoot() string {
	d, _ := os.Getwd()

	for {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			return d
		}

		p := filepath.Dir(d)
		if p == d {
			fatal("cannot find repo root (no go.mod found)")
		}

		d = p
	}
}

// kubectlProbeTimeout bounds a quick kubectl probe. Without it, kubectl
// waits minutes for a cluster that is gone (a stopped or deleted VM).
const kubectlProbeTimeout = 10 * time.Second

// isOpenShift reports whether the cluster has OpenShift's ClusterVersion; an
// unreachable cluster is not OpenShift.
func isOpenShift(kubeconfig string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), kubectlProbeTimeout)
	defer cancel()

	return exec.CommandContext(ctx, "kubectl", "--kubeconfig="+kubeconfig, //nolint:gosec // our envfile's kubeconfigs
		"get", "clusterversions", "--no-headers",
	).Run() == nil
}

func podNamespace(c cluster) string {
	if !c.openshift {
		return "ramen-system"
	}

	if c.hub {
		return "openshift-operators"
	}

	return "openshift-dr-system"
}

func fatal(f string, a ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", a...)
	os.Exit(1)
}
