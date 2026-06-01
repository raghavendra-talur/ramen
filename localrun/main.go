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
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
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

func main() {
	root := repoRoot()

	var (
		envfilePath string
		logDir      string
		noLogs      bool
		reconcilers string
		skipBuild   bool
	)

	rootCmd := &cobra.Command{
		Use:   "localrun",
		Short: "Build and run ramen managers locally against drenv clusters",
	}

	rootCmd.PersistentFlags().StringVar(&envfilePath, "envfile",
		filepath.Join(root, "test", "envs", "regional-dr.yaml"),
		"path to drenv environment file")

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Build and start ramen managers (one process per reconciler)",
		Run: func(cmd *cobra.Command, args []string) {
			clusters := loadClusters(envfilePath)
			bin := filepath.Join(root, "localrun", "bin", "manager")

			if !skipBuild {
				build(root, bin)
			}

			resolvedLogDir := ""
			if !noLogs {
				resolvedLogDir = logDir
				if !filepath.IsAbs(resolvedLogDir) {
					resolvedLogDir = filepath.Join(root, resolvedLogDir)
				}

				os.MkdirAll(resolvedLogDir, 0o755)
			}

			for _, c := range clusters {
				scaleDownOperator(c)
			}

			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			fmt.Println("Starting ramen managers (Ctrl-C to stop)...")

			var wg sync.WaitGroup

			filter := parseReconcilerFilter(reconcilers)

			for _, c := range clusters {
				for _, rec := range reconcilersForCluster(c) {
					if len(filter) > 0 && !filter[rec] {
						continue
					}

					wg.Add(1)

					go func() {
						defer wg.Done()
						runManager(ctx, bin, c, rec, resolvedLogDir)
					}()
				}
			}

			wg.Wait()
			fmt.Println("All managers stopped.")
		},
	}

	runCmd.Flags().BoolVar(&skipBuild, "skip-build", false, "skip building the manager binary")
	runCmd.Flags().StringVar(&logDir, "log-dir", "localrun/logs", "directory for per-reconciler log files")
	runCmd.Flags().BoolVar(&noLogs, "no-logs", false, "disable log files (terminal only)")
	runCmd.Flags().StringVar(&reconcilers, "reconcilers", "", "comma-separated reconciler filter (e.g. vrg,pvrgl)")

	configureCmd := &cobra.Command{
		Use:   "configure",
		Short: "Install CRDs, create namespaces, S3 secrets, and config",
		Run: func(cmd *cobra.Command, args []string) {
			clusters := loadClusters(envfilePath)

			for _, c := range clusters {
				if c.openshift {
					fatal("configure is only supported for upstream (drenv) environments; "+
						"cluster %q is OpenShift and must be configured out-of-band", c.name)
				}
			}

			configure(root, clusters)
		},
	}

	refreshCmd := &cobra.Command{
		Use:   "refresh",
		Short: "Kill local managers, clear logs, rebuild, and restart",
		Run: func(cmd *cobra.Command, args []string) {
			clusters := loadClusters(envfilePath)
			bin := filepath.Join(root, "localrun", "bin", "manager")

			fmt.Println("Killing existing local manager processes...")
			killLocalManagers(bin)

			resolvedLogDir := ""
			if !noLogs {
				resolvedLogDir = logDir
				if !filepath.IsAbs(resolvedLogDir) {
					resolvedLogDir = filepath.Join(root, resolvedLogDir)
				}

				fmt.Printf("Clearing log directory %s...\n", resolvedLogDir)
				clearLogDir(resolvedLogDir)
			}

			if !skipBuild {
				build(root, bin)
			}

			for _, c := range clusters {
				scaleDownOperator(c)
			}

			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			fmt.Println("Starting ramen managers (Ctrl-C to stop)...")

			var wg sync.WaitGroup

			filter := parseReconcilerFilter(reconcilers)

			for _, c := range clusters {
				for _, rec := range reconcilersForCluster(c) {
					if len(filter) > 0 && !filter[rec] {
						continue
					}

					wg.Add(1)

					go func() {
						defer wg.Done()
						runManager(ctx, bin, c, rec, resolvedLogDir)
					}()
				}
			}

			wg.Wait()
			fmt.Println("All managers stopped.")
		},
	}

	refreshCmd.Flags().BoolVar(&skipBuild, "skip-build", false, "skip building the manager binary")
	refreshCmd.Flags().StringVar(&logDir, "log-dir", "localrun/logs", "directory for per-reconciler log files")
	refreshCmd.Flags().BoolVar(&noLogs, "no-logs", false, "disable log files (terminal only)")
	refreshCmd.Flags().StringVar(&reconcilers, "reconcilers", "", "comma-separated reconciler filter (e.g. vrg,pvrgl)")

	rootCmd.AddCommand(runCmd, configureCmd, refreshCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func loadClusters(envfilePath string) []cluster {
	data, err := os.ReadFile(envfilePath)
	if err != nil {
		fatal("cannot read envfile %s: %v", envfilePath, err)
	}

	var env envFile
	if err := yaml.Unmarshal(data, &env); err != nil {
		fatal("cannot parse envfile %s: %v", envfilePath, err)
	}

	if env.Name == "" {
		fatal("envfile %s: missing 'name' field", envfilePath)
	}

	if env.Ramen.Hub == "" {
		fatal("envfile %s: missing 'ramen.hub' field", envfilePath)
	}

	if len(env.Ramen.Clusters) == 0 {
		fatal("envfile %s: missing 'ramen.clusters' field", envfilePath)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		fatal("cannot get home directory: %v", err)
	}

	kcDir := filepath.Join(home, ".config", "drenv", env.Name, "kubeconfigs")

	clusters := []cluster{
		{name: env.Ramen.Hub, kubeconfig: filepath.Join(kcDir, env.Ramen.Hub), hub: true, index: 1},
	}

	for i, name := range env.Ramen.Clusters {
		clusters = append(clusters, cluster{
			name:       name,
			kubeconfig: filepath.Join(kcDir, name),
			index:      i + 2,
		})
	}

	for i, c := range clusters {
		checkFile(c.kubeconfig, "kubeconfig")

		clusters[i].openshift = isOpenShift(c.kubeconfig)
		if clusters[i].openshift {
			fmt.Printf("[%s] OpenShift cluster detected\n", c.name)
		}
	}

	return clusters
}

func reconcilersForCluster(c cluster) []string {
	if c.hub {
		return hubReconcilers
	}

	return drClusterReconcilers
}

func parseReconcilerFilter(s string) map[string]bool {
	if s == "" {
		return nil
	}

	m := make(map[string]bool)

	for _, name := range strings.Split(s, ",") {
		m[strings.TrimSpace(name)] = true
	}

	return m
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

	cmd := exec.Command("go", "build", "-o", out, "./cmd")
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		fatal("build failed: %v", err)
	}

	fmt.Println("Build complete.")
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

func runManager(ctx context.Context, bin string, c cluster, rec, logDirPath string) {
	tag := fmt.Sprintf("%s:%s", c.name, rec)

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

	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	fmt.Printf("[%s] starting (kubeconfig=%s)\n", tag, c.kubeconfig)

	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "[%s] start failed: %v\n", tag, err)
		return
	}

	var logFile *os.File

	if logDirPath != "" {
		logFileName := filepath.Join(logDirPath, fmt.Sprintf("%s-%s.log", c.name, rec))

		var err error

		logFile, err = os.Create(logFileName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[%s] cannot create log file %s: %v\n", tag, logFileName, err)
		} else {
			defer logFile.Close()
			fmt.Printf("[%s] logging to %s\n", tag, logFileName)
		}
	}

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		logLines(os.Stderr, stdout, tag, logFile)
	}()

	go func() {
		defer wg.Done()
		logLines(os.Stderr, stderr, tag, logFile)
	}()

	waitErr := cmd.Wait()
	wg.Wait()

	if ctx.Err() != nil {
		fmt.Printf("[%s] stopped\n", tag)
	} else if waitErr != nil {
		fmt.Fprintf(os.Stderr, "[%s] exited: %v\n", tag, waitErr)
	}
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

func killLocalManagers(bin string) {
	exec.Command("pkill", "-f", bin).Run() //nolint:errcheck
	time.Sleep(1 * time.Second)
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

func isOpenShift(kubeconfig string) bool {
	return exec.Command("kubectl", "--kubeconfig="+kubeconfig,
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
