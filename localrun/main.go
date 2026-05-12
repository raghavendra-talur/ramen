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

type cluster struct {
	name       string
	kubeconfig string
	hub        bool
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

	var envfilePath string

	rootCmd := &cobra.Command{
		Use:   "localrun",
		Short: "Build and run ramen managers locally against drenv clusters",
	}

	rootCmd.PersistentFlags().StringVar(&envfilePath, "envfile",
		filepath.Join(root, "test", "envs", "regional-dr.yaml"),
		"path to drenv environment file")

	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Build and start ramen managers",
		Run: func(cmd *cobra.Command, args []string) {
			clusters := loadClusters(envfilePath)
			skipBuild, _ := cmd.Flags().GetBool("skip-build")

			bin := filepath.Join(root, "localrun", "bin", "manager")

			if !skipBuild {
				build(root, bin)
			}

			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			fmt.Println("Starting ramen managers (Ctrl-C to stop)...")

			var wg sync.WaitGroup

			for _, c := range clusters {
				wg.Add(1)

				go func() {
					defer wg.Done()
					runManager(ctx, bin, c)
				}()
			}

			wg.Wait()

			fmt.Println("All managers stopped.")
		},
	}

	runCmd.Flags().Bool("skip-build", false, "skip building the manager binary")

	configureCmd := &cobra.Command{
		Use:   "configure",
		Short: "Install CRDs, create namespaces, S3 secrets, and config",
		Run: func(cmd *cobra.Command, args []string) {
			clusters := loadClusters(envfilePath)
			configure(root, clusters)
		},
	}

	rootCmd.AddCommand(runCmd, configureCmd)

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
		{env.Ramen.Hub, filepath.Join(kcDir, env.Ramen.Hub), true},
	}

	for _, name := range env.Ramen.Clusters {
		clusters = append(clusters, cluster{name, filepath.Join(kcDir, name), false})
	}

	for _, c := range clusters {
		checkFile(c.kubeconfig, "kubeconfig")
	}

	return clusters
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

	cmd := exec.Command("go", "build", "-o", out, "./cmd/main.go")
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		fatal("build failed: %v", err)
	}

	fmt.Println("Build complete.")
}

func runManager(ctx context.Context, bin string, c cluster) {
	cmd := exec.CommandContext(ctx, bin,
		"--kubeconfig="+c.kubeconfig,
	)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 10 * time.Second
	controllerType := "dr-cluster"
	if c.hub {
		controllerType = "dr-hub"
	}

	cmd.Env = append(os.Environ(),
		"POD_NAMESPACE=ramen-system",
		"RAMEN_CONTROLLER_TYPE="+controllerType,
	)

	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	fmt.Printf("[%s] starting (kubeconfig=%s)\n", c.name, c.kubeconfig)

	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "[%s] start failed: %v\n", c.name, err)
		return
	}

	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		logLines(os.Stderr, stdout, c.name)
	}()

	go func() {
		defer wg.Done()
		logLines(os.Stderr, stderr, c.name)
	}()

	waitErr := cmd.Wait()
	wg.Wait()

	if ctx.Err() != nil {
		fmt.Printf("[%s] stopped\n", c.name)
	} else if waitErr != nil {
		fmt.Fprintf(os.Stderr, "[%s] exited: %v\n", c.name, waitErr)
	}
}

func logLines(dst io.Writer, src io.Reader, tag string) {
	s := bufio.NewScanner(src)
	s.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for s.Scan() {
		fmt.Fprintf(dst, "[%s] %s\n", tag, s.Text())
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

func fatal(f string, a ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", a...)
	os.Exit(1)
}
