// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// localrun builds and runs the ramen manager locally against 3 drenv clusters.
// Usage: go run ./localrun [flags]
package main

import (
	"bufio"
	"context"
	"flag"
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
)

type cluster struct {
	name       string
	kubeconfig string
	hub        bool
}

func main() {
	home, err := os.UserHomeDir()
	if err != nil {
		fatal("cannot get home directory: %v", err)
	}

	kcDir := filepath.Join(home, ".config", "drenv", "rdr", "kubeconfigs")
	root := repoRoot()

	var (
		hubKC       = flag.String("hub-kubeconfig", filepath.Join(kcDir, "hub"), "hub kubeconfig")
		dr1KC       = flag.String("dr1-kubeconfig", filepath.Join(kcDir, "dr1"), "dr1 kubeconfig")
		dr2KC       = flag.String("dr2-kubeconfig", filepath.Join(kcDir, "dr2"), "dr2 kubeconfig")
		noBuild     = flag.Bool("skip-build", false, "skip building the manager binary")
		doConfigure = flag.Bool("configure", false, "install CRDs, create namespaces, S3 secrets, and config, then exit")
	)

	flag.Parse()

	clusters := []cluster{
		{"hub", *hubKC, true},
		{"dr1", *dr1KC, false},
		{"dr2", *dr2KC, false},
	}

	for _, c := range clusters {
		checkFile(c.kubeconfig, "kubeconfig")
	}

	if *doConfigure {
		configure(root, clusters)
		return
	}

	bin := filepath.Join(root, "localrun", "bin", "manager")

	if !*noBuild {
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
			run(ctx, bin, c)
		}()
	}

	wg.Wait()

	fmt.Println("All managers stopped.")
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

	dr1URL := minioServiceURL("dr1")
	dr2URL := minioServiceURL("dr2")

	cfgDir := filepath.Join(root, "localrun", "configs")

	for _, c := range clusters {
		fmt.Printf("[%s] creating namespaces...\n", c.name)
		createNamespace(c.kubeconfig, "ramen-system")
		kubectl(c.kubeconfig, "apply", "-f",
			filepath.Join(root, "helper", "ramenops-ns.yaml"))

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

		fmt.Printf("[%s] creating secrets...\n", c.name)
		kubectl(c.kubeconfig, "apply", "-f",
			filepath.Join(root, "helper", "ramen-s3-secret-dr1.yaml"))
		kubectl(c.kubeconfig, "apply", "-f",
			filepath.Join(root, "helper", "ramen-s3-secret-dr2.yaml"))
		kubectl(c.kubeconfig, "apply", "-f",
			filepath.Join(root, "helper", "cloud-credentials-secret.yaml"))

		fmt.Printf("[%s] creating ramen config...\n", c.name)
		configFile := filepath.Join(cfgDir, c.name+".yaml")
		applyResolvedConfig(c.kubeconfig, configFile, dr1URL, dr2URL)
	}

	hubKC := clusters[0].kubeconfig

	fmt.Println("[hub] creating DRPolicy and DRClusters...")
	kubectl(hubKC, "apply", "-f",
		filepath.Join(root, "helper", "managedclustersetbinding.yaml"))
	kubectl(hubKC, "apply", "-f",
		filepath.Join(root, "helper", "dr-clusters.yaml"))
	kubectl(hubKC, "apply", "-f",
		filepath.Join(root, "helper", "dr-policy.yaml"))

	fmt.Println("Configuration complete.")
}

func applyResolvedConfig(kubeconfig, configPath, dr1URL, dr2URL string) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		fatal("cannot read config %s: %v", configPath, err)
	}

	resolved := strings.ReplaceAll(string(data), "http://CHANGE_ME_DR1_IP:30000", dr1URL)
	resolved = strings.ReplaceAll(resolved, "http://CHANGE_ME_DR2_IP:30000", dr2URL)

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

func kubectl(kubeconfig string, args ...string) {
	args = append([]string{"--kubeconfig=" + kubeconfig}, args...)

	cmd := exec.Command("kubectl", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		fatal("kubectl %s failed: %v", strings.Join(args, " "), err)
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

func run(ctx context.Context, bin string, c cluster) {
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

func fatal(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, f+"\n", a...)
	os.Exit(1)
}
