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
	config     string
}

func main() {
	home, err := os.UserHomeDir()
	if err != nil {
		fatal("cannot get home directory: %v", err)
	}

	kcDir := filepath.Join(home, ".config", "drenv", "rdr", "kubeconfigs")
	root := repoRoot()
	cfgDir := filepath.Join(root, "localrun", "configs")

	var (
		hubKC   = flag.String("hub-kubeconfig", filepath.Join(kcDir, "hub"), "hub kubeconfig")
		dr1KC   = flag.String("dr1-kubeconfig", filepath.Join(kcDir, "dr1"), "dr1 kubeconfig")
		dr2KC   = flag.String("dr2-kubeconfig", filepath.Join(kcDir, "dr2"), "dr2 kubeconfig")
		hubCfg  = flag.String("hub-config", filepath.Join(cfgDir, "hub.yaml"), "hub ramen config")
		dr1Cfg  = flag.String("dr1-config", filepath.Join(cfgDir, "dr1.yaml"), "dr1 ramen config")
		dr2Cfg  = flag.String("dr2-config", filepath.Join(cfgDir, "dr2.yaml"), "dr2 ramen config")
		noBuild = flag.Bool("skip-build", false, "skip building the manager binary")
	)

	flag.Parse()

	clusters := []cluster{
		{"hub", *hubKC, *hubCfg},
		{"dr1", *dr1KC, *dr1Cfg},
		{"dr2", *dr2KC, *dr2Cfg},
	}

	for _, c := range clusters {
		checkFile(c.kubeconfig, "kubeconfig")
		checkFile(c.config, "config")
	}

	// Discover minio S3 endpoints from dr1 and dr2 clusters and generate
	// config files with the real URLs substituted for placeholders.
	dr1URL := minioServiceURL("dr1")
	dr2URL := minioServiceURL("dr2")

	var tmpFiles []string

	defer func() {
		for _, f := range tmpFiles {
			os.Remove(f)
		}
	}()

	for i := range clusters {
		resolved, err := resolveConfig(clusters[i].config, dr1URL, dr2URL)
		if err != nil {
			fatal("[%s] config resolution failed: %v", clusters[i].name, err)
		}

		tmpFiles = append(tmpFiles, resolved)
		clusters[i].config = resolved
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

func resolveConfig(configPath, dr1URL, dr2URL string) (string, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return "", err
	}

	resolved := strings.ReplaceAll(string(data), "http://CHANGE_ME_DR1_IP:30000", dr1URL)
	resolved = strings.ReplaceAll(resolved, "http://CHANGE_ME_DR2_IP:30000", dr2URL)

	f, err := os.CreateTemp("", "ramen-config-*.yaml")
	if err != nil {
		return "", err
	}

	if _, err := f.WriteString(resolved); err != nil {
		f.Close()
		os.Remove(f.Name())

		return "", err
	}

	f.Close()

	return f.Name(), nil
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
		"--config="+c.config,
		"--kubeconfig="+c.kubeconfig,
	)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 10 * time.Second
	cmd.Env = append(os.Environ(), "POD_NAMESPACE=ramen-system")

	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	fmt.Printf("[%s] starting (kubeconfig=%s config=%s)\n", c.name, c.kubeconfig, c.config)

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
