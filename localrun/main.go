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
