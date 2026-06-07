<!--
SPDX-FileCopyrightText: The RamenDR authors
SPDX-License-Identifier: Apache-2.0
-->

# drenv-go Milestone 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `test/drenv-go/` Go module with the `ensure` model core (Step / Ensure / Group / Reporter), an envfile parser, and a read-only `drenv-go status` command that parses an envfile and prints its tree.

**Architecture:** A new, 4th Go module at `test/drenv-go/` built with mage + `tools.mod`. Everything is composed from one idempotent `Step` interface (`Done` = read-only check used as both pre-check and verify; `Do` = act). An `Ensure` runner skips satisfied steps, acts otherwise, then verifies by polling `Done`. `Group` composes steps serially or in parallel and is itself a `Step`. The envfile parser reuses the existing `test/envs/*.yaml` schema. This milestone does not touch clusters — providers and addons come in later milestones.

**Tech Stack:** Go 1.25, cobra (CLI), gopkg.in/yaml.v3 (config), golang.org/x/sync/errgroup (parallel groups), mage (build), magefile + tools.mod toolchain.

---

## File Structure

```
test/drenv-go/
  go.mod                                  # new module: github.com/ramendr/ramen/test/drenv-go
  go.sum
  tools.mod                               # pins mage (and future workflow tools)
  tools.sum
  Makefile                                # thin: delegates to mage via go tool
  magefile.go                             # Build / Test / Lint / Clean
  .gitignore                              # ignores bin/
  cmd/drenv-go/
    main.go                               # cobra root + --envfile persistent flag
    status.go                             # `status` subcommand
  internal/
    ensure/
      step.go                             # Step interface, Result
      ensure.go                           # Options, Reporter iface, Ensure, waitDone
      ensure_test.go
      group.go                            # Group (Serial/Parallel), implements Step
      group_test.go
      reporter.go                         # ConsoleReporter, nopReporter
      reporter_test.go
    envfile/
      envfile.go                          # types, Load, template expansion, Tree
      envfile_test.go
      testdata/sample.yaml                # fixture mirroring the real envfile schema
```

Root `Makefile` gains one `drenv-go` target delegating into the module. `rtalur-readme.md` parity table is updated. Nothing in the existing Python `drenv` or other modules changes.

**Shared types defined once, used across tasks (consistency reference):**
- `ensure.Step` — `Name() string`, `Done(ctx) (bool, error)`, `Do(ctx) error`
- `ensure.Result` — `Skipped`, `Changed`, `Failed`
- `ensure.Options` — `VerifyTimeout time.Duration`, `VerifyInterval time.Duration`, `Reporter Reporter`
- `ensure.DefaultOptions() Options`
- `ensure.Ensure(ctx, Step, Options) (Result, error)`
- `ensure.Reporter` — `Start(name)`, `Skipped(name, d)`, `Changed(name, d)`, `Failed(name, d, err)`
- `ensure.Group`, `ensure.Mode` (`Serial`, `Parallel`), `ensure.NewGroup(name, mode, opts, steps...) *Group`
- `envfile.Env/Ramen/Template/Profile/Worker/Addon`, `envfile.Load(path) (*Env, error)`, `envfile.Tree(*Env) string`

All Go files start with the SPDX header:
```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0
```
Imports follow gci order: standard, then third-party, then `github.com/ramendr/ramen/test/drenv-go/...`.

---

## Task 1: Scaffold the module and toolchain

**Files:**
- Create: `test/drenv-go/go.mod`
- Create: `test/drenv-go/tools.mod`
- Create: `test/drenv-go/Makefile`
- Create: `test/drenv-go/magefile.go`
- Create: `test/drenv-go/.gitignore`
- Create: `test/drenv-go/cmd/drenv-go/main.go`

- [ ] **Step 1: Create the module go.mod**

Create `test/drenv-go/go.mod`:

```
module github.com/ramendr/ramen/test/drenv-go

go 1.25
```

- [ ] **Step 2: Create the tools.mod toolchain file**

Create `test/drenv-go/tools.mod`:

```
module github.com/ramendr/ramen/test/drenv-go/tools

go 1.25
```

- [ ] **Step 3: Add mage as a pinned tool**

Run (from `test/drenv-go/`):

```bash
cd test/drenv-go
go get -modfile=tools.mod -tool github.com/magefile/mage@latest
```

Expected: `tools.mod` gains a `require github.com/magefile/mage ...` and a `tool github.com/magefile/mage` directive; `tools.sum` is created.

- [ ] **Step 4: Create the .gitignore**

Create `test/drenv-go/.gitignore`:

```
/bin/
```

- [ ] **Step 5: Create the magefile**

Create `test/drenv-go/magefile.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

//go:build mage

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func run(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// Build compiles the drenv-go binary into bin/drenv-go.
func Build() error {
	return run("go", "build", "-o", "bin/drenv-go", "./cmd/drenv-go")
}

// Test runs the unit tests.
func Test() error {
	return run("go", "test", "./...")
}

// Lint runs gofmt and go vet.
func Lint() error {
	out, err := exec.Command("gofmt", "-l", ".").Output()
	if err != nil {
		return err
	}
	if files := strings.TrimSpace(string(out)); files != "" {
		return fmt.Errorf("gofmt found unformatted files:\n%s", files)
	}
	return run("go", "vet", "./...")
}

// Clean removes build artifacts.
func Clean() error {
	return os.RemoveAll("bin")
}
```

- [ ] **Step 6: Create the thin Makefile**

Create `test/drenv-go/Makefile`:

```make
# SPDX-FileCopyrightText: The RamenDR authors
# SPDX-License-Identifier: Apache-2.0

TOOLS_MOD := tools.mod
GO_TOOL := go tool -modfile=$(TOOLS_MOD)

.PHONY: build test lint clean

build:
	$(GO_TOOL) mage build

test:
	$(GO_TOOL) mage test

lint:
	$(GO_TOOL) mage lint

clean:
	$(GO_TOOL) mage clean
```

- [ ] **Step 7: Create the cobra root command**

Create `test/drenv-go/cmd/drenv-go/main.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// envfilePath is bound to the persistent --envfile flag and consumed by subcommands.
var envfilePath string

func main() {
	root := &cobra.Command{
		Use:   "drenv-go",
		Short: "Ensure-model test environment manager (parallel Go rewrite of drenv)",
	}
	root.PersistentFlags().StringVar(&envfilePath, "envfile", "", "path to the environment file")

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

- [ ] **Step 8: Resolve dependencies**

Run (from `test/drenv-go/`):

```bash
go mod tidy
```

Expected: `go.mod`/`go.sum` gain `github.com/spf13/cobra`. No errors.

- [ ] **Step 9: Build and smoke-test**

Run (from `test/drenv-go/`):

```bash
make build && ./bin/drenv-go --help
```

Expected: build succeeds; `--help` prints usage including the `--envfile` flag.

- [ ] **Step 10: Commit**

```bash
git add test/drenv-go/go.mod test/drenv-go/go.sum test/drenv-go/tools.mod test/drenv-go/tools.sum test/drenv-go/Makefile test/drenv-go/magefile.go test/drenv-go/.gitignore test/drenv-go/cmd/drenv-go/main.go
git commit -s -m "drenv-go: scaffold module, toolchain, and cobra root

Assisted-by: Claude Code/claude-opus-4-8"
```

---

## Task 2: The Step interface and Ensure runner

**Files:**
- Create: `test/drenv-go/internal/ensure/step.go`
- Create: `test/drenv-go/internal/ensure/ensure.go`
- Test: `test/drenv-go/internal/ensure/ensure_test.go`

- [ ] **Step 1: Write the failing test**

Create `test/drenv-go/internal/ensure/ensure_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ensure

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeStep returns the values in doneSeq on successive Done calls (repeating the
// last value), and records how many times Do/Done were called.
type fakeStep struct {
	name      string
	doneSeq   []bool
	doneErr   error
	doErr     error
	doCalls   int
	doneCalls int
}

func (f *fakeStep) Name() string { return f.name }

func (f *fakeStep) Done(ctx context.Context) (bool, error) {
	i := f.doneCalls
	f.doneCalls++
	if f.doneErr != nil {
		return false, f.doneErr
	}
	switch {
	case i < len(f.doneSeq):
		return f.doneSeq[i], nil
	case len(f.doneSeq) > 0:
		return f.doneSeq[len(f.doneSeq)-1], nil
	default:
		return false, nil
	}
}

func (f *fakeStep) Do(ctx context.Context) error {
	f.doCalls++
	return f.doErr
}

func fastOpts() Options {
	return Options{VerifyTimeout: 50 * time.Millisecond, VerifyInterval: time.Millisecond}
}

func TestEnsureSkipsWhenAlreadyDone(t *testing.T) {
	s := &fakeStep{name: "x", doneSeq: []bool{true}}
	res, err := Ensure(context.Background(), s, fastOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != Skipped {
		t.Fatalf("got %v, want Skipped", res)
	}
	if s.doCalls != 0 {
		t.Fatalf("Do called %d times, want 0", s.doCalls)
	}
}

func TestEnsureActsThenVerifies(t *testing.T) {
	s := &fakeStep{name: "x", doneSeq: []bool{false, true}}
	res, err := Ensure(context.Background(), s, fastOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != Changed {
		t.Fatalf("got %v, want Changed", res)
	}
	if s.doCalls != 1 {
		t.Fatalf("Do called %d times, want 1", s.doCalls)
	}
}

func TestEnsureFailsOnDoError(t *testing.T) {
	s := &fakeStep{name: "x", doneSeq: []bool{false}, doErr: errors.New("boom")}
	res, err := Ensure(context.Background(), s, fastOpts())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if res != Failed {
		t.Fatalf("got %v, want Failed", res)
	}
}

func TestEnsureFailsOnDoneError(t *testing.T) {
	s := &fakeStep{name: "x", doneErr: errors.New("read failed")}
	res, err := Ensure(context.Background(), s, fastOpts())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if res != Failed {
		t.Fatalf("got %v, want Failed", res)
	}
}

func TestEnsureFailsOnVerifyTimeout(t *testing.T) {
	s := &fakeStep{name: "x", doneSeq: []bool{false}} // never becomes done
	res, err := Ensure(context.Background(), s, fastOpts())
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if res != Failed {
		t.Fatalf("got %v, want Failed", res)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `test/drenv-go/`):

```bash
go test ./internal/ensure/...
```

Expected: FAIL — compile errors (`Options`, `Ensure`, `Skipped`, `Changed`, `Failed` undefined).

- [ ] **Step 3: Write step.go**

Create `test/drenv-go/internal/ensure/step.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package ensure provides an idempotent "ensure" model: every unit of work is a
// Step that checks reality, acts only if reality is wrong, then verifies.
package ensure

import "context"

// Result is the outcome of ensuring a Step.
type Result int

const (
	// Skipped means the step was already satisfied; no action was taken.
	Skipped Result = iota
	// Changed means the step performed work and is now satisfied.
	Changed
	// Failed means the step could not be satisfied.
	Failed
)

func (r Result) String() string {
	switch r {
	case Skipped:
		return "skipped"
	case Changed:
		return "changed"
	case Failed:
		return "failed"
	default:
		return "unknown"
	}
}

// Step is one idempotent unit of work. Done is a cheap, read-only check used both
// as the pre-condition (skip if already true) and as verification after Do.
type Step interface {
	Name() string
	Done(ctx context.Context) (bool, error)
	Do(ctx context.Context) error
}
```

- [ ] **Step 4: Write ensure.go**

Create `test/drenv-go/internal/ensure/ensure.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ensure

import (
	"context"
	"fmt"
	"time"
)

// Reporter receives checkpoint events as steps are ensured. A nil Reporter in
// Options disables reporting.
type Reporter interface {
	Start(name string)
	Skipped(name string, d time.Duration)
	Changed(name string, d time.Duration)
	Failed(name string, d time.Duration, err error)
}

// Options control verification polling and reporting.
type Options struct {
	VerifyTimeout  time.Duration
	VerifyInterval time.Duration
	Reporter       Reporter
}

// DefaultOptions returns production-friendly defaults.
func DefaultOptions() Options {
	return Options{VerifyTimeout: 5 * time.Minute, VerifyInterval: 2 * time.Second}
}

type nopReporter struct{}

func (nopReporter) Start(string)                       {}
func (nopReporter) Skipped(string, time.Duration)      {}
func (nopReporter) Changed(string, time.Duration)      {}
func (nopReporter) Failed(string, time.Duration, error) {}

// Ensure makes the step's desired state hold: skip if already Done, otherwise Do
// and then verify by polling Done until true or timeout.
func Ensure(ctx context.Context, s Step, opts Options) (Result, error) {
	r := opts.Reporter
	if r == nil {
		r = nopReporter{}
	}
	start := time.Now()
	r.Start(s.Name())

	ok, err := s.Done(ctx)
	if err != nil {
		r.Failed(s.Name(), time.Since(start), err)
		return Failed, err
	}
	if ok {
		r.Skipped(s.Name(), time.Since(start))
		return Skipped, nil
	}

	if err := s.Do(ctx); err != nil {
		r.Failed(s.Name(), time.Since(start), err)
		return Failed, err
	}

	if err := waitDone(ctx, s, opts); err != nil {
		r.Failed(s.Name(), time.Since(start), err)
		return Failed, err
	}

	r.Changed(s.Name(), time.Since(start))
	return Changed, nil
}

// waitDone polls s.Done until it returns true, the context is cancelled, or the
// verify timeout elapses.
func waitDone(ctx context.Context, s Step, opts Options) error {
	ok, err := s.Done(ctx)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	ticker := time.NewTicker(opts.VerifyInterval)
	defer ticker.Stop()
	timeout := time.NewTimer(opts.VerifyTimeout)
	defer timeout.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timeout.C:
			return fmt.Errorf("ensure %q: timed out after %s waiting for Done", s.Name(), opts.VerifyTimeout)
		case <-ticker.C:
			ok, err := s.Done(ctx)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		}
	}
}
```

- [ ] **Step 5: Run test to verify it passes**

Run (from `test/drenv-go/`):

```bash
go test ./internal/ensure/...
```

Expected: PASS (all 5 tests).

- [ ] **Step 6: Commit**

```bash
git add test/drenv-go/internal/ensure/step.go test/drenv-go/internal/ensure/ensure.go test/drenv-go/internal/ensure/ensure_test.go
git commit -s -m "drenv-go: add ensure model Step interface and runner

Assisted-by: Claude Code/claude-opus-4-8"
```

---

## Task 3: Group composition (serial/parallel)

**Files:**
- Create: `test/drenv-go/internal/ensure/group.go`
- Test: `test/drenv-go/internal/ensure/group_test.go`

- [ ] **Step 1: Write the failing test**

Create `test/drenv-go/internal/ensure/group_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ensure

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// recordStep appends its name to rec when Do runs, then reports Done.
type recordStep struct {
	name string
	rec  *[]string
	mu   *sync.Mutex
	done bool
	doErr error
}

func (s *recordStep) Name() string { return s.name }

func (s *recordStep) Done(ctx context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done, nil
}

func (s *recordStep) Do(ctx context.Context) error {
	if s.doErr != nil {
		return s.doErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	*s.rec = append(*s.rec, s.name)
	s.done = true
	return nil
}

func testOpts() Options {
	return Options{VerifyTimeout: time.Second, VerifyInterval: time.Millisecond}
}

func TestGroupSerialRunsInOrder(t *testing.T) {
	var mu sync.Mutex
	var rec []string
	a := &recordStep{name: "a", rec: &rec, mu: &mu}
	b := &recordStep{name: "b", rec: &rec, mu: &mu}
	g := NewGroup("grp", Serial, testOpts(), a, b)

	res, err := Ensure(context.Background(), g, testOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != Changed {
		t.Fatalf("got %v, want Changed", res)
	}
	if len(rec) != 2 || rec[0] != "a" || rec[1] != "b" {
		t.Fatalf("got order %v, want [a b]", rec)
	}
}

func TestGroupParallelRunsAll(t *testing.T) {
	var mu sync.Mutex
	var rec []string
	a := &recordStep{name: "a", rec: &rec, mu: &mu}
	b := &recordStep{name: "b", rec: &rec, mu: &mu}
	g := NewGroup("grp", Parallel, testOpts(), a, b)

	if _, err := Ensure(context.Background(), g, testOpts()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rec) != 2 {
		t.Fatalf("got %d steps run, want 2", len(rec))
	}
}

func TestGroupDoneAggregates(t *testing.T) {
	var mu sync.Mutex
	var rec []string
	a := &recordStep{name: "a", rec: &rec, mu: &mu, done: true}
	b := &recordStep{name: "b", rec: &rec, mu: &mu, done: false}
	g := NewGroup("grp", Serial, testOpts(), a, b)

	ok, err := g.Done(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("group Done = true, want false (one child not done)")
	}
}

func TestGroupPropagatesError(t *testing.T) {
	var mu sync.Mutex
	var rec []string
	a := &recordStep{name: "a", rec: &rec, mu: &mu, doErr: errors.New("boom")}
	g := NewGroup("grp", Serial, testOpts(), a)

	res, err := Ensure(context.Background(), g, testOpts())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if res != Failed {
		t.Fatalf("got %v, want Failed", res)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `test/drenv-go/`):

```bash
go test ./internal/ensure/...
```

Expected: FAIL — compile errors (`NewGroup`, `Serial`, `Parallel` undefined).

- [ ] **Step 3: Write group.go**

Create `test/drenv-go/internal/ensure/group.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ensure

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// Mode controls how a Group ensures its children.
type Mode int

const (
	// Serial ensures children one at a time, in order.
	Serial Mode = iota
	// Parallel ensures children concurrently.
	Parallel
)

// Group is a Step composed of child steps. Its Done is the conjunction of its
// children's Done; its Do ensures the children under the group's Mode.
type Group struct {
	name  string
	mode  Mode
	opts  Options
	steps []Step
}

// NewGroup builds a Group. opts is used when ensuring children.
func NewGroup(name string, mode Mode, opts Options, steps ...Step) *Group {
	return &Group{name: name, mode: mode, opts: opts, steps: steps}
}

func (g *Group) Name() string { return g.name }

// Done reports whether every child is done.
func (g *Group) Done(ctx context.Context) (bool, error) {
	for _, s := range g.steps {
		ok, err := s.Done(ctx)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// Do ensures all children according to the group's Mode.
func (g *Group) Do(ctx context.Context) error {
	if g.mode == Serial {
		for _, s := range g.steps {
			if _, err := Ensure(ctx, s, g.opts); err != nil {
				return err
			}
		}
		return nil
	}

	eg, egCtx := errgroup.WithContext(ctx)
	for _, s := range g.steps {
		s := s
		eg.Go(func() error {
			_, err := Ensure(egCtx, s, g.opts)
			return err
		})
	}
	return eg.Wait()
}
```

- [ ] **Step 4: Resolve the errgroup dependency**

Run (from `test/drenv-go/`):

```bash
go mod tidy
```

Expected: `golang.org/x/sync` added to go.mod/go.sum.

- [ ] **Step 5: Run test to verify it passes**

Run (from `test/drenv-go/`):

```bash
go test ./internal/ensure/...
```

Expected: PASS (all ensure + group tests).

- [ ] **Step 6: Commit**

```bash
git add test/drenv-go/internal/ensure/group.go test/drenv-go/internal/ensure/group_test.go test/drenv-go/go.mod test/drenv-go/go.sum
git commit -s -m "drenv-go: add serial/parallel Group composition

Assisted-by: Claude Code/claude-opus-4-8"
```

---

## Task 4: Console reporter

**Files:**
- Create: `test/drenv-go/internal/ensure/reporter.go`
- Test: `test/drenv-go/internal/ensure/reporter_test.go`

- [ ] **Step 1: Write the failing test**

Create `test/drenv-go/internal/ensure/reporter_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ensure

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestConsoleReporterSkipped(t *testing.T) {
	var buf bytes.Buffer
	r := ConsoleReporter{W: &buf}
	r.Skipped("minikube/dr1", 0)
	out := buf.String()
	if !strings.Contains(out, "minikube/dr1") || !strings.Contains(out, "skipped") {
		t.Fatalf("output %q missing name or 'skipped'", out)
	}
}

func TestConsoleReporterChanged(t *testing.T) {
	var buf bytes.Buffer
	r := ConsoleReporter{W: &buf}
	r.Changed("addon/rook", 12300*time.Millisecond)
	out := buf.String()
	if !strings.Contains(out, "addon/rook") {
		t.Fatalf("output %q missing name", out)
	}
}

func TestConsoleReporterFailed(t *testing.T) {
	var buf bytes.Buffer
	r := ConsoleReporter{W: &buf}
	r.Failed("addon/velero", 0, errors.New("not ready"))
	out := buf.String()
	if !strings.Contains(out, "addon/velero") || !strings.Contains(out, "not ready") {
		t.Fatalf("output %q missing name or error", out)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `test/drenv-go/`):

```bash
go test ./internal/ensure/...
```

Expected: FAIL — `ConsoleReporter` undefined.

- [ ] **Step 3: Write reporter.go**

Create `test/drenv-go/internal/ensure/reporter.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ensure

import (
	"fmt"
	"io"
	"time"
)

// ConsoleReporter writes human-readable checkpoint lines to W.
type ConsoleReporter struct {
	W io.Writer
}

func (c ConsoleReporter) Start(name string) {}

func (c ConsoleReporter) Skipped(name string, d time.Duration) {
	fmt.Fprintf(c.W, "✓ %s (skipped, already satisfied)\n", name)
}

func (c ConsoleReporter) Changed(name string, d time.Duration) {
	fmt.Fprintf(c.W, "✓ %s (done in %.1fs)\n", name, d.Seconds())
}

func (c ConsoleReporter) Failed(name string, d time.Duration, err error) {
	fmt.Fprintf(c.W, "✗ %s: %v\n", name, err)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run (from `test/drenv-go/`):

```bash
go test ./internal/ensure/...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add test/drenv-go/internal/ensure/reporter.go test/drenv-go/internal/ensure/reporter_test.go
git commit -s -m "drenv-go: add console checkpoint reporter

Assisted-by: Claude Code/claude-opus-4-8"
```

---

## Task 5: envfile parser and template expansion

**Files:**
- Create: `test/drenv-go/internal/envfile/envfile.go`
- Create: `test/drenv-go/internal/envfile/testdata/sample.yaml`
- Test: `test/drenv-go/internal/envfile/envfile_test.go`

- [ ] **Step 1: Create the test fixture**

Create `test/drenv-go/internal/envfile/testdata/sample.yaml`:

```yaml
# SPDX-FileCopyrightText: The RamenDR authors
# SPDX-License-Identifier: Apache-2.0
---
name: sample
ramen:
  hub: hub
  clusters: [dr1, dr2]
  topology: regional-dr
templates:
  - name: dr-cluster
    driver: "$vm"
    network: "$network"
    cpus: 4
    memory: "8g"
    workers:
      - addons:
          - name: rook-operator
          - name: ocm-cluster
            args: ["$name", "hub"]
  - name: hub-cluster
    driver: "$vm"
    cpus: 2
    memory: "6g"
    workers:
      - addons:
          - name: ocm-hub
profiles:
  - name: dr1
    template: dr-cluster
  - name: dr2
    template: dr-cluster
  - name: hub
    template: hub-cluster
workers:
  - addons:
      - name: rbd-mirror
        args: ["dr1", "dr2"]
```

- [ ] **Step 2: Write the failing test**

Create `test/drenv-go/internal/envfile/envfile_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package envfile

import (
	"strings"
	"testing"
)

func TestLoadParsesEnv(t *testing.T) {
	env, err := Load("testdata/sample.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if env.Name != "sample" {
		t.Fatalf("Name = %q, want sample", env.Name)
	}
	if len(env.Profiles) != 3 {
		t.Fatalf("got %d profiles, want 3", len(env.Profiles))
	}
	if env.Ramen == nil || env.Ramen.Hub != "hub" {
		t.Fatalf("ramen.hub not parsed: %+v", env.Ramen)
	}
}

func TestLoadAppliesTemplate(t *testing.T) {
	env, err := Load("testdata/sample.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	dr1 := env.Profiles[0]
	if dr1.Name != "dr1" {
		t.Fatalf("profile[0] = %q, want dr1", dr1.Name)
	}
	if dr1.Driver != "$vm" {
		t.Fatalf("dr1.Driver = %q, want $vm (from template)", dr1.Driver)
	}
	if dr1.CPUs != 4 {
		t.Fatalf("dr1.CPUs = %d, want 4 (from template)", dr1.CPUs)
	}
	if len(dr1.Workers) != 1 || len(dr1.Workers[0].Addons) != 2 {
		t.Fatalf("dr1 workers/addons not inherited: %+v", dr1.Workers)
	}
}

func TestLoadExpandsNameArg(t *testing.T) {
	env, err := Load("testdata/sample.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// dr1's ocm-cluster addon args ["$name","hub"] should become ["dr1","hub"].
	addon := env.Profiles[0].Workers[0].Addons[1]
	if addon.Name != "ocm-cluster" {
		t.Fatalf("addon = %q, want ocm-cluster", addon.Name)
	}
	if len(addon.Args) != 2 || addon.Args[0] != "dr1" || addon.Args[1] != "hub" {
		t.Fatalf("args = %v, want [dr1 hub]", addon.Args)
	}
	// dr2 should independently expand to dr2 (no slice aliasing).
	addon2 := env.Profiles[1].Workers[0].Addons[1]
	if addon2.Args[0] != "dr2" {
		t.Fatalf("dr2 args = %v, want first elem dr2", addon2.Args)
	}
}

func TestLoadParsesGlobalWorkers(t *testing.T) {
	env, err := Load("testdata/sample.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(env.Workers) != 1 || env.Workers[0].Addons[0].Name != "rbd-mirror" {
		t.Fatalf("global workers not parsed: %+v", env.Workers)
	}
}

func TestLoadUnknownTemplateErrors(t *testing.T) {
	_, err := Load("testdata/sample.yaml")
	if err != nil {
		return // sample is valid; this guards the error path indirectly below
	}
	// Build an env referencing a missing template and expand directly.
	e := &Env{
		Profiles: []Profile{{Name: "x", Template: "nope"}},
	}
	if err := e.expand(); err == nil || !strings.Contains(err.Error(), "unknown template") {
		t.Fatalf("expected unknown template error, got %v", err)
	}
}

func TestTreeRendersStructure(t *testing.T) {
	env, err := Load("testdata/sample.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	out := Tree(env)
	if !strings.Contains(out, "profile dr1") {
		t.Fatalf("tree missing 'profile dr1':\n%s", out)
	}
	if !strings.Contains(out, "rook-operator") {
		t.Fatalf("tree missing 'rook-operator':\n%s", out)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run (from `test/drenv-go/`):

```bash
go test ./internal/envfile/...
```

Expected: FAIL — compile errors (`Env`, `Load`, `Tree`, etc. undefined).

- [ ] **Step 4: Write envfile.go**

Create `test/drenv-go/internal/envfile/envfile.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package envfile parses the drenv environment YAML (the same schema used by the
// Python drenv under test/envs) and expands profile templates.
package envfile

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Env is a parsed environment file.
type Env struct {
	Name      string     `yaml:"name"`
	Ramen     *Ramen     `yaml:"ramen,omitempty"`
	Templates []Template `yaml:"templates,omitempty"`
	Profiles  []Profile  `yaml:"profiles"`
	Workers   []Worker   `yaml:"workers,omitempty"`
}

// Ramen holds the DR topology metadata.
type Ramen struct {
	Hub      string   `yaml:"hub"`
	Clusters []string `yaml:"clusters"`
	Topology string   `yaml:"topology"`
}

// Template is a reusable base for profiles.
type Template struct {
	Name    string   `yaml:"name"`
	Driver  string   `yaml:"driver,omitempty"`
	Network string   `yaml:"network,omitempty"`
	CPUs    int      `yaml:"cpus,omitempty"`
	Memory  string   `yaml:"memory,omitempty"`
	Workers []Worker `yaml:"workers,omitempty"`
}

// Profile is a single cluster definition.
type Profile struct {
	Name     string   `yaml:"name"`
	Template string   `yaml:"template,omitempty"`
	Driver   string   `yaml:"driver,omitempty"`
	Network  string   `yaml:"network,omitempty"`
	CPUs     int      `yaml:"cpus,omitempty"`
	Memory   string   `yaml:"memory,omitempty"`
	Workers  []Worker `yaml:"workers,omitempty"`
}

// Worker is a parallel unit holding a serial list of addons.
type Worker struct {
	Addons []Addon `yaml:"addons"`
}

// Addon is a single addon invocation.
type Addon struct {
	Name string   `yaml:"name"`
	Args []string `yaml:"args,omitempty"`
}

// Load reads, parses, and expands an environment file.
func Load(path string) (*Env, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var env Env
	if err := yaml.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := env.expand(); err != nil {
		return nil, err
	}
	return &env, nil
}

// expand merges each profile's referenced template and substitutes $name in
// addon args with the profile's name.
func (e *Env) expand() error {
	tmpls := make(map[string]Template, len(e.Templates))
	for _, t := range e.Templates {
		tmpls[t.Name] = t
	}
	for i := range e.Profiles {
		p := &e.Profiles[i]
		if p.Template != "" {
			t, ok := tmpls[p.Template]
			if !ok {
				return fmt.Errorf("profile %q references unknown template %q", p.Name, p.Template)
			}
			applyTemplate(p, t)
		}
		expandArgs(p)
	}
	return nil
}

func applyTemplate(p *Profile, t Template) {
	if p.Driver == "" {
		p.Driver = t.Driver
	}
	if p.Network == "" {
		p.Network = t.Network
	}
	if p.CPUs == 0 {
		p.CPUs = t.CPUs
	}
	if p.Memory == "" {
		p.Memory = t.Memory
	}
	if len(p.Workers) == 0 {
		p.Workers = cloneWorkers(t.Workers)
	}
}

// cloneWorkers deep-copies workers so per-profile arg expansion does not alias
// the template's slices.
func cloneWorkers(ws []Worker) []Worker {
	out := make([]Worker, len(ws))
	for i, w := range ws {
		addons := make([]Addon, len(w.Addons))
		for j, a := range w.Addons {
			addons[j] = Addon{
				Name: a.Name,
				Args: append([]string(nil), a.Args...),
			}
		}
		out[i] = Worker{Addons: addons}
	}
	return out
}

func expandArgs(p *Profile) {
	for wi := range p.Workers {
		for ai := range p.Workers[wi].Addons {
			args := p.Workers[wi].Addons[ai].Args
			for k := range args {
				args[k] = strings.ReplaceAll(args[k], "$name", p.Name)
			}
		}
	}
}

// Tree renders the parsed environment as an indented text tree.
func Tree(e *Env) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n", e.Name)
	for _, p := range e.Profiles {
		fmt.Fprintf(&b, "  profile %s\n", p.Name)
		for wi, w := range p.Workers {
			fmt.Fprintf(&b, "    worker %d\n", wi)
			for _, a := range w.Addons {
				writeAddon(&b, a)
			}
		}
	}
	for wi, w := range e.Workers {
		fmt.Fprintf(&b, "  global worker %d\n", wi)
		for _, a := range w.Addons {
			writeAddon(&b, a)
		}
	}
	return b.String()
}

func writeAddon(b *strings.Builder, a Addon) {
	if len(a.Args) > 0 {
		fmt.Fprintf(b, "      addon %s %v\n", a.Name, a.Args)
	} else {
		fmt.Fprintf(b, "      addon %s\n", a.Name)
	}
}
```

- [ ] **Step 5: Resolve the yaml dependency**

Run (from `test/drenv-go/`):

```bash
go mod tidy
```

Expected: `gopkg.in/yaml.v3` added.

- [ ] **Step 6: Run test to verify it passes**

Run (from `test/drenv-go/`):

```bash
go test ./internal/envfile/...
```

Expected: PASS (all envfile tests).

- [ ] **Step 7: Verify it parses the real envfile**

Run (from `test/drenv-go/`):

```bash
go run ./cmd/drenv-go --envfile ../envs/regional-dr.yaml status 2>/dev/null || echo "status not wired yet (expected until Task 6)"
```

Expected: prints "status not wired yet" — the `status` command is added in Task 6. (This step just confirms the build still compiles.)

- [ ] **Step 8: Commit**

```bash
git add test/drenv-go/internal/envfile/envfile.go test/drenv-go/internal/envfile/envfile_test.go test/drenv-go/internal/envfile/testdata/sample.yaml test/drenv-go/go.mod test/drenv-go/go.sum
git commit -s -m "drenv-go: add envfile parser with template expansion

Assisted-by: Claude Code/claude-opus-4-8"
```

---

## Task 6: Wire the `status` command

**Files:**
- Create: `test/drenv-go/cmd/drenv-go/status.go`
- Modify: `test/drenv-go/cmd/drenv-go/main.go`

- [ ] **Step 1: Create the status subcommand**

Create `test/drenv-go/cmd/drenv-go/status.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

func newStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Parse the envfile and print its tree",
		RunE: func(cmd *cobra.Command, args []string) error {
			if envfilePath == "" {
				return fmt.Errorf("--envfile is required")
			}
			env, err := envfile.Load(envfilePath)
			if err != nil {
				return err
			}
			fmt.Print(envfile.Tree(env))
			return nil
		},
	}
}
```

- [ ] **Step 2: Register the subcommand in main.go**

In `test/drenv-go/cmd/drenv-go/main.go`, add the registration line after the persistent flag is set. The relevant section becomes:

```go
	root.PersistentFlags().StringVar(&envfilePath, "envfile", "", "path to the environment file")
	root.AddCommand(newStatusCommand())

	if err := root.Execute(); err != nil {
```

- [ ] **Step 3: Build**

Run (from `test/drenv-go/`):

```bash
make build
```

Expected: build succeeds.

- [ ] **Step 4: Run status against the real regional-dr envfile**

Run (from `test/drenv-go/`):

```bash
./bin/drenv-go --envfile ../envs/regional-dr.yaml status
```

Expected: prints the tree, including `rdr`, `profile dr1`, `profile hub`, `addon rook-operator`, and `addon ocm-cluster [dr1 hub]`.

- [ ] **Step 5: Verify the required-flag error path**

Run (from `test/drenv-go/`):

```bash
./bin/drenv-go status; echo "exit=$?"
```

Expected: prints `--envfile is required` and `exit=1`.

- [ ] **Step 6: Commit**

```bash
git add test/drenv-go/cmd/drenv-go/status.go test/drenv-go/cmd/drenv-go/main.go
git commit -s -m "drenv-go: add status command that prints the env tree

Assisted-by: Claude Code/claude-opus-4-8"
```

---

## Task 7: Root Makefile wiring and rtalur-readme update

**Files:**
- Modify: `Makefile` (repo root)
- Modify: `rtalur-readme.md` (repo root)

- [ ] **Step 1: Add the drenv-go target to the root Makefile**

In the repo-root `Makefile`, after the `e2e-rdr` target block (around line 215), add:

```make
.PHONY: drenv-go
drenv-go: ## Build the drenv-go binary (parallel Go rewrite of drenv).
	$(MAKE) -C test/drenv-go build
```

- [ ] **Step 2: Build via the root target**

Run (from the repo root):

```bash
make drenv-go
```

Expected: delegates into `test/drenv-go` and produces `test/drenv-go/bin/drenv-go`.

- [ ] **Step 3: Update the parity table in rtalur-readme.md**

In `rtalur-readme.md`, change the first parity row from:

```markdown
| Module skeleton (mage, tools.mod, cobra, ensure pkg, envfile parser) | ⬜ |
```

to:

```markdown
| Module skeleton (mage, tools.mod, cobra, ensure pkg, envfile parser) | ✅ |
```

- [ ] **Step 4: Commit**

```bash
git add Makefile rtalur-readme.md
git commit -s -m "drenv-go: wire root make target and update dev-loop notes

Assisted-by: Claude Code/claude-opus-4-8"
```

---

## Self-Review

**Spec coverage (against `docs/superpowers/specs/2026-06-07-drenv-go-design.md`):**
- Ensure model (Step/Done/Do, skip-act-verify) → Tasks 2.
- Reality-is-the-checkpoint (no state file; Done re-checked) → Task 2 (`Ensure` re-checks `Done`).
- Composition & concurrency (profiles parallel, workers parallel, addons serial; Group is a Step) → Task 3.
- Checkpoint reporting → Task 4.
- Config: reuse existing envfile YAML, template expansion → Task 5 (verified against real `regional-dr.yaml` in Task 6).
- CLI MVP `status` (read-only) → Task 6.
- Module layout: `test/drenv-go/` with mage + tools.mod → Task 1.
- Makefile wiring → Task 7.
- rtalur-readme maintained → Task 7.
- Deferred to later milestones (explicitly out of scope here): minikube provider + lifecycle, kubectl/kustomize wrappers, addons, `start`/`delete`, lima/external, `$vm`/`$network` resolution. These are named in the spec's incremental rollout and will each get their own plan.

**Placeholder scan:** No TBD/TODO/"handle edge cases" — every code step shows complete code; every run step shows the command and expected output.

**Type consistency:** `Step`, `Result` (`Skipped`/`Changed`/`Failed`), `Options{VerifyTimeout,VerifyInterval,Reporter}`, `Ensure`, `Reporter{Start,Skipped,Changed,Failed}`, `Group`/`Mode`(`Serial`/`Parallel`)/`NewGroup`, `ConsoleReporter{W}`, and `envfile.{Env,Ramen,Template,Profile,Worker,Addon,Load,Tree}` are used identically across Tasks 2–6. `envfilePath` defined in Task 1 `main.go`, consumed in Task 6 `status.go`. The `Group.Do` parallel branch reuses `Ensure` so reporting/verify behavior is uniform at every level.

**Note on `expand()` test visibility:** `TestLoadUnknownTemplateErrors` calls the unexported `expand()` directly — valid because the test is in the same package (`package envfile`).
