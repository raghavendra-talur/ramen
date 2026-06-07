// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"sync"
)

// Call records a single invocation of a Runner method.
type Call struct {
	Name  string
	Args  []string
	Stdin string   // non-empty only for RunStdin calls
	Env   []string // non-nil only for RunEnv calls
}

// FakeResult is a scripted (output, error) pair that FakeRunner will return
// for one call, in the order they were queued via Script.
type FakeResult struct {
	Out string
	Err error
}

// FakeRunner is a test double for Runner. It records every call in Calls and
// replays pre-scripted results in FIFO order. When the script is exhausted it
// returns zero values (empty output, nil error) so tests only need to script
// the calls they care about.
//
// Scripting scheme: call f.Script(FakeResult{...}) once per expected call to
// queue a result. Both Run and Output consume from the same queue, keyed by
// call index (not by command name), giving full per-call control. This keeps
// minikube tests straightforward: the first call gets the first result, and
// so on.
//
// FakeRunner is safe for concurrent use, so tests exercising parallel ensure
// Groups may share one instance. Note that with concurrent calls the FIFO
// script is consumed in a nondeterministic order, so per-call scripting is only
// reliable for single-threaded tests; concurrent tests should script uniform
// results (or none) and assert on Calls afterward.
type FakeRunner struct {
	mu sync.Mutex

	// Calls is the ordered record of every Run or Output invocation.
	Calls []Call

	// script is the FIFO queue of scripted results.
	script []FakeResult
}

// Script enqueues a result to be returned by the next Run or Output call.
func (f *FakeRunner) Script(r FakeResult) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.script = append(f.script, r)
}

// nextResult dequeues the next scripted result, or returns a zero value if
// the queue is empty. The caller must hold f.mu.
func (f *FakeRunner) nextResult() FakeResult {
	if len(f.script) == 0 {
		return FakeResult{}
	}
	r := f.script[0]
	f.script = f.script[1:]
	return r
}

// Run records the call and returns the next scripted error (if any).
func (f *FakeRunner) Run(_ context.Context, name string, args ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Name: name, Args: args})
	return f.nextResult().Err
}

// Output records the call and returns the next scripted (output, error) pair.
func (f *FakeRunner) Output(_ context.Context, name string, args ...string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Name: name, Args: args})
	r := f.nextResult()
	return r.Out, r.Err
}

// RunStdin records the call (including stdin) and returns the next scripted error (if any).
func (f *FakeRunner) RunStdin(_ context.Context, stdin string, name string, args ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Name: name, Args: args, Stdin: stdin})
	return f.nextResult().Err
}

// RunEnv records the call (including env) and returns the next scripted error (if any).
func (f *FakeRunner) RunEnv(_ context.Context, env []string, name string, args ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, Call{Name: name, Args: args, Env: env})
	return f.nextResult().Err
}
