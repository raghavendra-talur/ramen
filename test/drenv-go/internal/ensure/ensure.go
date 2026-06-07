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

func (nopReporter) Start(string)                        {}
func (nopReporter) Skipped(string, time.Duration)       {}
func (nopReporter) Changed(string, time.Duration)       {}
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
