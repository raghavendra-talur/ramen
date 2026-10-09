// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package report_test

import (
	"context"
	"sync"

	"github.com/ramendr/ramen/test/drenv-go/internal/build"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
)

// fakeProvider reports scripted per-profile statuses (or errors); profiles not
// scripted are not-found. Lifecycle methods are no-ops. Safe for concurrent use.
type fakeProvider struct {
	mu       sync.Mutex
	statuses map[string]provider.Status
	errs     map[string]error
	calls    map[string]int
}

func newFakeProvider() *fakeProvider {
	return &fakeProvider{
		statuses: map[string]provider.Status{},
		errs:     map[string]error{},
		calls:    map[string]int{},
	}
}

func (f *fakeProvider) Status(_ context.Context, profile string) (provider.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[profile]++
	if err := f.errs[profile]; err != nil {
		return provider.StatusUnknown, err
	}
	if s, ok := f.statuses[profile]; ok {
		return s, nil
	}
	return provider.StatusNotFound, nil
}

func (f *fakeProvider) Exists(context.Context, string) (bool, error)    { return true, nil }
func (f *fakeProvider) Start(context.Context, envfile.Profile) error    { return nil }
func (f *fakeProvider) Stop(context.Context, string) error              { return nil }
func (f *fakeProvider) Delete(context.Context, string) error            { return nil }
func (f *fakeProvider) LoadImage(context.Context, string, string) error { return nil }
func (f *fakeProvider) Suspend(context.Context, string) error           { return nil }
func (f *fakeProvider) Resume(context.Context, string) error            { return nil }

func (f *fakeProvider) selector() build.ProviderSelector {
	return func(envfile.Profile) provider.Provider { return f }
}
