// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
)

// Enabled reports whether the operator asked for the UI (SIMTEST_UI=1 or
// SIMTEST_UI=<addr>).
func Enabled() bool { return os.Getenv("SIMTEST_UI") != "" }

// Options carries everything Launch needs; the ui package never imports
// world, world passes refs in.
type Options struct {
	Addr    string // "" or "1" for ephemeral, or ":8090" / "host:port"
	Dir     string // artifacts dir for ui-events.jsonl; "" disables persist
	Scheme  *runtime.Scheme
	Hub     ClusterRef
	Managed []ClusterRef
}

// UI bundles the hub and its server for the world to own.
type UI struct {
	Hub *Hub

	srv         *Server
	stopPersist func()
	closeOnce   sync.Once
}

// Launch builds the hub, starts persistence, the server, and (when cluster
// refs are given) the watches. Errors are returned for the caller to log —
// per spec, UI failures must never fail a test.
func Launch(ctx context.Context, o Options) (*UI, error) {
	h := New()
	u := &UI{Hub: h}

	if o.Dir != "" {
		stop, err := StartPersist(h, filepath.Join(o.Dir, "ui-events.jsonl"))
		if err != nil {
			return nil, err
		}
		u.stopPersist = stop
	}

	srv, err := Serve(h, o.Addr)
	if err != nil {
		if u.stopPersist != nil {
			u.stopPersist()
		}
		return nil, err
	}
	u.srv = srv

	if o.Hub.Cfg != nil {
		if err := StartWatches(ctx, h, o.Scheme, o.Hub, o.Managed); err != nil {
			u.Close()
			return nil, err
		}
	}

	return u, nil
}

func (u *UI) URL() string { return u.srv.URL() }

func (u *UI) Close() {
	u.closeOnce.Do(func() {
		if u.srv != nil {
			u.srv.Close()
		}
		if u.stopPersist != nil {
			u.stopPersist()
		}
	})
}
