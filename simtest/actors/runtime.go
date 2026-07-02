// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

type ClusterRef struct {
	Name string
	Cfg  *rest.Config
}

type Runtime struct {
	Store *Store
	Log   *EvLog
}

// Start launches the framework-side external actors: per managed cluster a
// manager running pvbinder + janitor + volrep, and on the hub a manager
// running the OCM work/view agents (one pair per managed cluster).
func Start(ctx context.Context, scheme *runtime.Scheme, hub ClusterRef, managed []ClusterRef, log *EvLog,
) (rt *Runtime, err error) {
	actorCtx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()

	rt = &Runtime{Store: NewStore(), Log: log}

	managedClients := map[string]client.Client{}

	for _, m := range managed {
		mgr, mgrErr := newManager(m.Cfg, scheme)
		if mgrErr != nil {
			err = fmt.Errorf("actor manager %s: %w", m.Name, mgrErr)
			return
		}

		managedClients[m.Name] = mgr.GetClient()

		if err = setupPVBinder(mgr, m.Name, rt); err != nil {
			return
		}
		if err = setupVolRep(mgr, m.Name, rt); err != nil {
			return
		}

		go runJanitor(actorCtx, mgr.GetClient(), m.Name, rt)
		go func(m manager.Manager, name string) {
			if err := m.Start(actorCtx); err != nil {
				rt.Log.Logf("actor-manager %s exited: %v", name, err)
			}
		}(mgr, m.Name)
	}

	hubMgr, hubErr := newManager(hub.Cfg, scheme)
	if hubErr != nil {
		err = fmt.Errorf("actor manager hub: %w", hubErr)
		return
	}

	for _, m := range managed {
		if err = setupOCMAgents(hubMgr, m.Name, managedClients[m.Name], rt); err != nil {
			return
		}
	}

	go func() {
		if err := hubMgr.Start(actorCtx); err != nil {
			rt.Log.Logf("actor-manager hub exited: %v", err)
		}
	}()

	return
}

func newManager(cfg *rest.Config, scheme *runtime.Scheme) (manager.Manager, error) {
	return ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
		// Tests reuse conventional cluster names ("hub", "dr1", ...) across
		// independent Test funcs in this package, each starting its own
		// manager. Controller name uniqueness is enforced via a process-global
		// registry that a stopped manager never clears, so repeat names would
		// otherwise collide the second time a test binary runs setupOCMAgents
		// et al. for the same cluster name.
		Controller: config.Controller{SkipNameValidation: ptr.To(true)},
	})
}
