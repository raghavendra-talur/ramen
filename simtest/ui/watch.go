// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterRef is the minimum the UI needs to watch a cluster. Kept local so
// the ui package never imports world (world imports ui).
type ClusterRef struct {
	Name string
	Cfg  *rest.Config
}

// StartWatches wires the hub to live cluster state: DRPCs on the hub
// cluster, VRGs and PVCs on each managed cluster.
func StartWatches(ctx context.Context, h *Hub, scheme *runtime.Scheme,
	hub ClusterRef, managed []ClusterRef,
) error {
	hubClient, err := client.NewWithWatch(hub.Cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("ui watch client %s: %w", hub.Name, err)
	}
	go watchInto(ctx, h, hubClient, &rmn.DRPlacementControlList{}, extractDRPC(hub.Name))

	for _, m := range managed {
		mc, err := client.NewWithWatch(m.Cfg, client.Options{Scheme: scheme})
		if err != nil {
			return fmt.Errorf("ui watch client %s: %w", m.Name, err)
		}
		go watchInto(ctx, h, mc, &rmn.VolumeReplicationGroupList{}, extractVRG(m.Name))
		go watchInto(ctx, h, mc, &corev1.PersistentVolumeClaimList{}, extractPVC(m.Name))
	}
	return nil
}

// watchInto runs one watch loop, feeding every event's object through
// extract into the hub. It re-establishes the watch when the server closes
// it and gives up only when ctx is done.
func watchInto(ctx context.Context, h *Hub, wc client.WithWatch,
	list client.ObjectList, extract func(client.Object) (ObjectState, bool),
) {
	for ctx.Err() == nil {
		wi, err := wc.Watch(ctx, list)
		if err == nil {
			consume(ctx, h, wi, extract)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// consume drains one established watch until the server closes it or ctx is
// canceled, feeding extracted objects into the hub.
func consume(ctx context.Context, h *Hub, wi watch.Interface,
	extract func(client.Object) (ObjectState, bool),
) {
	defer wi.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-wi.ResultChan():
			if !ok {
				return
			}
			obj, isObj := ev.Object.(client.Object)
			if !isObj {
				continue
			}
			if o, k := extract(obj); k {
				if ev.Type == watch.Deleted {
					h.RemoveObject(o)
				} else {
					o.Raw = rawJSON(obj)
					h.ObserveObject(o)
				}
			}
		}
	}
}

// rawJSON renders the watched object for the drawer. Managed fields are
// pure noise at reading size, so they are stripped first; the watch decodes
// a fresh object per event, so mutating it here is safe.
func rawJSON(obj client.Object) json.RawMessage {
	obj.SetManagedFields(nil)

	b, err := json.Marshal(obj)
	if err != nil {
		return nil
	}

	return b
}

func extractDRPC(cluster string) func(client.Object) (ObjectState, bool) {
	return func(obj client.Object) (ObjectState, bool) {
		d, ok := obj.(*rmn.DRPlacementControl)
		if !ok {
			return ObjectState{}, false
		}
		fields := map[string]string{
			"phase":       string(d.Status.Phase),
			"progression": string(d.Status.Progression),
			// Action targets let the UI point at the cluster a Wait*
			// progression is actually waiting on.
			"action":           string(d.Spec.Action),
			"failoverCluster":  d.Spec.FailoverCluster,
			"preferredCluster": d.Spec.PreferredCluster,
		}
		for _, c := range d.Status.Conditions {
			fields["cond-"+c.Type] = string(c.Status)
		}
		return ObjectState{Cluster: cluster, Kind: "DRPlacementControl",
			Namespace: d.Namespace, Name: d.Name, Fields: fields}, true
	}
}

func extractVRG(cluster string) func(client.Object) (ObjectState, bool) {
	return func(obj client.Object) (ObjectState, bool) {
		v, ok := obj.(*rmn.VolumeReplicationGroup)
		if !ok {
			return ObjectState{}, false
		}
		fields := map[string]string{
			"state": strings.ToLower(string(v.Spec.ReplicationState)),
		}
		for _, c := range v.Status.Conditions {
			fields["cond-"+c.Type] = string(c.Status)
		}
		return ObjectState{Cluster: cluster, Kind: "VolumeReplicationGroup",
			Namespace: v.Namespace, Name: v.Name, Fields: fields}, true
	}
}

func extractPVC(cluster string) func(client.Object) (ObjectState, bool) {
	return func(obj client.Object) (ObjectState, bool) {
		p, ok := obj.(*corev1.PersistentVolumeClaim)
		if !ok {
			return ObjectState{}, false
		}
		return ObjectState{Cluster: cluster, Kind: "PersistentVolumeClaim",
			Namespace: p.Namespace, Name: p.Name,
			Fields: map[string]string{"phase": string(p.Status.Phase)}}, true
	}
}
