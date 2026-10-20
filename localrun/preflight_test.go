// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
)

var preflightClusters = []cluster{
	{name: "hub", kubeconfig: "kc-hub", hub: true},
	{name: "dr1", kubeconfig: "kc-dr1"},
}

// fakeGetter answers from stderr by "kubeconfig resource"; anything not
// listed exists. Clusters are checked in parallel, so it records calls under
// a lock, and they are compared sorted.
func fakeGetter(fails map[string]string, calls *[]string) getter {
	var mu sync.Mutex

	return func(kubeconfig, resource string) (string, error) {
		key := kubeconfig + " " + resource

		mu.Lock()

		*calls = append(*calls, key)

		mu.Unlock()

		if stderr, ok := fails[key]; ok {
			return stderr, errors.New("exit status 1")
		}

		return "", nil
	}
}

func TestPreflightPassesWhenConfigured(t *testing.T) {
	var calls []string

	if err := preflight(preflightClusters, fakeGetter(nil, &calls)); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"kc-dr1 " + drClusterCRD,
		"kc-dr1 namespaces/ramen-system",
		"kc-hub " + hubCRD,
		"kc-hub namespaces/ramen-system",
	}

	slices.Sort(calls)

	if !slices.Equal(calls, want) {
		t.Fatalf("checked\n%s", strings.Join(calls, "\n"))
	}
}

func TestPreflightNamesWhatIsMissing(t *testing.T) {
	var calls []string

	err := preflight(preflightClusters, fakeGetter(map[string]string{
		"kc-hub namespaces/ramen-system": `Error from server (NotFound): namespaces "ramen-system" not found`,
		"kc-dr1 " + drClusterCRD:         `Error from server (NotFound): customresourcedefinitions "x" not found`,
	}, &calls))
	if err == nil {
		t.Fatal("passed")
	}

	msg := err.Error()
	for _, want := range []string{"localrun configure", "[hub] namespaces/ramen-system", "[dr1] " + drClusterCRD} {
		if !strings.Contains(msg, want) {
			t.Errorf("error lacks %q:\n%s", want, msg)
		}
	}

	if strings.Contains(msg, "cannot check") {
		t.Errorf("missing reported as unreachable:\n%s", msg)
	}
}

func TestPreflightReportsUnreachableClusters(t *testing.T) {
	var calls []string

	err := preflight(preflightClusters, fakeGetter(map[string]string{
		"kc-hub " + hubCRD:               "",
		"kc-dr1 namespaces/ramen-system": "Unable to connect to the server: dial tcp 192.168.64.3:8443: i/o timeout\nmore",
	}, &calls))
	if err == nil {
		t.Fatal("passed")
	}

	msg := err.Error()
	if strings.Contains(msg, "localrun configure") {
		t.Errorf("unreachable reported as not configured:\n%s", msg)
	}

	for _, want := range []string{
		"[hub] cannot check " + hubCRD + ": exit status 1",
		"[dr1] cannot check namespaces/ramen-system: Unable to connect to the server: dial tcp 192.168.64.3:8443: " +
			"i/o timeout",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error lacks %q:\n%s", want, msg)
		}
	}

	if strings.Contains(msg, "more") {
		t.Errorf("more than the first line of stderr:\n%s", msg)
	}

	// The error lists clusters in envfile order, whatever order they answered.
	if strings.Index(msg, "[hub]") > strings.Index(msg, "[dr1]") {
		t.Errorf("clusters out of order:\n%s", msg)
	}

	// An unreachable cluster is not asked again: the next check would wait
	// as long.
	if slices.Contains(calls, "kc-dr1 "+drClusterCRD) {
		t.Errorf("checked an unreachable cluster again: %q", calls)
	}
}

func TestPreflightUsesOpenShiftNamespaces(t *testing.T) {
	var calls []string

	clusters := []cluster{{name: "hub", kubeconfig: "kc", hub: true, openshift: true}}
	if err := preflight(clusters, fakeGetter(nil, &calls)); err != nil {
		t.Fatal(err)
	}

	if calls[0] != "kc namespaces/openshift-operators" {
		t.Fatalf("checked %q", calls[0])
	}
}
