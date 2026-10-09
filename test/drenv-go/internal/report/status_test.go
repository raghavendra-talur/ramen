// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package report_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
	"github.com/ramendr/ramen/test/drenv-go/internal/report"
)

func TestBuildStatusJSON(t *testing.T) {
	env := &envfile.Env{
		Name: "rdr",
		Profiles: []envfile.Profile{
			{Name: "dr1", Workers: []envfile.Worker{{Addons: []envfile.Addon{{Name: "rook-operator"}}}}},
			{Name: "dr2"},
			{Name: "hub", External: true},
			{Name: "bad"},
		},
		Workers: []envfile.Worker{{Addons: []envfile.Addon{{Name: "rbd-mirror", Args: []string{"dr1", "dr2"}}}}},
	}
	fp := newFakeProvider()
	fp.statuses["dr1"] = provider.StatusRunning
	fp.statuses["dr2"] = provider.StatusStopped
	fp.errs["bad"] = errors.New("boom")

	got, err := json.Marshal(report.BuildStatus(context.Background(), env, fp.selector()))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"env":"rdr","clusters":[` +
		`{"name":"dr1","state":"running","external":false,"error":""},` +
		`{"name":"dr2","state":"stopped","external":false,"error":""},` +
		`{"name":"hub","state":"not-found","external":true,"error":""},` +
		`{"name":"bad","state":"unknown","external":false,"error":"boom"}],` +
		`"profiles":[` +
		`{"name":"dr1","external":false,"workers":[{"addons":[{"name":"rook-operator","args":[]}]}]},` +
		`{"name":"dr2","external":false,"workers":[]},` +
		`{"name":"hub","external":true,"workers":[]},` +
		`{"name":"bad","external":false,"workers":[]}],` +
		`"workers":[{"addons":[{"name":"rbd-mirror","args":["dr1","dr2"]}]}]}`
	if string(got) != want {
		t.Fatalf("status JSON =\n%s\nwant\n%s", got, want)
	}
}

func TestBuildStatusEmptyEnvHasArrays(t *testing.T) {
	got, err := json.Marshal(report.BuildStatus(context.Background(), &envfile.Env{Name: "e"}, newFakeProvider().selector()))
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"env":"e","clusters":[],"profiles":[],"workers":[]}`; string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}
