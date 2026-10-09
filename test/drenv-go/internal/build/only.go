// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package build

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// globalScope is the selector scope naming the global workers.
const globalScope = "global"

// Selector picks addons out of a plan. It is written NAME (every occurrence),
// NAME@PROFILE (only under that profile) or NAME@global (only in global
// workers).
type Selector struct {
	Name string
	// Scope is "" (anywhere), a profile name, or "global".
	Scope string
}

func (s Selector) String() string {
	if s.Scope == "" {
		return s.Name
	}
	return s.Name + "@" + s.Scope
}

func (s Selector) matches(a PlannedAddon) bool {
	if s.Name != a.Name {
		return false
	}
	switch s.Scope {
	case "":
		return true
	case globalScope:
		return a.Global
	default:
		return !a.Global && a.Profile == s.Scope
	}
}

// ParseSelectors parses --only values. Each value may hold several
// comma-separated selectors; empty items are ignored.
func ParseSelectors(values []string) ([]Selector, error) {
	var out []Selector
	for _, v := range values {
		for _, item := range strings.Split(v, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			name, scope, hasScope := strings.Cut(item, "@")
			if name == "" || (hasScope && scope == "") || strings.Contains(scope, "@") {
				return nil, fmt.Errorf("invalid selector %q: want NAME, NAME@PROFILE or NAME@global", item)
			}
			out = append(out, Selector{Name: name, Scope: scope})
		}
	}
	return out, nil
}

// Only returns a copy of the plan reduced to the selected addons. It keeps the
// cluster (and containerd) steps of every profile that has a selected addon or
// is named in a selected global addon's args, keeps worker grouping, order and
// original worker indices, and drops workers left empty. A selector matching
// nothing is an error that lists the valid addon names. With no selectors the
// plan is returned unchanged.
func (p *Plan) Only(sels []Selector) (*Plan, error) {
	if len(sels) == 0 {
		return p, nil
	}
	if err := p.checkSelectors(sels); err != nil {
		return nil, err
	}

	selected := func(a PlannedAddon) bool {
		return slices.ContainsFunc(sels, func(s Selector) bool { return s.matches(a) })
	}

	out := &Plan{Name: p.Name, opts: p.opts}
	out.Workers = filterWorkers(p.Workers, selected)

	// Profiles the kept global addons address must be running for them.
	needed := map[string]bool{}
	for _, w := range out.Workers {
		for _, a := range w.Addons {
			for _, arg := range a.Args {
				needed[arg] = true
			}
		}
	}

	for _, pp := range p.Profiles {
		workers := filterWorkers(pp.Workers, selected)
		if len(workers) == 0 && !needed[pp.Profile.Name] {
			continue
		}
		pp.Workers = workers
		out.Profiles = append(out.Profiles, pp)
	}
	return out, nil
}

func filterWorkers(ws []PlannedWorker, keep func(PlannedAddon) bool) []PlannedWorker {
	var out []PlannedWorker
	for _, w := range ws {
		var addons []PlannedAddon
		for _, a := range w.Addons {
			if keep(a) {
				addons = append(addons, a)
			}
		}
		if len(addons) > 0 {
			out = append(out, PlannedWorker{Index: w.Index, Addons: addons})
		}
	}
	return out
}

// checkSelectors returns an error naming every selector that matches no addon.
func (p *Plan) checkSelectors(sels []Selector) error {
	all := p.Addons()
	var bad []string
	for _, s := range sels {
		if !slices.ContainsFunc(all, s.matches) {
			bad = append(bad, fmt.Sprintf("%q", s.String()))
		}
	}
	if len(bad) == 0 {
		return nil
	}

	names := map[string]bool{}
	for _, a := range all {
		names[a.Name] = true
	}
	valid := make([]string, 0, len(names))
	for n := range names {
		valid = append(valid, n)
	}
	sort.Strings(valid)

	profiles := make([]string, len(p.Profiles))
	for i, pp := range p.Profiles {
		profiles[i] = pp.Profile.Name
	}
	return fmt.Errorf("--only %s matches no addon in env %q; valid addons: %s (scope with @PROFILE [%s] or @global)",
		strings.Join(bad, ", "), p.Name, strings.Join(valid, ", "), strings.Join(profiles, ", "))
}
