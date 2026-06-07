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
	// External marks the cluster as pre-existing and externally managed.
	// drenv-go will not create, stop, delete, suspend, or resume external
	// clusters; it only runs the addons. This mirrors the Python drenv
	// external provider (test/drenv/providers/external.py).
	External bool `yaml:"external,omitempty"`
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
	// yaml.v3 silently ignores fields absent from Env, so envfile keys the Go
	// tool does not yet model (e.g. container_runtime, extra_disks) are dropped
	// rather than rejected. Add them to the structs when the tool needs them.
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
	// CPUs == 0 is treated as unset; a profile cannot explicitly request 0 CPUs.
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
