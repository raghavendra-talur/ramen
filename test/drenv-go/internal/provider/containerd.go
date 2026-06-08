// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package provider

// Per-node containerd configuration, mirroring Python drenv's
// _configure_containerd: after a minikube cluster starts, merge the profile's
// `containerd:` block into the node's /etc/containerd/config.toml and restart
// containerd. This is what rook needs for
// device_ownership_from_security_context.
//
// Scope note: the Python helper also merges a registry-mirror block
// (max_concurrent_downloads, registry config_path → /etc/containerd/certs.d)
// that belongs to drenv's local registry cache. drenv-go does not run that
// cache (see rtalur-readme.md), so only the profile's own containerd config is
// applied here.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	toml "github.com/pelletier/go-toml/v2"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

const containerdConfigPath = "/etc/containerd/config.toml"

// ContainerdConfigStep returns an ensure.Step that merges the profile's
// containerd config into the node and restarts containerd. Done reports true
// when the config is already present (so re-runs are cheap and do not restart a
// healthy containerd); Do performs the merge + restart.
func ContainerdConfigStep(mk *cli.Minikube, p envfile.Profile) ensure.Step {
	return containerdStep{mk: mk, p: p}
}

type containerdStep struct {
	mk *cli.Minikube
	p  envfile.Profile
}

func (s containerdStep) Name() string { return "containerd-config/" + s.p.Name }

func (s containerdStep) Done(ctx context.Context) (bool, error) {
	if len(s.p.Containerd) == 0 {
		return true, nil
	}
	existing, overlay, err := s.readConfigs(ctx)
	if err != nil {
		return false, err
	}
	return containerdConfigSatisfied(existing, overlay), nil
}

// containerdConfigSatisfied reports whether overlay is already fully present in
// existing, i.e. merging it would change nothing.
func containerdConfigSatisfied(existing, overlay map[string]any) bool {
	return reflect.DeepEqual(mergeContainerdConfig(existing, overlay), existing)
}

func (s containerdStep) Do(ctx context.Context) error {
	if len(s.p.Containerd) == 0 {
		return nil
	}
	existing, overlay, err := s.readConfigs(ctx)
	if err != nil {
		return err
	}
	merged := mergeContainerdConfig(existing, overlay)

	out, err := toml.Marshal(merged)
	if err != nil {
		return fmt.Errorf("containerd: marshal merged config: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "drenv-containerd-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)
	local := filepath.Join(tmpDir, "config.toml")
	if err := os.WriteFile(local, out, 0o644); err != nil {
		return fmt.Errorf("containerd: write merged config: %w", err)
	}

	remote := s.p.Name + ":" + containerdConfigPath
	if err := s.mk.Cp(ctx, s.p.Name, local, remote); err != nil {
		return fmt.Errorf("containerd: copy config to %s: %w", s.p.Name, err)
	}
	if err := s.mk.SSH(ctx, s.p.Name, "sudo systemctl restart containerd"); err != nil {
		return fmt.Errorf("containerd: restart on %s: %w", s.p.Name, err)
	}
	return nil
}

// readConfigs copies the node's containerd config.toml out and returns it parsed
// alongside the profile's overlay normalized through TOML (so numeric and other
// types match the parsed config, making the DeepEqual idempotency check reliable).
func (s containerdStep) readConfigs(ctx context.Context) (existing, overlay map[string]any, err error) {
	tmpDir, err := os.MkdirTemp("", "drenv-containerd-")
	if err != nil {
		return nil, nil, err
	}
	defer os.RemoveAll(tmpDir)
	local := filepath.Join(tmpDir, "config.toml")

	remote := s.p.Name + ":" + containerdConfigPath
	if err := s.mk.Cp(ctx, s.p.Name, remote, local); err != nil {
		return nil, nil, fmt.Errorf("containerd: copy config from %s: %w", s.p.Name, err)
	}
	data, err := os.ReadFile(local)
	if err != nil {
		return nil, nil, fmt.Errorf("containerd: read config from %s: %w", s.p.Name, err)
	}
	existing = map[string]any{}
	if err := toml.Unmarshal(data, &existing); err != nil {
		return nil, nil, fmt.Errorf("containerd: parse config from %s: %w", s.p.Name, err)
	}
	overlay, err = normalizeViaTOML(s.p.Containerd)
	if err != nil {
		return nil, nil, fmt.Errorf("containerd: normalize overlay: %w", err)
	}
	return existing, overlay, nil
}

// normalizeViaTOML round-trips a map through TOML so its scalar types (e.g.
// int64, bool) match those produced by parsing the node's config.toml.
func normalizeViaTOML(m map[string]any) (map[string]any, error) {
	b, err := toml.Marshal(m)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if err := toml.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// mergeContainerdConfig recursively merges overlay into base (overlay wins),
// returning a new map. Mirrors patch.merge in the Python drenv.
func mergeContainerdConfig(base, overlay map[string]any) map[string]any {
	out := make(map[string]any, len(base))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range overlay {
		if existing, ok := out[k]; ok {
			em, eok := existing.(map[string]any)
			om, ook := v.(map[string]any)
			if eok && ook {
				out[k] = mergeContainerdConfig(em, om)
				continue
			}
		}
		out[k] = v
	}
	return out
}
