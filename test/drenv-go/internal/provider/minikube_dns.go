// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// DNS-bypass configuration for minikube, mirroring the Python drenv minikube
// dns and networkextension modules added in #2786.
//
// On managed Macs, corporate security agents install network extensions that
// silently drop DNS traffic from the minikube VM bridge to the host resolver.
// When one is active we point the VM at public DNS servers directly via
// `minikube start --dns-servers 8.8.8.8,1.1.1.1`, which reaches the internet via
// NAT normally. On non-VM drivers (e.g. docker) static DNS is unsupported, and
// on non-managed hosts we leave minikube's default DNS in place.

package provider

import (
	"context"
	"fmt"
	"strings"
)

// DNS modes, mirroring the --dns-mode values accepted by the Python provider.
const (
	dnsModeAuto   = "auto"
	dnsModeStatic = "static"
	dnsModeHost   = "host"
)

// dnsPublicServers are the public DNS servers used in static mode, mirroring
// SERVERS in the Python minikube dns module:
//   - 8.8.8.8: Google Public DNS
//   - 1.1.1.1: Cloudflare DNS
var dnsPublicServers = []string{"8.8.8.8", "1.1.1.1"}

// dnsVMDrivers lists the minikube drivers that run in a VM and therefore support
// static DNS mode, mirroring the is_vm check in the Python dns module.
var dnsVMDrivers = map[string]bool{"kvm2": true, "vfkit": true}

// networkExtensionCategory is the systemextensionsctl category listing network
// extensions (VPNs, content filters), mirroring CATEGORY in the Python
// networkextension module.
const networkExtensionCategory = "com.apple.system_extension.network_extension"

// dnsServers returns the cluster DNS servers for a driver and mode, mirroring
// dns.servers() in the Python minikube provider.
//
// In static mode it returns the public DNS servers (bypassing the host's DNS);
// in host mode it returns nil (use minikube's default DNS). In auto mode it
// selects static on VM drivers with a managed Mac, else host. static mode on a
// non-VM driver is unsupported and falls back to host.
func dnsServers(driver, dnsMode string, managedMac bool) ([]string, error) {
	isVM := dnsVMDrivers[driver]

	if dnsMode == dnsModeAuto {
		if isVM && managedMac {
			dnsMode = dnsModeStatic
		} else {
			dnsMode = dnsModeHost
		}
	}

	if dnsMode == dnsModeStatic && !isVM {
		// static dns mode not supported for non-VM drivers.
		dnsMode = dnsModeHost
	}

	switch dnsMode {
	case dnsModeHost:
		return nil, nil
	case dnsModeStatic:
		return append([]string(nil), dnsPublicServers...), nil
	default:
		return nil, fmt.Errorf("invalid dns_mode %q", dnsMode)
	}
}

// outputFunc matches cli.Runner.Output, letting isManagedMac be unit-tested with
// a stub instead of shelling out to systemextensionsctl.
type outputFunc func(ctx context.Context, name string, args ...string) (string, error)

// isManagedMac reports whether we run on macOS with an enabled and active
// network extension (e.g. a VPN or content filter), mirroring is_managed_mac()
// in the Python minikube dns module. On any non-Darwin OS it returns false
// without running systemextensionsctl.
func isManagedMac(ctx context.Context, goos string, output outputFunc) (bool, error) {
	if goos != "darwin" {
		return false, nil
	}
	out, err := output(ctx, "systemextensionsctl", "list", networkExtensionCategory)
	if err != nil {
		return false, fmt.Errorf("list network extensions: %w", err)
	}
	for _, ext := range parseNetworkExtensions(out) {
		if ext.enabled && ext.active {
			return true, nil
		}
	}
	return false, nil
}

// networkExtension is one row of `systemextensionsctl list`, mirroring the
// NetworkExtension namedtuple in the Python networkextension module. Only
// enabled and active are consulted by isManagedMac; the rest aid debugging.
type networkExtension struct {
	enabled  bool
	active   bool
	teamID   string
	bundleID string
	name     string
	state    string
}

// parseNetworkExtensions parses `systemextensionsctl list` output into network
// extensions, mirroring parse_output() in the Python networkextension module.
// Lines that are not tab-separated extension rows (blanks, the header, category
// counts) are skipped.
func parseNetworkExtensions(out string) []networkExtension {
	var result []networkExtension
	for line := range strings.SplitSeq(out, "\n") {
		fields := strings.Split(line, "\t")
		// Extension rows have exactly 6 tab-separated fields; skip anything else
		// and the known header line that starts with "enabled\t".
		if len(fields) != 6 || strings.HasPrefix(line, "enabled\t") {
			continue
		}

		enabled, okE := parseExtensionBool(fields[0])
		active, okA := parseExtensionBool(fields[1])
		if !okE || !okA {
			// Unknown enabled/active marker — skip rather than guess.
			continue
		}

		state := strings.TrimSpace(fields[5])
		state = strings.TrimPrefix(state, "[")
		state = strings.TrimSuffix(state, "]")

		result = append(result, networkExtension{
			enabled:  enabled,
			active:   active,
			teamID:   fields[2],
			bundleID: fields[3],
			name:     fields[4],
			state:    state,
		})
	}
	return result
}

// parseExtensionBool maps a systemextensionsctl enabled/active marker to a bool,
// mirroring _parse_bool() in the Python networkextension module: "*" is true,
// " " or "" is false, anything else is unknown (ok=false).
func parseExtensionBool(marker string) (value, ok bool) {
	switch marker {
	case "*":
		return true, true
	case " ", "":
		return false, true
	default:
		return false, false
	}
}
