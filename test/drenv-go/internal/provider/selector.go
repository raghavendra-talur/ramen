// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

// For returns the appropriate Provider for a given profile.
//
// Selection rules:
//   - External profiles (prof.External == true) → ExternalProvider backed by k.
//   - All other profiles → MinikubeProvider backed by mk.
//
// dnsMode selects the minikube DNS behavior ("auto"/"static"/"host"; "" means
// auto) and only affects cluster creation; it is ignored for external profiles
// and for lifecycle operations that never call Start.
//
// This is the single authoritative place where the profile → provider mapping
// lives; cmd/ and build/ both delegate to this function.
func For(prof envfile.Profile, mk *cli.Minikube, k *cli.Kubectl, dnsMode string) Provider {
	if prof.External {
		return ExternalProvider{K: k}
	}
	return MinikubeProvider{MK: mk, DNSMode: dnsMode}
}
