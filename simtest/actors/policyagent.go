// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	cfgpolicyv1 "open-cluster-management.io/config-policy-controller/api/v1"
	policyv1 "open-cluster-management.io/governance-policy-propagator/api/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// hubFromSecretRE matches the one hub-template form ramen's volsync secret
// propagator emits: {{hub fromSecret "<ns>" "<name>" "<key>" hub}}.
var hubFromSecretRE = regexp.MustCompile(`^\{\{hub fromSecret "([^"]+)" "([^"]+)" "([^"]+)" hub\}\}$`)

// policyAgent stands in for OCM's governance framework (policy propagator on
// the hub + config-policy-controller on the managed cluster): it enforces
// the Secret object-templates embedded in hub-side Policies onto one managed
// cluster, resolving {{hub fromSecret ...}} data values against the hub.
// Ramen uses this channel to distribute the VolSync pre-shared-key secret.
// When a Policy is deleted, the secrets it created are removed. Gated by
// the policy store under the PolicyAgent key.
type policyAgent struct {
	hub     client.Client
	managed client.Client
	cluster string
	rt      *Runtime

	// enforced maps a Policy's namespace/name to the secrets it created on
	// this cluster, so deletion can be mirrored (the governance framework
	// prunes enforced objects; envtest has no such controller).
	mu       sync.Mutex
	enforced map[string][]types.NamespacedName
}

func setupPolicyAgent(hubMgr manager.Manager, cluster string, managedClient client.Client, rt *Runtime) error {
	a := &policyAgent{
		hub: hubMgr.GetClient(), managed: managedClient, cluster: cluster, rt: rt,
		enforced: map[string][]types.NamespacedName{},
	}

	return ctrl.NewControllerManagedBy(hubMgr).
		For(&policyv1.Policy{}).
		Named("polagent-" + cluster).
		Complete(a)
}

func (a *policyAgent) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	d := a.rt.Store.Decide(PolicyAgent(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	pol := &policyv1.Policy{}

	err := a.hub.Get(ctx, req.NamespacedName, pol)
	if apierrors.IsNotFound(err) || (err == nil && !pol.GetDeletionTimestamp().IsZero()) {
		return ctrl.Result{}, a.prune(ctx, req.String())
	}

	if err != nil {
		return ctrl.Result{}, err
	}

	secrets, err := a.embeddedSecrets(pol)
	if err != nil {
		return ctrl.Result{}, err
	}

	created := []types.NamespacedName{}

	for _, s := range secrets {
		if err := a.enforceSecret(ctx, s); err != nil {
			return ctrl.Result{}, err
		}

		created = append(created, types.NamespacedName{Namespace: s.Metadata.Namespace, Name: s.Metadata.Name})
	}

	a.mu.Lock()
	if a.enforced == nil {
		a.enforced = map[string][]types.NamespacedName{}
	}

	a.enforced[req.String()] = created
	a.mu.Unlock()

	return ctrl.Result{}, nil
}

// prune deletes what the (now gone) Policy had enforced on this cluster.
func (a *policyAgent) prune(ctx context.Context, key string) error {
	a.mu.Lock()
	created := a.enforced[key]
	delete(a.enforced, key)
	a.mu.Unlock()

	for _, nn := range created {
		s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: nn.Namespace, Name: nn.Name}}
		if err := client.IgnoreNotFound(a.managed.Delete(ctx, s)); err != nil {
			return err
		}

		a.rt.Log.Logf("polagent@%s pruned secret %s/%s", a.cluster, nn.Namespace, nn.Name)
	}

	return nil
}

// secretTemplate is a Secret object-template as ramen's propagator emits
// it: data values are strings — either {{hub fromSecret ...}} templates or
// base64 — so it cannot be decoded as a corev1.Secret (whose data values
// must be valid base64).
type secretTemplate struct {
	Kind     string            `json:"kind"`
	Metadata metav1.ObjectMeta `json:"metadata"`
	Type     corev1.SecretType `json:"type"`
	Data     map[string]string `json:"data"`
}

// embeddedSecrets extracts the Secret object-templates from a Policy's
// embedded ConfigurationPolicies. Non-Secret templates are ignored — ramen
// only ships secrets through this channel.
func (a *policyAgent) embeddedSecrets(pol *policyv1.Policy) ([]*secretTemplate, error) {
	secrets := []*secretTemplate{}

	for _, tmpl := range pol.Spec.PolicyTemplates {
		if tmpl == nil || tmpl.ObjectDefinition.Raw == nil {
			continue
		}

		cfgPol := &cfgpolicyv1.ConfigurationPolicy{}
		if err := json.Unmarshal(tmpl.ObjectDefinition.Raw, cfgPol); err != nil {
			return nil, fmt.Errorf("decode embedded config policy: %w", err)
		}

		if cfgPol.Kind != "ConfigurationPolicy" || cfgPol.Spec == nil {
			continue
		}

		for _, ot := range cfgPol.Spec.ObjectTemplates {
			if ot == nil || ot.ObjectDefinition.Raw == nil {
				continue
			}

			s := &secretTemplate{}
			if err := json.Unmarshal(ot.ObjectDefinition.Raw, s); err != nil || s.Kind != "Secret" {
				continue
			}

			secrets = append(secrets, s)
		}
	}

	return secrets, nil
}

// enforceSecret resolves the secret's hub-template data values against the
// hub and creates the secret on the managed cluster if absent.
func (a *policyAgent) enforceSecret(ctx context.Context, tpl *secretTemplate) error {
	existing := &corev1.Secret{}

	err := a.managed.Get(ctx,
		types.NamespacedName{Namespace: tpl.Metadata.Namespace, Name: tpl.Metadata.Name}, existing)
	if err == nil || !apierrors.IsNotFound(err) {
		return err
	}

	data := map[string][]byte{}

	for key, val := range tpl.Data {
		resolved, err := a.resolveHubTemplate(ctx, key, val)
		if err != nil {
			return err
		}

		data[key] = resolved
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: tpl.Metadata.Namespace, Name: tpl.Metadata.Name, Labels: tpl.Metadata.Labels,
		},
		Type: tpl.Type,
		Data: data,
	}
	if err := a.managed.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("enforce secret %s/%s: %w", tpl.Metadata.Namespace, tpl.Metadata.Name, err)
	}

	a.rt.Log.Logf("polagent@%s enforced secret %s/%s", a.cluster, tpl.Metadata.Namespace, tpl.Metadata.Name)

	return nil
}

func (a *policyAgent) resolveHubTemplate(ctx context.Context, key, val string) ([]byte, error) {
	m := hubFromSecretRE.FindStringSubmatch(val)
	if m == nil {
		if decoded, err := base64.StdEncoding.DecodeString(val); err == nil {
			return decoded, nil
		}

		return []byte(val), nil
	}

	src := &corev1.Secret{}
	if err := a.hub.Get(ctx, types.NamespacedName{Namespace: m[1], Name: m[2]}, src); err != nil {
		return nil, fmt.Errorf("resolve hub template for %q: %w", key, err)
	}

	return src.Data[m[3]], nil
}
