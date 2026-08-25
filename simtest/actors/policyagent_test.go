// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	cfgpolicyv1 "open-cluster-management.io/config-policy-controller/api/v1"
	policyv1 "open-cluster-management.io/governance-policy-propagator/api/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func policyScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := policyv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}

	return s
}

// newVSSecretPolicy builds a Policy the way ramen's volsync secret
// propagator does: an embedded ConfigurationPolicy whose object-template is
// a Secret with hub-template data values referencing the hub source secret.
func newVSSecretPolicy(t *testing.T) *policyv1.Policy {
	t.Helper()

	secretObjDefinition := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]interface{}{
			"name":      "bl-vs-secret",
			"namespace": "app-ns",
		},
		"type": "Opaque",
		"data": map[string]interface{}{
			"psk.txt": `{{hub fromSecret "ramen-ops" "bl-vs-secret-hub" "psk.txt" hub}}`,
		},
	}
	secretRaw, err := json.Marshal(secretObjDefinition)
	if err != nil {
		t.Fatal(err)
	}

	cfgPolicy := &cfgpolicyv1.ConfigurationPolicy{
		TypeMeta: metav1.TypeMeta{
			APIVersion: cfgpolicyv1.GroupVersion.String(),
			Kind:       "ConfigurationPolicy",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "rmn-app-ns-bl-vs-secret"},
		Spec: &cfgpolicyv1.ConfigurationPolicySpec{
			ObjectTemplates: []*cfgpolicyv1.ObjectTemplate{
				{
					ComplianceType:   cfgpolicyv1.MustHave,
					ObjectDefinition: runtime.RawExtension{Raw: secretRaw},
				},
			},
			RemediationAction: cfgpolicyv1.Enforce,
		},
	}
	cfgPolicyRaw, err := json.Marshal(cfgPolicy)
	if err != nil {
		t.Fatal(err)
	}

	return &policyv1.Policy{
		ObjectMeta: metav1.ObjectMeta{Name: "bl-vs-secret", Namespace: "ramen-ops"},
		Spec: policyv1.PolicySpec{
			Disabled: false,
			PolicyTemplates: []*policyv1.PolicyTemplate{
				{ObjectDefinition: runtime.RawExtension{Raw: cfgPolicyRaw}},
			},
		},
	}
}

// The policy agent stands in for OCM's governance framework: the hub-side
// Policy carrying an embedded ConfigurationPolicy must be enforced on the
// managed cluster, with hub-template data values resolved from the hub
// source secret; deleting the Policy removes what it created.
func TestPolicyAgentEnforcesSecret(t *testing.T) {
	hubSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "bl-vs-secret-hub", Namespace: "ramen-ops"},
		Data:       map[string][]byte{"psk.txt": []byte("the-psk")},
	}
	pol := newVSSecretPolicy(t)
	hubClient := fake.NewClientBuilder().WithScheme(policyScheme(t)).
		WithObjects(hubSecret, pol).Build()
	managedClient := fake.NewClientBuilder().WithScheme(policyScheme(t)).Build()
	a := &policyAgent{hub: hubClient, managed: managedClient, cluster: "dr1", rt: newTestRuntime(t)}
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ramen-ops", Name: "bl-vs-secret"}}

	if _, err := a.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}

	got := &corev1.Secret{}
	if err := managedClient.Get(ctx, types.NamespacedName{Namespace: "app-ns", Name: "bl-vs-secret"}, got); err != nil {
		t.Fatalf("secret not enforced on managed cluster: %v", err)
	}
	if string(got.Data["psk.txt"]) != "the-psk" {
		t.Fatalf("psk.txt = %q, want the hub secret's value", got.Data["psk.txt"])
	}

	if err := hubClient.Delete(ctx, pol); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := managedClient.Get(ctx,
		types.NamespacedName{Namespace: "app-ns", Name: "bl-vs-secret"}, got); err == nil {
		t.Fatal("secret must be removed once its Policy is gone")
	}
}

// A Silent fault against the policy agent must stall enforcement.
func TestPolicyAgentSilentPolicyStalls(t *testing.T) {
	pol := newVSSecretPolicy(t)
	hubClient := fake.NewClientBuilder().WithScheme(policyScheme(t)).WithObjects(pol).Build()
	managedClient := fake.NewClientBuilder().WithScheme(policyScheme(t)).Build()
	rt := newTestRuntime(t)
	rt.Store.Set(PolicyAgent("dr1"), Silent{})
	a := &policyAgent{hub: hubClient, managed: managedClient, cluster: "dr1", rt: rt}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "ramen-ops", Name: "bl-vs-secret"}}

	res, err := a.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("silent policy must requeue")
	}

	got := &corev1.Secret{}
	if err := managedClient.Get(context.Background(),
		types.NamespacedName{Namespace: "app-ns", Name: "bl-vs-secret"}, got); err == nil {
		t.Fatal("silent policy must not enforce the secret")
	}
}
