// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package util_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/ramendr/ramen/internal/controller/util"
)

func TestKubeVirtValidation(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "KubeVirt Validation Suite")
}

var _ = Describe("KubeVirt Velero Plugin Validation", func() {
	var (
		ctx       context.Context
		k8sClient client.Client
		scheme    *runtime.Scheme
		log       = zap.New(zap.UseDevMode(true))
	)

	BeforeEach(func() {
		ctx = context.Background()
		scheme = runtime.NewScheme()
		Expect(apiextensionsv1.AddToScheme(scheme)).To(Succeed())
		Expect(appsv1.AddToScheme(scheme)).To(Succeed())
		Expect(corev1.AddToScheme(scheme)).To(Succeed())
	})

	Context("when KubeVirt CRDs are not installed", func() {
		BeforeEach(func() {
			k8sClient = fake.NewClientBuilder().WithScheme(scheme).Build()
		})

		It("should return valid result with KubeVirtInstalled=false", func() {
			result := util.ValidateKubeVirtVeleroPlugin(ctx, k8sClient, k8sClient, "velero", log)

			Expect(result.KubeVirtInstalled).To(BeFalse())
			Expect(result.VeleroPluginInstalled).To(BeFalse())
			Expect(result.ValidationError).To(BeNil())
			Expect(result.IsValid()).To(BeTrue())
			Expect(result.Message).To(ContainSubstring("KubeVirt CRDs not found"))
		})
	})

	Context("when KubeVirt CRDs are installed", func() {
		var kubeVirtCRD *apiextensionsv1.CustomResourceDefinition

		BeforeEach(func() {
			kubeVirtCRD = &apiextensionsv1.CustomResourceDefinition{
				ObjectMeta: metav1.ObjectMeta{
					Name: util.VirtualMachineCRDName,
				},
				Spec: apiextensionsv1.CustomResourceDefinitionSpec{
					Group: "kubevirt.io",
					Names: apiextensionsv1.CustomResourceDefinitionNames{
						Kind:   "VirtualMachine",
						Plural: "virtualmachines",
					},
					Scope: apiextensionsv1.NamespaceScoped,
					Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
						{Name: "v1", Served: true, Storage: true},
					},
				},
			}
		})

		Context("and kubevirt-velero-plugin is installed", func() {
			BeforeEach(func() {
				veleroDeployment := &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      util.VeleroDeploymentName,
						Namespace: "velero",
					},
					Spec: appsv1.DeploymentSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "velero"},
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{"app": "velero"},
							},
							Spec: corev1.PodSpec{
								InitContainers: []corev1.Container{
									{
										Name:  "velero-plugin-for-kubevirt",
										Image: "quay.io/kubevirt/kubevirt-velero-plugin:v0.7.1",
									},
								},
								Containers: []corev1.Container{
									{
										Name:  "velero",
										Image: "velero/velero:v1.12.0",
									},
								},
							},
						},
					},
				}
				k8sClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(kubeVirtCRD, veleroDeployment).
					Build()
			})

			It("should return valid result", func() {
				result := util.ValidateKubeVirtVeleroPlugin(ctx, k8sClient, k8sClient, "velero", log)

				Expect(result.KubeVirtInstalled).To(BeTrue())
				Expect(result.VeleroPluginInstalled).To(BeTrue())
				Expect(result.ValidationError).To(BeNil())
				Expect(result.IsValid()).To(BeTrue())
				Expect(result.Message).To(ContainSubstring("properly configured"))
			})
		})

		Context("and kubevirt-velero-plugin is NOT installed", func() {
			BeforeEach(func() {
				veleroDeployment := &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      util.VeleroDeploymentName,
						Namespace: "velero",
					},
					Spec: appsv1.DeploymentSpec{
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": "velero"},
						},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{"app": "velero"},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "velero",
										Image: "velero/velero:v1.12.0",
									},
								},
							},
						},
					},
				}
				k8sClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(kubeVirtCRD, veleroDeployment).
					Build()
			})

			It("should return invalid result", func() {
				result := util.ValidateKubeVirtVeleroPlugin(ctx, k8sClient, k8sClient, "velero", log)

				Expect(result.KubeVirtInstalled).To(BeTrue())
				Expect(result.VeleroPluginInstalled).To(BeFalse())
				Expect(result.ValidationError).To(BeNil())
				Expect(result.IsValid()).To(BeFalse())
				Expect(result.Message).To(ContainSubstring("kubevirt-velero-plugin is not configured"))
			})
		})

		Context("and Velero deployment does not exist", func() {
			BeforeEach(func() {
				k8sClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(kubeVirtCRD).
					Build()
			})

			It("should return invalid result", func() {
				result := util.ValidateKubeVirtVeleroPlugin(ctx, k8sClient, k8sClient, "velero", log)

				Expect(result.KubeVirtInstalled).To(BeTrue())
				Expect(result.VeleroPluginInstalled).To(BeFalse())
				Expect(result.ValidationError).To(BeNil())
				Expect(result.IsValid()).To(BeFalse())
			})
		})
	})
})
