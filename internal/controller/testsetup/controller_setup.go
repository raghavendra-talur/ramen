// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testsetup

import (
	"context"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	config "k8s.io/component-base/config/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	ramencontrollers "github.com/ramendr/ramen/internal/controller"
	"github.com/ramendr/ramen/internal/controller/util"
)

// ControllerTestSetup extends CommonTestSetup with controller-specific setup
type ControllerTestSetup struct {
	*CommonTestSetup
	APIReader      client.Reader
	RamenConfig    *ramendrv1alpha1.RamenConfig
	RamenNamespace string
	Manager        manager.Manager
}

// SetupControllerTestEnvironment sets up the controller test environment
func SetupControllerTestEnvironment() *ControllerTestSetup {
	opts := DefaultSetupOptions("Controller Suite")
	opts.CRDDirectoryPaths = []string{
		"../config/crd/bases",
		"../hack/test",
	}
	opts.TestAssetsPath = "../../testbin/testassets.txt"
	
	commonSetup := SetupCommonTestEnvironment(opts)
	
	setup := &ControllerTestSetup{
		CommonTestSetup: commonSetup,
		RamenNamespace:  "ns-envtest",
	}
	
	// Set default controller type to DRHubType
	ramencontrollers.ControllerType = ramendrv1alpha1.DRHubType
	
	// Setup POD_NAMESPACE environment variable
	setupPodNamespace(setup.RamenNamespace)
	
	// Create operator namespace
	setup.createOperatorNamespace()
	
	// Setup RamenConfig
	setup.setupRamenConfig()
	
	// Setup manager and controllers
	setup.setupManagerAndControllers()
	
	return setup
}

// setupPodNamespace sets up the POD_NAMESPACE environment variable
func setupPodNamespace(ramenNamespace string) {
	rNs, set := os.LookupEnv("POD_NAMESPACE")
	if !set {
		Expect(os.Setenv("POD_NAMESPACE", ramenNamespace)).To(Succeed())
	} else {
		ramenNamespace = rNs
	}
}

// createOperatorNamespace creates the operator namespace if it doesn't exist
func (setup *ControllerTestSetup) createOperatorNamespace() {
	ramenNamespaceLookupKey := types.NamespacedName{Name: setup.RamenNamespace}
	ramenNamespaceObj := &corev1.Namespace{}

	err := setup.K8sClient.Get(context.TODO(), ramenNamespaceLookupKey, ramenNamespaceObj)
	if err != nil {
		setup.namespaceCreate(setup.RamenNamespace)
	}
}

// namespaceCreate creates a namespace
func (setup *ControllerTestSetup) namespaceCreate(name string) {
	Expect(setup.K8sClient.Create(context.TODO(),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})).To(Succeed())
}

// setupRamenConfig creates and configures the RamenConfig
func (setup *ControllerTestSetup) setupRamenConfig() {
	setup.RamenConfig = &ramendrv1alpha1.RamenConfig{
		TypeMeta: metav1.TypeMeta{
			Kind:       "RamenConfig",
			APIVersion: ramendrv1alpha1.GroupVersion.String(),
		},
		LeaderElection: &config.LeaderElectionConfiguration{
			LeaderElect:  new(bool),
			ResourceName: ramencontrollers.HubLeaderElectionResourceName,
		},
		RamenControllerType: ramendrv1alpha1.DRHubType,
	}
	setup.RamenConfig.DrClusterOperator.DeploymentAutomationEnabled = true
	setup.RamenConfig.DrClusterOperator.S3SecretDistributionEnabled = true
	setup.RamenConfig.MultiNamespace.FeatureEnabled = true
}

// setupManagerAndControllers sets up the controller manager and registers controllers
func (setup *ControllerTestSetup) setupManagerAndControllers() {
	// Create Velero namespace
	setup.namespaceCreate(ramencontrollers.VeleroNamespaceNameDefault)
	
	options := manager.Options{Scheme: setup.K8sClient.Scheme()}
	ramencontrollers.LoadControllerOptions(&options, setup.RamenConfig)

	var err error
	setup.Manager, err = ctrl.NewManager(setup.Cfg, options)
	Expect(err).ToNot(HaveOccurred())

	// Index fields that are required for VSHandler
	err = util.IndexFieldsForVSHandler(context.TODO(), setup.Manager.GetFieldIndexer())
	Expect(err).ToNot(HaveOccurred())

	rateLimiter := workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](10*time.Millisecond, 100*time.Millisecond),
	)

	// Setup controllers (this would be expanded based on the original suite_test.go)
	setup.setupControllers(rateLimiter)

	// Start the manager
	go func() {
		err = setup.Manager.Start(setup.Ctx)
		Expect(err).ToNot(HaveOccurred())
	}()

	// Update clients to use manager clients
	setup.K8sClient = setup.Manager.GetClient()
	Expect(setup.K8sClient).ToNot(BeNil())
	setup.APIReader = setup.Manager.GetAPIReader()
	Expect(setup.APIReader).ToNot(BeNil())
}

// setupControllers registers all the controllers with the manager
func (setup *ControllerTestSetup) setupControllers(rateLimiter workqueue.TypedRateLimiter[reconcile.Request]) {
	// This is a simplified version - the full implementation would include all controllers
	// from the original suite_test.go file
	
	// Example: DRCluster controller setup
	Expect((&ramencontrollers.DRClusterReconciler{
		Client:    setup.Manager.GetClient(),
		APIReader: setup.Manager.GetAPIReader(),
		Scheme:    setup.Manager.GetScheme(),
		Log:       ctrl.Log.WithName("controllers").WithName("DRCluster"),
		// Note: This would need the actual implementations of MCV and ObjectStore getters
		RateLimiter: &rateLimiter,
	}).SetupWithManager(setup.Manager)).To(Succeed())
	
	// Additional controllers would be added here...
}

// TeardownControllerTestEnvironment cleans up the controller test environment
func (setup *ControllerTestSetup) TeardownControllerTestEnvironment() {
	setup.TeardownCommonTestEnvironment()
}