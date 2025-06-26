// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testsetup

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	// Common API imports
	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	groupsnapv1beta1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1beta1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	Recipe "github.com/ramendr/recipe/api/v1alpha1"
	plrv1 "github.com/stolostron/multicloud-operators-placementrule/pkg/apis/apps/v1"
	velero "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	ocmclv1 "open-cluster-management.io/api/cluster/v1"
	clusterv1alpha1 "open-cluster-management.io/api/cluster/v1alpha1"
	clrapiv1beta1 "open-cluster-management.io/api/cluster/v1beta1"
	ocmworkv1 "open-cluster-management.io/api/work/v1"
	cpcv1 "open-cluster-management.io/config-policy-controller/api/v1"
	gppv1 "open-cluster-management.io/governance-policy-propagator/api/v1"
	viewv1beta1 "open-cluster-management.io/multicloud-operators-subscription/pkg/apis/view/v1beta1"

	argocdv1alpha1hack "github.com/ramendr/ramen/internal/controller/argocd"
)

// CommonTestSetup holds the common test environment setup
type CommonTestSetup struct {
	Cfg       *rest.Config
	K8sClient client.Client
	TestEnv   *envtest.Environment
	Logger    logr.Logger
	Ctx       context.Context
	Cancel    context.CancelFunc
}

// SetupOptions allows customization of the test setup
type SetupOptions struct {
	SuiteName           string
	CRDDirectoryPaths   []string
	TestAssetsPath      string
	SkipManagerStart    bool
	AdditionalSchemes   []func(*runtime.Scheme) error
}

// DefaultSetupOptions returns sensible defaults for test setup
func DefaultSetupOptions(suiteName string) *SetupOptions {
	return &SetupOptions{
		SuiteName: suiteName,
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "..", "config", "crd", "bases"),
			filepath.Join("..", "..", "..", "hack", "test"),
		},
		TestAssetsPath: "../../../testbin/testassets.txt",
		SkipManagerStart: false,
	}
}

// SetupCommonTestEnvironment sets up the common test environment
func SetupCommonTestEnvironment(opts *SetupOptions) *CommonTestSetup {
	setup := &CommonTestSetup{}
	
	// Setup context
	setup.Ctx, setup.Cancel = context.WithCancel(context.TODO())
	
	// Setup Gomega output formatting
	format.MaxLength = 0
	
	// Setup logger
	setup.Logger = zap.New(zap.UseFlagOptions(&zap.Options{
		Development: true,
		DestWriter:  GinkgoWriter,
		TimeEncoder: zapcore.ISO8601TimeEncoder,
	}))
	logf.SetLogger(setup.Logger)
	
	testLog := ctrl.Log.WithName("tester")
	testLog.Info("Starting test suite", "suite", opts.SuiteName, "time", time.Now())

	// Setup KUBEBUILDER_ASSETS
	setupKubebuilderAssets(opts.TestAssetsPath, testLog)

	// Bootstrap test environment
	By("Bootstrapping test environment")
	setup.TestEnv = &envtest.Environment{
		CRDDirectoryPaths: opts.CRDDirectoryPaths,
	}

	var err error
	setup.Cfg, err = setup.TestEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(setup.Cfg).NotTo(BeNil())

	// Register common schemes
	registerCommonSchemes()
	
	// Register additional schemes if provided
	if opts.AdditionalSchemes != nil {
		for _, addScheme := range opts.AdditionalSchemes {
			err = addScheme(scheme.Scheme)
			Expect(err).NotTo(HaveOccurred())
		}
	}

	// Create k8s client
	setup.K8sClient, err = client.New(setup.Cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(setup.K8sClient).NotTo(BeNil())

	return setup
}

// TeardownCommonTestEnvironment cleans up the test environment
func (setup *CommonTestSetup) TeardownCommonTestEnvironment() {
	By("Tearing down the test environment")
	if setup.Cancel != nil {
		setup.Cancel()
	}
	if setup.TestEnv != nil {
		err := setup.TestEnv.Stop()
		Expect(err).NotTo(HaveOccurred())
	}
}

// setupKubebuilderAssets sets up KUBEBUILDER_ASSETS environment variable
func setupKubebuilderAssets(testAssetsPath string, testLog logr.Logger) {
	By("Setting up KUBEBUILDER_ASSETS for envtest")
	if _, set := os.LookupEnv("KUBEBUILDER_ASSETS"); !set {
		testLog.Info("Setting up KUBEBUILDER_ASSETS for envtest")
		
		content, err := os.ReadFile(testAssetsPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(os.Setenv("KUBEBUILDER_ASSETS", string(content))).To(Succeed())
	}
}

// registerCommonSchemes registers all commonly used schemes
func registerCommonSchemes() {
	By("Setting up required schemes in envtest")
	
	schemes := []func(*runtime.Scheme) error{
		ocmworkv1.AddToScheme,
		ocmclv1.AddToScheme,
		plrv1.AddToScheme,
		viewv1beta1.AddToScheme,
		cpcv1.AddToScheme,
		gppv1.AddToScheme,
		ramendrv1alpha1.AddToScheme,
		Recipe.AddToScheme,
		volrep.AddToScheme,
		volsyncv1alpha1.AddToScheme,
		snapv1.AddToScheme,
		velero.AddToScheme,
		clrapiv1beta1.AddToScheme,
		clusterv1alpha1.AddToScheme,
		argocdv1alpha1hack.AddToScheme,
		apiextensions.AddToScheme,
		groupsnapv1beta1.AddToScheme,
	}
	
	for _, addScheme := range schemes {
		err := addScheme(scheme.Scheme)
		Expect(err).NotTo(HaveOccurred())
	}
}