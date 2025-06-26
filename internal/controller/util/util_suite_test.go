// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package util_test

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/ramendr/ramen/internal/controller/testsetup"
	"github.com/ramendr/ramen/internal/controller/util"
)

const (
	timeout  = time.Second * 10
	interval = time.Millisecond * 10
)

var (
	cfg         *rest.Config
	k8sClient   client.Client
	testEnv     *envtest.Environment
	secretsUtil util.SecretsUtil
	testLogger  logr.Logger
)

func TestUtil(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Util Suite")
}

var _ = BeforeSuite(func() {
	testSetup := testsetup.SetupCommonTestEnvironment(testsetup.DefaultSetupOptions("Util Suite"))
	
	// Extract the common setup values
	cfg = testSetup.Cfg
	k8sClient = testSetup.K8sClient
	testEnv = testSetup.TestEnv
	testLogger = testSetup.Logger

	// Setup util-specific components
	secretsUtil = util.SecretsUtil{
		Client:    k8sClient,
		APIReader: k8sClient,
		Ctx:       context.TODO(),
		Log:       ctrl.Log.WithName("secrets_util"),
	}
	
	// Register cleanup
	DeferCleanup(testSetup.TeardownCommonTestEnvironment)
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})