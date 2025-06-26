// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cel_test

import (
	"testing"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/ramendr/ramen/internal/controller/testsetup"
)

const (
	timeout  = time.Second * 10
	interval = time.Millisecond * 10
)

var (
	cfg        *rest.Config
	k8sClient  client.Client
	testEnv    *envtest.Environment
	testLogger logr.Logger
)

func TestUtil(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "CEL Suite")
}

var _ = BeforeSuite(func() {
	testSetup := testsetup.SetupCommonTestEnvironment(testsetup.DefaultSetupOptions("CEL Suite"))
	
	// Extract the common setup values
	cfg = testSetup.Cfg
	k8sClient = testSetup.K8sClient
	testEnv = testSetup.TestEnv
	testLogger = testSetup.Logger
	
	// Register cleanup
	DeferCleanup(testSetup.TeardownCommonTestEnvironment)
})

// AfterSuite cleanup is handled by DeferCleanup in BeforeSuite