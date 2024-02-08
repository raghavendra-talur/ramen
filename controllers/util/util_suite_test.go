// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package util_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/ramendr/ramen/controllers/testutils"
	"github.com/ramendr/ramen/controllers/util"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
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
	var err error
	testLogger = testutils.ConfigureTestLogger()

	testEnv, cfg, _, err = testutils.ConfigureSetupEnvTest()
	Expect(err).NotTo(HaveOccurred())

	err = testutils.AddSchemes()
	Expect(err).NotTo(HaveOccurred())

	By("Creating a k8s client")
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	secretsUtil = util.SecretsUtil{
		Client:    k8sClient,
		APIReader: k8sClient,
		Ctx:       context.TODO(),
		Log:       ctrl.Log.WithName("secrets_util"),
	}
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})
