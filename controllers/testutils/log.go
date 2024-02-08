// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"flag"

	"github.com/go-logr/logr"
	ginkgo "github.com/onsi/ginkgo/v2"
	uberzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func ConfigureTestLogger() logr.Logger {
	opts := ConfigureLogOptions()
	opts.DestWriter = ginkgo.GinkgoWriter

	testLogger := zap.New(zap.UseFlagOptions(&opts))
	ctrl.SetLogger(testLogger)

	return testLogger
}

func ConfigureLogOptions() zap.Options {
	opts := zap.Options{
		Development: true,
		ZapOpts: []uberzap.Option{
			uberzap.AddCaller(),
		},
		TimeEncoder: zapcore.ISO8601TimeEncoder,
	}

	opts.BindFlags(flag.CommandLine)

	return opts
}
