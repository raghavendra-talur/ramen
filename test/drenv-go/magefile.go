// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

//go:build mage

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func run(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// Build compiles the drenv-go binary into bin/drenv-go.
func Build() error {
	return run("go", "build", "-o", "bin/drenv-go", "./cmd/drenv-go")
}

// Test runs the unit tests.
func Test() error {
	return run("go", "test", "./...")
}

// Lint runs gofmt and go vet.
func Lint() error {
	out, err := exec.Command("gofmt", "-l", ".").Output()
	if err != nil {
		return err
	}
	if files := strings.TrimSpace(string(out)); files != "" {
		return fmt.Errorf("gofmt found unformatted files:\n%s", files)
	}
	return run("go", "vet", "./...")
}

// Clean removes build artifacts.
func Clean() error {
	return os.RemoveAll("bin")
}
