// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// e2e-step drives the DR lifecycle of a single e2e test one action at a time, and reports where the test application
// is in the lifecycle between actions. Unlike running a single TestDR step with -test.run, it never deletes the
// channel, so applications keep working between steps.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/ramendr/ramen/e2e/app"
	"github.com/ramendr/ramen/e2e/config"
	"github.com/ramendr/ramen/e2e/env"
	"github.com/ramendr/ramen/e2e/stepper"
	"github.com/ramendr/ramen/e2e/types"
	"github.com/ramendr/ramen/e2e/util"
	"github.com/ramendr/ramen/e2e/validate"
)

const (
	// connectTimeout limits connecting to the clusters and detecting the distro when observing the state.
	connectTimeout = 10 * time.Second

	// channelDeleteTimeout is the time TestDR waits for deleting the channel.
	channelDeleteTimeout = 1 * time.Minute
)

const usage = `Usage: e2e-step COMMAND [flags]

Drive the DR lifecycle of a single e2e test one action at a time.

Commands:
  list [--json]                      List the configured tests (does not access the clusters)
  run --test NAME --action ACTION    Run one action for a test: %s
  state --test NAME [--json]         Report where the test application is in the DR lifecycle
  channel ensure|delete              Create or delete the test channel

Flags (accepted by all commands):
  --config FILE     e2e configuration file (default "config.yaml")
  --logfile FILE    e2e log file (default "e2e-step.log")

In --json mode stdout contains only the JSON document; logs go to stderr and
the log file. The state command exits with 1 if any lookup failed; the
document is printed anyway, with the failures in "errors".
`

type options struct {
	configFile string
	logFile    string
	json       bool
	test       string
	action     string
}

type command struct {
	// flags registers the command specific flags.
	flags func(*flag.FlagSet, *options)
	// run runs the command, returning the exit code.
	run func(*options, []string, *zap.SugaredLogger) int
}

var commands = map[string]command{
	"list":    {flags: jsonFlag, run: listCommand},
	"run":     {flags: runFlags, run: runCommand},
	"state":   {flags: stateFlags, run: stateCommand},
	"channel": {flags: func(*flag.FlagSet, *options) {}, run: channelCommand},
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		printUsage(os.Stderr)

		return 1
	}

	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		printUsage(os.Stdout)

		return 0
	}

	cmd, ok := commands[args[0]]
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown command %q\n\n", args[0])
		printUsage(os.Stderr)

		return 1
	}

	opts, positional, err := parseFlags(args[0], args[1:], cmd.flags)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}

		fmt.Fprintf(os.Stderr, "Error: %s\n", err)

		return 1
	}

	log, err := app.CreateLogger(opts.logFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create logger: %s\n", err)

		return 1
	}

	defer func() {
		_ = log.Sync() //nolint:errcheck
	}()

	log.Debugf("Using config file %q", opts.configFile)
	log.Debugf("Using log file %q", opts.logFile)

	return cmd.run(opts, positional, log)
}

func printUsage(w io.Writer) {
	fmt.Fprintf(w, usage, strings.Join(actionNames(), "|"))
}

func actionNames() []string {
	actions := stepper.Actions()

	names := make([]string, 0, len(actions))
	for _, a := range actions {
		names = append(names, string(a))
	}

	return names
}

// parseFlags parses the common and command flags. Flags may appear before and after positional arguments.
func parseFlags(name string, args []string, register func(*flag.FlagSet, *options)) (*options, []string, error) {
	opts := &options{}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.Usage = func() { printUsage(fs.Output()) }
	fs.StringVar(&opts.configFile, "config", "config.yaml", "e2e configuration file")
	fs.StringVar(&opts.logFile, "logfile", "e2e-step.log", "e2e log file")
	register(fs, opts)

	var positional []string

	for {
		if err := fs.Parse(args); err != nil {
			return nil, nil, err
		}

		if fs.NArg() == 0 {
			return opts, positional, nil
		}

		positional = append(positional, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func jsonFlag(fs *flag.FlagSet, opts *options) {
	fs.BoolVar(&opts.json, "json", false, "print a JSON document to stdout")
}

func runFlags(fs *flag.FlagSet, opts *options) {
	fs.StringVar(&opts.test, "test", "", "test name (see the list command)")
	fs.StringVar(&opts.action, "action", "", "action to run: "+strings.Join(actionNames(), "|"))
}

func stateFlags(fs *flag.FlagSet, opts *options) {
	jsonFlag(fs, opts)
	fs.StringVar(&opts.test, "test", "", "test name (see the list command)")
}

func readConfig(opts *options, log *zap.SugaredLogger) (*config.Config, bool) {
	cfg, err := app.ReadConfig(opts.configFile)
	if err != nil {
		log.Errorf("Failed to read config: %s", err)

		return nil, false
	}

	return cfg, true
}

// newContext creates the global context like TestMain, connecting to the clusters.
func newContext(ctx context.Context, cfg *config.Config, log *zap.SugaredLogger) (*app.Context, error) {
	env, err := env.New(ctx, cfg.Clusters, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create env: %w", err)
	}

	return app.NewContext(ctx, cfg, env, log), nil
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt)
}

func printJSON(v any) int {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to encode JSON: %s\n", err)

		return 1
	}

	fmt.Fprintln(os.Stdout, string(data))

	return 0
}

func listCommand(opts *options, positional []string, log *zap.SugaredLogger) int {
	if len(positional) > 0 {
		log.Errorf("Unexpected arguments: %q", positional)

		return 1
	}

	cfg, ok := readConfig(opts, log)
	if !ok {
		return 1
	}

	list := stepper.ListTests(cfg)

	if opts.json {
		return printJSON(list)
	}

	for _, t := range list.Tests {
		fmt.Fprintf(os.Stdout, "%-40s deployer=%s(%s) workload=%s pvcSpec=%s namespace=%s\n",
			t.Name, t.Deployer, t.DeployerType, t.Workload, t.PVCSpec, t.Namespace)
	}

	return 0
}

func runCommand(opts *options, positional []string, log *zap.SugaredLogger) int {
	if len(positional) > 0 || opts.test == "" || opts.action == "" {
		log.Errorf("Usage: e2e-step run --test NAME --action %s", strings.Join(actionNames(), "|"))

		return 1
	}

	action, err := stepper.ParseAction(opts.action)
	if err != nil {
		log.Error(err)

		return 1
	}

	cfg, ok := readConfig(opts, log)
	if !ok {
		return 1
	}

	// Validate the test name before accessing the clusters.
	if _, err := stepper.FindTest(cfg, opts.test); err != nil {
		log.Error(err)

		return 1
	}

	ctx, stop := signalContext()
	defer stop()

	appCtx, err := newContext(ctx, cfg, log)
	if err != nil {
		log.Error(err)

		return 1
	}

	testCtx, err := stepper.NewTestContext(appCtx, opts.test)
	if err != nil {
		log.Error(err)

		return 1
	}

	testCtx.Logger().Infof("Running action %q (timeout %v)", action, action.Timeout())

	start := time.Now()

	if err := stepper.RunAction(testCtx, action); err != nil {
		testCtx.Logger().Errorf("Action %q failed after %.3f seconds: %s", action, time.Since(start).Seconds(), err)

		return 1
	}

	testCtx.Logger().Infof("Action %q completed in %.3f seconds", action, time.Since(start).Seconds())

	return 0
}

func channelCommand(opts *options, positional []string, log *zap.SugaredLogger) int {
	if len(positional) != 1 || (positional[0] != "ensure" && positional[0] != "delete") {
		log.Error("Usage: e2e-step channel ensure|delete")

		return 1
	}

	cfg, ok := readConfig(opts, log)
	if !ok {
		return 1
	}

	ctx, stop := signalContext()
	defer stop()

	appCtx, err := newContext(ctx, cfg, log)
	if err != nil {
		log.Error(err)

		return 1
	}

	if positional[0] == "ensure" {
		err = util.EnsureChannel(appCtx)
	} else {
		timedCtx, cancel := appCtx.WithTimeout(channelDeleteTimeout)
		defer cancel()

		err = util.EnsureChannelDeleted(timedCtx)
	}

	if err != nil {
		log.Errorf("Failed to %s channel: %s", positional[0], err)

		return 1
	}

	return 0
}

func stateCommand(opts *options, positional []string, log *zap.SugaredLogger) int {
	if len(positional) > 0 || opts.test == "" {
		log.Error("Usage: e2e-step state --test NAME [--json]")

		return 1
	}

	cfg, ok := readConfig(opts, log)
	if !ok {
		return 1
	}

	ctx, stop := signalContext()
	defer stop()

	// Validate the test name before accessing the clusters.
	if _, err := stepper.FindTest(cfg, opts.test); err != nil {
		log.Error(err)

		return 1
	}

	state := observe(ctx, cfg, opts.test, log)

	var code int
	if opts.json {
		code = printJSON(state)
	} else {
		state.WriteText(os.Stdout)
	}

	if code == 0 && len(state.Errors) > 0 {
		code = 1
	}

	return code
}

// observe connects to the clusters and observes the test state. If the clusters cannot be reached in time, it
// returns an unobserved state reporting the failure.
func observe(ctx context.Context, cfg *config.Config, name string, log *zap.SugaredLogger) *stepper.State {
	env, err := connect(ctx, cfg, log)
	if err != nil {
		return unobserved(ctx, cfg, name, log, err)
	}

	testCtx, err := stepper.NewTestContext(app.NewContext(ctx, cfg, env, log), name)
	if err != nil {
		return unobserved(ctx, cfg, name, log, err)
	}

	return stepper.Observe(testCtx)
}

type connectResult struct {
	env *types.Env
	err error
}

// connect connects to the clusters and detects the distro within connectTimeout. Connecting performs API
// discovery, which does not honor the context, so we stop waiting at the deadline and leave the connection attempt
// in the background.
func connect(ctx context.Context, cfg *config.Config, log *zap.SugaredLogger) (*types.Env, error) {
	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	ch := make(chan connectResult, 1)

	go func() {
		appCtx, err := newContext(connectCtx, cfg, log)
		if err != nil {
			ch <- connectResult{err: err}

			return
		}

		if err := validate.DetectDistro(appCtx); err != nil {
			ch <- connectResult{err: fmt.Errorf("failed to detect distro: %w", err)}

			return
		}

		ch <- connectResult{env: appCtx.Env()}
	}()

	select {
	case res := <-ch:
		return res.env, res.err
	case <-connectCtx.Done():
		return nil, fmt.Errorf("failed to connect to the clusters in %v: %w", connectTimeout, connectCtx.Err())
	}
}

func unobserved(
	ctx context.Context,
	cfg *config.Config,
	name string,
	log *zap.SugaredLogger,
	cause error,
) *stepper.State {
	log.Errorf("Failed to observe test %q: %s", name, cause)

	testCtx, err := stepper.NewTestContext(app.NewContext(ctx, cfg, nil, log), name)
	if err != nil {
		// Cannot happen: the test name was validated.
		panic(err)
	}

	return stepper.Unobserved(testCtx, cause)
}
