package core

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"sdo.dev/operator/sdk"
)

const Version = "dev"

type RuntimeOptions struct {
	Args             []string
	Stdout           io.Writer
	Stderr           io.Writer
	Provider         SnapshotProvider
	Sink             Sink
	PlaybookResolver PlaybookResolver
}

func Run(detectors []sdk.Detector) {
	if err := RunWithOptions(context.Background(), detectors, RuntimeOptions{}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func RunWithOptions(ctx context.Context, detectors []sdk.Detector, options RuntimeOptions) error {
	args := options.Args
	if args == nil {
		args = os.Args[1:]
	}
	stdout := options.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	stderr := options.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}

	flags := flag.NewFlagSet("sdo-controller", flag.ContinueOnError)
	flags.SetOutput(stderr)
	namespace := flags.String("namespace", "", "namespace to observe")
	flags.String("app-root", "", "application root (accepted by the shared controller command)")
	validateDetectors := flags.Bool("validate-detectors", false, "validate detector registration and exit")
	version := flags.Bool("version", false, "print controller version and exit")
	if err := flags.Parse(args); err != nil {
		return err
	}

	if *version {
		fmt.Fprintf(stdout, "sdo-controller %s\n", Version)
		return nil
	}
	if err := ValidateDetectors(detectors); err != nil {
		return err
	}
	if *validateDetectors {
		fmt.Fprintf(stdout, "validated %d detector(s)\n", len(detectors))
		return nil
	}

	sink := options.Sink
	if sink == nil {
		sink = JSONSink{Writer: stdout}
	}
	resolver := options.PlaybookResolver
	if resolver == nil {
		resolver = logicalPlaybookIDs
	}
	sink = PlaybookResolvingSink{Inner: sink, Resolve: resolver}
	provider := options.Provider
	if provider == nil {
		kubernetesProvider, err := NewKubernetesSnapshotProvider(*namespace)
		if err != nil {
			return fmt.Errorf("create kubernetes snapshot provider: %w", err)
		}
		provider = kubernetesProvider
	}
	return Runner{
		Detectors: detectors,
		Provider:  provider,
		Sink:      sink,
	}.RunOnce(ctx)
}
