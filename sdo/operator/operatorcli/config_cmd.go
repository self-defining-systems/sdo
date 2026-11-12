package operatorcli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/evidencesink"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// RunConfigProcess runs `sdo-operator config` against the process environment.
func RunConfigProcess(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return RunConfig(ctx, args, processio.Environ(), stdout, stderr)
}

// RunConfig is `sdo-operator config`: `print [--config FILE]...` writes the configuration a host would resolve from
// those files and the environment, as the YAML the sdo-config ConfigMap would carry. It is the one way to see what a
// lane runs with. The document is printed even when it is incomplete (for example, no model yet); the exit status
// then reports the first problem.
func RunConfig(_ context.Context, args []string, environ []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "print" {
		fmt.Fprintln(stderr, "usage: sdo-operator config print [--config FILE]...")
		return 2
	}
	flags := flag.NewFlagSet("sdo-operator config print", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var files repeatable
	flags.Var(&files, "config", "operator configuration YAML (repeatable; later files override earlier ones)")
	if err := flags.Parse(args[1:]); err != nil {
		return 2
	}
	cfg, err := settings.Resolve(files, environ)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	document, err := cfg.YAML()
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if _, err := stdout.Write(document); err != nil {
		return 1
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

// publishResolvedSettings records the configuration the controller runs under once, beside its other runtime
// evidence, where the evidence sink picks it up. A controller started without a mounted config records nothing.
func publishResolvedSettings(mountedPath, repositoryMount string) error {
	cfg, err := loadMountedSettings(mountedPath)
	if err != nil || cfg == nil {
		return err
	}
	document, err := cfg.YAML()
	if err != nil {
		return err
	}
	directory := filepath.Join(repositoryMount, ".sdo-runtime")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(directory, evidencesink.ResolvedConfigFile), document, 0o644)
}
