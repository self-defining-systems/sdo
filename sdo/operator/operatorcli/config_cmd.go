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

// resolvedSettingsPath is the controller's record of the configuration it runs under, beside its other runtime
// evidence on the repository volume, where the evidence sink picks it up. The compiled child reads this record
// (ControllerConfig.settings_path), never the live mount, so the parent and every child it launches run with the
// values the parent started with.
func resolvedSettingsPath(repositoryMount string) string {
	return filepath.Join(repositoryMount, ".sdo-runtime", evidencesink.ResolvedConfigFile)
}

// publishResolvedSettings writes that record once, at the parent's start.
func publishResolvedSettings(cfg settings.Config, repositoryMount string) error {
	document, err := cfg.YAML()
	if err != nil {
		return err
	}
	path := resolvedSettingsPath(repositoryMount)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, document, 0o644)
}
