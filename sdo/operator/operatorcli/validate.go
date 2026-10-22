package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk/sdktest"
)

func ParseValidateLaunch(args []string) (builder.Config, error) {
	if len(args) == 0 || args[0] != "validate" {
		return builder.Config{}, errors.New("expected validate subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator validate", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	app := flags.String("app", "", "candidate application checkout")
	operatorRoot := flags.String("operator-root", "/opt/sdo/sdo/operator", "baked operator sources")
	expected := flags.String("expect-schema", "", "accepted validator schema identity")
	healthyBaseline := flags.String("healthy-baseline", "", "recorded healthy snapshots for incident detector validation")
	if err := flags.Parse(args[1:]); err != nil {
		return builder.Config{}, err
	}
	if flags.NArg() != 0 || strings.TrimSpace(*app) == "" || strings.TrimSpace(*expected) == "" {
		return builder.Config{}, errors.New("validator requires application checkout and expected schema identity")
	}
	if !filepath.IsAbs(*app) || !filepath.IsAbs(*operatorRoot) {
		return builder.Config{}, errors.New("validator paths must be absolute")
	}
	if *healthyBaseline != "" && !filepath.IsAbs(*healthyBaseline) {
		return builder.Config{}, errors.New("healthy baseline path must be absolute")
	}
	actual, err := builder.SchemaIdentity(*operatorRoot)
	if err != nil {
		return builder.Config{}, fmt.Errorf("cannot verify validator schema identity: %w", err)
	}
	if actual != *expected {
		return builder.Config{}, fmt.Errorf("stale validator image: it embeds schema %q but the caller expects %q", actual, *expected)
	}
	return builder.Config{ApplicationRoot: *app, OperatorRoot: *operatorRoot, HealthyBaseline: *healthyBaseline}, nil
}

// RunValidate is the validation Job's body: the shared validator's compile
// layer over the staged candidate. The broker has already run the read-only
// layers in its own process.
func RunValidate(ctx context.Context, config builder.Config) error {
	return RunValidateReport(ctx, config, io.Discard)
}

// RunValidateReport carries content failures across the isolated Job boundary.
func RunValidateReport(ctx context.Context, config builder.Config, out io.Writer) error {
	var notes []sdktest.MitigationReplayResult
	diagnostics, err := validate.CompileCandidate(ctx, validate.Request{
		MitigationReplayNotes: &notes,
		Root:                  config.ApplicationRoot, Layers: validate.Compile, OperatorRoot: config.OperatorRoot, HealthyBaseline: config.HealthyBaseline,
	})
	if err != nil {
		return err
	}
	if err := validate.WriteReport(out, diagnostics, notes...); err != nil {
		return err
	}
	return validate.Err(diagnostics)
}
