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
	"sdo.dev/operator/memory"
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

func RunValidate(ctx context.Context, config builder.Config) error {
	projection, err := memory.CandidateDiagnostics(config.ApplicationRoot)
	if err != nil {
		if errors.Is(err, memory.ErrDiagnosticsNotBootstrapped) {
			return nil
		}
		return err
	}
	config.DiagnosticsRoot = projection
	workspace, err := builder.BuildWorkspace(ctx, config)
	if err != nil {
		return err
	}
	defer workspace.Close()
	_, err = workspace.ValidateAndBuild(ctx)
	return err
}
