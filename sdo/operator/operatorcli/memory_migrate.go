package operatorcli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sdo.dev/operator/artifacts"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// memoryMigrate publishes through exactly the same Upkeep operation as startup.
// A failed broker validation leaves the accepted branch unchanged; the proposal is
// retained outside it for diagnosis. No upstream or cluster connection is used.
func memoryMigrate(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	flags := flag.NewFlagSet("memory migrate", flag.ContinueOnError)
	replay := flags.String("replay", getenv(settings.ReplayEnvironment), "replay policy: on or off")
	operator := flags.String("operator-root", operatorRootDefault(getenv), "operator modules")
	worktrees := flags.String("worktree-root", "", "isolated broker workspaces outside repository (required)")
	positional, err := parseFlags(flags, args)
	if err != nil {
		return err
	}
	if len(positional) != 1 || *worktrees == "" {
		return fmt.Errorf("migrate requires a repository directory and --worktree-root")
	}
	root, err := absRoot(positional[0])
	if err != nil {
		return err
	}
	if _, err = processio.AcceptedHead(ctx, root); err != nil {
		return err
	}
	mode, err := settings.Replay(*replay)
	if err != nil {
		return err
	}
	repository, err := broker.New(root, *worktrees, (broker.SubprocessValidator{OperatorRoot: *operator}).Validator(), broker.ObservationPolicy{Replay: mode})
	if err != nil {
		return err
	}
	coordinator := lifecycle.Coordinator{Broker: repository, Bootstrap: artifacts.BootstrapStore{}}
	receipt, err := coordinator.MigrateMemory(ctx, memoryMigrationOperationID())
	if err != nil {
		return err
	}
	auditPolicy, err := memoryTelemetryPolicy(getenv, "")
	if err != nil {
		return err
	}
	diagnostics, err := validate.CheckAccepted(ctx, validate.Request{Telemetry: auditPolicy, TelemetryRegistry: telemetryRegistry(), Root: root, OperatorRoot: *operator, Replay: mode}, validate.CompileCandidate)
	if err != nil {
		return err
	}
	if err = validate.Err(diagnostics); err != nil {
		return err
	}
	fmt.Fprintf(out, "memory check passed; migration: %+v\n", receipt)
	return nil
}
