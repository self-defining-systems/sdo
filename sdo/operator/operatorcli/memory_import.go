package operatorcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"path/filepath"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/memory/observation"
)

type memoryImportConfig struct {
	Replay               string
	Action               string
	Root                 string
	Workspaces           string
	ID                   string
	ApplicationNamespace string
	OperatorRoot         string
	HealthyBaseline      string
}

func parseMemoryImport(args []string, getenv func(string) string) (memoryImportConfig, error) {
	c := memoryImportConfig{}
	if len(args) == 0 {
		return c, errors.New("import requires prepare or publish")
	}
	c.Action = args[0]
	f := flag.NewFlagSet("memory import", flag.ContinueOnError)
	f.StringVar(&c.Replay, "replay", getenv(settings.ReplayEnvironment), "replay policy: on (default) or off")
	f.StringVar(&c.Root, "root", ".", "accepted application Git root")
	f.StringVar(&c.Workspaces, "worktree-root", "", "broker workspaces, outside accepted repository")
	f.StringVar(&c.ID, "id", "", "unique operator import ID")
	f.StringVar(&c.ApplicationNamespace, "application-namespace", "", "application observation namespace")
	f.StringVar(&c.OperatorRoot, "operator-root", operatorRootDefault(getenv), "installed Go operator modules")
	f.StringVar(&c.HealthyBaseline, "healthy-baseline", "", "directory of captured healthy snapshots to validate incident detectors against")
	positional, err := parseFlags(f, args[1:])
	if err != nil {
		return c, err
	}
	if _, err := settings.Replay(c.Replay); err != nil {
		return c, err
	}
	if len(positional) != 0 || (c.Action != "prepare" && c.Action != "publish") || c.ID == "" || c.Workspaces == "" {
		return c, errors.New("import requires prepare|publish, --id, and --worktree-root")
	}
	c.Root, err = absRoot(c.Root)
	if err != nil {
		return c, err
	}
	c.Workspaces, err = filepath.Abs(c.Workspaces)
	if err != nil {
		return c, err
	}
	return c, nil
}

func memoryImport(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	c, err := parseMemoryImport(args, getenv)
	if err != nil {
		return err
	}
	ceiling, err := observation.CeilingFromEnvironment()
	if err != nil {
		return err
	}
	var runner broker.Validator
	if c.Action == "publish" {
		runner = (broker.SubprocessValidator{OperatorRoot: c.OperatorRoot, HealthyBaseline: c.HealthyBaseline}).Validator()
	}
	repository, err := broker.New(c.Root, c.Workspaces, runner, broker.ObservationPolicy{Replay: c.Replay, Ceiling: ceiling, ApplicationNamespace: c.ApplicationNamespace})
	if err != nil {
		return err
	}
	if c.Action == "prepare" {
		proposal, err := repository.CreateProposalForPhase(ctx, c.ID, broker.ImportPhase)
		if err != nil {
			return err
		}
		return json.NewEncoder(out).Encode(proposal)
	}
	receipt, err := repository.Publish(ctx, c.ID, broker.Human, broker.ImportPhase)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(receipt)
}
