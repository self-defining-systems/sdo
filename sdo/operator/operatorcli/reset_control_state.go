package operatorcli

import (
	"context"
	"errors"
	"flag"
	"io"
	"time"

	"sdo.dev/operator/onboarding"
)

type ResetControlStateConfig struct {
	ControlNamespace string
	Kubeconfig       string
	Timeout          time.Duration
	// KeepInstallStartup keeps the install's established start-up record
	// (a restart of the same install from memory).
	KeepInstallStartup bool
}

type ResetControlStateResult struct {
	ControlNamespace string   `json:"control_namespace"`
	Deleted          []string `json:"deleted"`
}

func ParseResetControlStateLaunch(args []string) (ResetControlStateConfig, error) {
	var cfg ResetControlStateConfig
	if len(args) == 0 || args[0] != "reset-control-state" {
		return cfg, errors.New("expected reset-control-state subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator reset-control-state", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.ControlNamespace, "control-namespace", "", "explicit operator namespace to reset before a from-scratch reinstall; stop runtime first")
	flags.StringVar(&cfg.Kubeconfig, "kubeconfig", "", "Kubernetes client config")
	flags.BoolVar(&cfg.KeepInstallStartup, "keep-install-startup", false, "keep the install's established start-up record: the reset prepares a restart of the same install from memory, which starts no new start-up period")
	flags.DurationVar(&cfg.Timeout, "timeout", 60*time.Second, "deadline for control state deletion")
	if err := flags.Parse(args[1:]); err != nil {
		return cfg, err
	}
	if flags.NArg() != 0 || cfg.ControlNamespace == "" || cfg.Timeout <= 0 {
		return cfg, errors.New("reset-control-state requires --control-namespace, a positive timeout and no positional arguments")
	}
	return cfg, nil
}

func RunResetControlState(ctx context.Context, cfg ResetControlStateConfig) (ResetControlStateResult, error) {
	result := ResetControlStateResult{ControlNamespace: cfg.ControlNamespace, Deleted: []string{}}
	if cfg.ControlNamespace == "" || cfg.Timeout <= 0 {
		return result, errors.New("reset control state requires an explicit control namespace and positive timeout")
	}
	client, err := hostKubernetesClient(cfg.Kubeconfig)
	if err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	result.Deleted, err = onboarding.ResetControlState(ctx, client, cfg.ControlNamespace, onboarding.ResetControlStateOptions{KeepInstallStartup: cfg.KeepInstallStartup})
	return result, err
}
