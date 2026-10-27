package operatorcli

import (
	"context"
	"errors"
	"flag"
	"io"
	"time"

	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/worker_registry"
)

type ResetControlStateConfig struct {
	ControlNamespace string
	Kubeconfig       string
	Timeout          time.Duration
	// WorkersOnly retires worker task state and keeps onboarding authority.
	WorkersOnly bool
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
	flags.BoolVar(&cfg.WorkersOnly, "workers-only", false, "delete only worker request, result and dispatch ConfigMaps; keep onboarding and controller records")
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
	if !cfg.WorkersOnly {
		if result.Deleted, err = onboarding.ResetControlState(ctx, client, cfg.ControlNamespace); err != nil {
			return result, err
		}
	}
	workers, err := worker_registry.ResetWorkerState(ctx, client, cfg.ControlNamespace)
	result.Deleted = append(result.Deleted, workers...)
	return result, err
}
