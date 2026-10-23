package operatorcli

import (
	"context"
	"errors"
	"flag"
	"io"
	"time"

	"sdo.dev/operator/install"
)

type StopConfig struct {
	ControlNamespace string
	Kubeconfig       string
	Timeout          time.Duration
	Inspect          bool
}

func ParseStopLaunch(args []string) (StopConfig, error) {
	var cfg StopConfig
	if len(args) == 0 || args[0] != "stop" {
		return cfg, errors.New("expected stop subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator stop", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.ControlNamespace, "control-namespace", "", "limit stop to one operator namespace; default all namespaces")
	flags.StringVar(&cfg.Kubeconfig, "kubeconfig", "", "Kubernetes client config")
	flags.DurationVar(&cfg.Timeout, "timeout", 60*time.Second, "deadline for runtime deletion")
	flags.BoolVar(&cfg.Inspect, "inspect", false, "only list runtime leftovers")
	if err := flags.Parse(args[1:]); err != nil {
		return cfg, err
	}
	if flags.NArg() != 0 || cfg.Timeout <= 0 {
		return cfg, errors.New("stop requires a positive timeout and no positional arguments")
	}
	return cfg, nil
}

func RunStop(ctx context.Context, cfg StopConfig) (install.StopResult, error) {
	client, err := hostKubernetesClient(cfg.Kubeconfig)
	if err != nil {
		return install.StopResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	if cfg.Inspect {
		return install.InspectRuntime(ctx, client, cfg.ControlNamespace)
	}
	return install.Stop(ctx, client, cfg.ControlNamespace)
}
