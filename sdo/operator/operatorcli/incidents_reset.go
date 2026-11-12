package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/clock"

	"sdo.dev/operator/install"
)

// IncidentsResetConfig is `sdo-operator incidents reset`: an operator-level
// reset of the running controller between problems, for example by a
// benchmark harness. It is never an agent tool.
type IncidentsResetConfig struct {
	ControlNamespace string
	Kubeconfig       string
	Timeout          time.Duration
}

// IncidentsResetResult is the command's JSON output.
type IncidentsResetResult struct {
	ControlNamespace string `json:"control_namespace"`
	Generation       string `json:"generation"`
	Acknowledged     bool   `json:"acknowledged"`
}

func ParseIncidentsResetLaunch(args []string) (IncidentsResetConfig, error) {
	var cfg IncidentsResetConfig
	if len(args) < 2 || args[0] != "incidents" || args[1] != "reset" {
		return cfg, errors.New("expected incidents reset subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator incidents reset", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.ControlNamespace, "control-namespace", "", "operator namespace of the running controller")
	flags.StringVar(&cfg.Kubeconfig, "kubeconfig", "", "Kubernetes client config")
	flags.DurationVar(&cfg.Timeout, "timeout", 2*time.Minute, "wait for the controller to acknowledge the reset")
	if err := flags.Parse(args[2:]); err != nil {
		return cfg, err
	}
	if flags.NArg() != 0 || cfg.ControlNamespace == "" || cfg.Timeout <= 0 {
		return cfg, errors.New("incidents reset requires --control-namespace, a positive --timeout and no positional arguments")
	}
	return cfg, nil
}

// RunIncidentsReset asks the controller to reset and waits for its
// acknowledgement: any open or escalated incident is closed as RESET (no
// outcome, no learning credit), its agent work is stopped, and pending
// findings are dropped so the next fault starts clean. Memory and learned
// playbooks are untouched.
func RunIncidentsReset(ctx context.Context, cfg IncidentsResetConfig) (IncidentsResetResult, error) {
	client, err := hostKubernetesClient(cfg.Kubeconfig)
	if err != nil {
		return IncidentsResetResult{ControlNamespace: cfg.ControlNamespace}, err
	}
	return requestIncidentsReset(ctx, client, clock.RealClock{}, cfg)
}

func requestIncidentsReset(ctx context.Context, client kubernetes.Interface, clk clock.WithTicker, cfg IncidentsResetConfig) (IncidentsResetResult, error) {
	generation := strconv.FormatInt(clk.Now().UnixNano(), 10)
	result := IncidentsResetResult{ControlNamespace: cfg.ControlNamespace, Generation: generation}
	resets := install.ResetRequests{Client: client, Namespace: cfg.ControlNamespace}
	if err := resets.Request(ctx, generation); err != nil {
		return result, err
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	ticker := clk.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		acknowledged, err := resets.Acknowledged(ctx)
		if err == nil && acknowledged == generation {
			result.Acknowledged = true
			return result, nil
		}
		select {
		case <-ctx.Done():
			return result, fmt.Errorf("controller in %s did not acknowledge reset %s within %s; is it running?", cfg.ControlNamespace, generation, cfg.Timeout)
		case <-ticker.C():
		}
	}
}
