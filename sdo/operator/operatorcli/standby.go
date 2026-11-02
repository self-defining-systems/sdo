package operatorcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/worker_registry"
)

// RunStandby keeps the configured command in the registry. The production
// daemon knows only protobuf tasks, the filesystem transport and process IO.
func RunStandby(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("standby", flag.ContinueOnError)
	raw := flags.String("command-json", "", "registered command")
	if err := flags.Parse(args); err != nil {
		return err
	}
	var command []string
	if err := json.Unmarshal([]byte(*raw), &command); err != nil {
		return err
	}
	return worker_registry.RunStandbyDaemon(ctx, worker_registry.StandbyDaemonOptions{Inbox: "/sdo/request", Ready: "/sdo/standby/ready/agent", Checkout: "/sdo/proposal", SessionDirectory: "/sdo/session", Command: command, ProxyAddress: APIProxyAddress})
}

func RunStandbyHistory(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("standby-history", flag.ContinueOnError)
	directory := flags.String("directory", "", "private history directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *directory == "" {
		return errors.New("standby history requires directory")
	}
	task, err := worker_registry.AwaitStandbyTask(ctx, "/sdo/request", "/sdo/standby/ready/history")
	if err != nil {
		return err
	}
	request := new(contracts.IncidentRequest)
	if err := task.GetRequest().UnmarshalTo(request); err != nil {
		return err
	}
	return RunAPIProxy(ctx, []string{"--incident", request.GetIncidentId(), "--directory", *directory})
}

func RunStandbyReady(args []string) error {
	if len(args) != 1 {
		return errors.New("standby-ready requires readiness path")
	}
	_, err := os.Stat(args[0])
	return err
}
