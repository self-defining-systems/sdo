package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli"
)

func main() {
	if len(os.Args) > 2 && os.Args[1] == "program" {
		var err error
		switch os.Args[2] {
		case "scaffold":
			var config operatorcli.ProgramScaffoldConfig
			config, err = operatorcli.ParseProgramScaffold(os.Args[1:])
			if err == nil {
				err = operatorcli.RunProgramScaffold(config)
			}
		default:
			err = fmt.Errorf("unknown program command %q", os.Args[2])
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "baseline-write" {
		if len(os.Args) != 2 {
			fmt.Fprintln(os.Stderr, "baseline-write takes no arguments")
			os.Exit(1)
		}
		if err := operatorcli.RunBaselineWrite(os.Stdin, operatorcli.BaselineDirectory); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == "schema-identity" {
		identity, err := builder.SchemaIdentity(".")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(identity)
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "network-canary" {
		if len(os.Args) != 3 || (os.Args[2] != "allow" && os.Args[2] != "deny") {
			fmt.Fprintln(os.Stderr, "network-canary requires allow or deny")
			os.Exit(1)
		}
		if err := operatorcli.RunNetworkCanary(context.Background(), os.Args[2] == "deny"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "validate" {
		config, err := operatorcli.ParseValidateLaunch(os.Args[1:])
		if err == nil {
			err = operatorcli.RunValidateReport(context.Background(), config, os.Stdout)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "memory" {
		os.Exit(operatorcli.RunMemory(context.Background(), os.Args[2:], os.Getenv, os.Stdout, os.Stderr))
	}
	if len(os.Args) > 1 && os.Args[1] == "operate" {
		config, err := operatorcli.ParseOperateLaunch(os.Args[1:])
		if err == nil {
			result, runErr := operatorcli.RunOperate(context.Background(), config)
			err = runErr
			if err == nil {
				encoded := json.NewEncoder(os.Stdout)
				err = encoded.Encode(install.OperateResult{OperationID: config.Lifecycle.OperationID, GoalCommit: result.GoalCommit, SchemaCommit: result.SchemaCommit, SourceCommit: result.SourceCommit, ArchitectureCommit: result.ArchitectureCommit, JudgeCommit: result.JudgeCommit, Attempts: result.Attempts, Installed: true})
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "host" {
		config, err := operatorcli.ParseHostLaunch(os.Args[1:])
		if err == nil {
			var result install.OperateResult
			result, err = operatorcli.RunHost(context.Background(), config)
			if err == nil {
				err = json.NewEncoder(os.Stdout).Encode(result)
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "install-existing" {
		config, err := operatorcli.ParseInstallExistingLaunch(os.Args[1:])
		if err == nil {
			var result operatorcli.InstallExistingResult
			result, err = operatorcli.RunInstallExisting(context.Background(), config)
			if err == nil {
				err = json.NewEncoder(os.Stdout).Encode(result)
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "inspect-existing" {
		config, err := operatorcli.ParseInspectExistingLaunch(os.Args[1:])
		if err == nil {
			var result install.ExistingController
			result, err = operatorcli.RunInspectExisting(context.Background(), config)
			if err == nil {
				err = json.NewEncoder(os.Stdout).Encode(result)
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "sync-existing" {
		config, err := operatorcli.ParseSyncExistingLaunch(os.Args[1:])
		if err == nil {
			err = operatorcli.RunSyncExisting(context.Background(), config)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "maintenance" {
		config, err := operatorcli.ParseMaintenanceLaunch(os.Args[1:])
		if err == nil {
			err = operatorcli.RunMaintenance(context.Background(), config)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "late-findings" {
		config, err := operatorcli.ParseLateFindingsLaunch(os.Args[1:])
		if err == nil {
			err = operatorcli.RunLateFindings(context.Background(), config, os.Stdout)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "incident-evidence" {
		config, err := operatorcli.ParseIncidentEvidenceLaunch(os.Args[1:])
		if err == nil {
			var result operatorcli.IncidentEvidenceResult
			result, err = operatorcli.RunIncidentEvidence(context.Background(), config)
			if err == nil {
				err = json.NewEncoder(os.Stdout).Encode(result)
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "runtime-artifacts" {
		config, err := operatorcli.ParseRuntimeArtifactsLaunch(os.Args[1:])
		if err == nil {
			var result operatorcli.RuntimeArtifactsResult
			result, err = operatorcli.RunRuntimeArtifacts(context.Background(), config)
			if err == nil {
				err = json.NewEncoder(os.Stdout).Encode(result)
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	config, supervise, err := operatorcli.ParseControllerLaunch(os.Args[1:])
	if err == nil {
		if supervise {
			err = builder.Supervise(context.Background(), config)
		} else {
			err = builder.RunOnce(context.Background(), config)
		}
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
