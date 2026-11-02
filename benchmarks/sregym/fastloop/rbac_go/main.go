// Emit benchmark expectations from the same public installer configuration.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli"
)

func main() {
	launch, err := operatorcli.ParseInstallExistingLaunch(os.Args[1:])
	if err != nil {
		panic(err)
	}
	cfg := launch.Install
	cfg.AcceptedCommit = strings.Repeat("0", 40)
	plan, err := install.Resources(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(plan); err != nil {
		panic(err)
	}
}
