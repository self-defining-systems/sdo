// Emit benchmark expectations from the real installer's public resource plan.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"sdo.dev/operator/install"
)

func main() {
	if len(os.Args) != 2 {
		panic("usage: rbac_go APPLICATION_NAMESPACE")
	}
	namespace := os.Args[1]
	plan, err := install.Resources(install.Config{
		Namespace: namespace, ControlNamespace: namespace + "-sdo", Application: "eval-dry-run",
		ControllerImage: "controller:test", ResponderImage: "responder:test", ValidatorImage: "validator:test",
		RepositoryPVC: "sdo-application-repository", CredentialsSecret: "unused", Model: "scripted",
		AcceptedCommit: strings.Repeat("0", 40), TimeoutSeconds: 1, NativeCommand: []string{"sdo-operator"},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(plan); err != nil {
		panic(err)
	}
}
