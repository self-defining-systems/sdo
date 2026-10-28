// The pool regenerates generic policy with the exact operator build it runs.
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

func main() {
	if len(os.Args) == 7 && os.Args[1] == "sync" {
		b, err := broker.New(os.Args[2], os.Args[3], nil)
		if err != nil {
			panic(err)
		}
		if err := b.SetUpstream(broker.Upstream{URL: os.Args[4], Branch: os.Args[5], Env: broker.SSHEnv(os.Args[6])}); err != nil {
			panic(err)
		}
		if err := b.SyncUpstream(context.Background()); err != nil {
			panic(err)
		}
		return
	}
	if len(os.Args) != 3 {
		panic("usage: seed_render CHECKOUT APPLICATION | sync CHECKOUT WORKSPACES URL BRANCH SSH_DIR")
	}
	root := os.Args[1]
	command := exec.Command("git", "-C", root, "rev-parse", "HEAD")
	data, err := command.Output()
	if err != nil {
		panic(err)
	}
	revision := strings.TrimSpace(string(data))
	facts, err := memory.SourceInventory(context.Background(), root, revision)
	if err != nil {
		panic(err)
	}
	var summary strings.Builder
	summary.WriteString("# Application architecture\n\nAccepted source resources:\n\n")
	for _, resource := range facts.Resources {
		fmt.Fprintf(&summary, "- %s %s (%s)\n", resource.Kind, resource.Name, resource.Source)
	}
	if err := memory.WriteArchitecture(root, os.Args[2], revision, facts.Fingerprint, summary.String()); err != nil {
		panic(err)
	}
	if err := memory.WriteSchema(os.Args[1]); err != nil {
		panic(err)
	}
	if err := memory.PrepareHealth(os.Args[1]); err != nil {
		panic(err)
	}
	if _, err := layout.WritePlaybookIndex(root); err != nil {
		panic(err)
	}
	fmt.Println("fresh generic health policy rendered")
}
