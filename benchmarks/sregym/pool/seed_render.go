// The pool regenerates generic policy with the exact operator build it runs.
package main

import (
	"context"
	"fmt"
	"os"

	"sdo.dev/operator/broker"
	"sdo.dev/operator/memory"
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
	if len(os.Args) != 2 {
		panic("usage: seed_render CHECKOUT | sync CHECKOUT WORKSPACES URL BRANCH SSH_DIR")
	}
	if err := memory.WriteSchema(os.Args[1]); err != nil {
		panic(err)
	}
	if err := memory.PrepareHealth(os.Args[1]); err != nil {
		panic(err)
	}
	fmt.Println("fresh generic health policy rendered")
}
