// The pool regenerates generic policy with the exact operator build it runs.
package main

import (
	"fmt"
	"os"

	"sdo.dev/operator/memory"
)

func main() {
	if len(os.Args) != 2 {
		panic("usage: seed_render CHECKOUT")
	}
	if err := memory.WriteSchema(os.Args[1]); err != nil {
		panic(err)
	}
	if err := memory.PrepareHealth(os.Args[1]); err != nil {
		panic(err)
	}
	fmt.Println("fresh generic health policy rendered")
}
